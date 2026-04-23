# -*- coding: utf-8 -*-
"""
Модуль развёртывания (flatten) произвольной структуры JSON в плоскую таблицу.
Имя колонки = путь от корня до значения (или от path_start) в виде "ключ1 - ключ2 - ключ3".
Поддержка: старт с заданной вложенности (path_start), исключение ключей (exclude_keys),
вывод только указанных колонок (include_only_keys).
path_start может заканчиваться массивом объектов: каждый элемент массива становится отдельной строкой.
"""

from __future__ import annotations

import json
import logging
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Разделитель между ключами в имени колонки (переопределяется из config.json)
DEFAULT_PATH_SEP = " - "

# Шаблон суффикса колонки из массива объектов: " - (1)", " - (2)" и т.д.
_ARRAY_INDEX_SUFFIX_RE = re.compile(r"^(.+) - \((\d+)\)$")


def _column_sort_key(col_name: str) -> tuple:
    """
    Ключ сортировки колонок: по ключам (первый сегмент пути), внутри ключа — по (1), (2), (3)…
    Не смешиваем все (1) из разных массивов: сначала все колонки agileManagers (1),(2),(3)…,
    затем agileRoles (1),(2),(3)… и т.д.
    """
    # Первый сегмент пути — группа (agileManagers, agileRoles, data и т.д.)
    parts = col_name.split(" - ")
    group_key = parts[0] if parts else col_name
    m = _ARRAY_INDEX_SUFFIX_RE.match(col_name)
    if m:
        # Внутри группы: сначала индекс (1), (2), (3), затем базовое имя
        return (group_key, 1, int(m.group(2)), m.group(1))
    return (group_key, 0, 0, col_name)


def _is_wildcard_path_segment(segment: str) -> bool:
    """Проверяет, что сегмент пути — wildcard для обхода массивов."""
    return segment in ("*", "[*]")


def _resolve_path_nodes(source: Any, path: List[str]) -> List[Any]:
    """
    Возвращает список узлов по пути path.
    Поддерживает wildcard-сегменты "*" / "[*]" для прохода по массивам.
    """
    nodes: List[Any] = [source]
    for segment in path:
        next_nodes: List[Any] = []
        if _is_wildcard_path_segment(str(segment)):
            for node in nodes:
                if isinstance(node, list):
                    next_nodes.extend(node)
            nodes = next_nodes
            continue

        key = str(segment)
        for node in nodes:
            if isinstance(node, dict) and key in node:
                next_nodes.append(node[key])
        nodes = next_nodes
    return nodes


def _split_path_spec(path_spec: Any) -> List[str]:
    """
    Нормализует путь:
    - список сегментов -> список строк;
    - строка "a.b.c" -> ["a","b","c"].
    """
    if isinstance(path_spec, list):
        return [str(x) for x in path_spec]
    if isinstance(path_spec, str):
        return [x for x in path_spec.split(".") if x]
    return []


def _get_by_path_spec(source: Any, path_spec: Any) -> Any:
    """Возвращает первое значение по пути (без wildcard-расширения для выдачи списка)."""
    path = _split_path_spec(path_spec)
    if not path:
        return None
    nodes = _resolve_path_nodes(source, path)
    if not nodes:
        return None
    return nodes[0]


def _extract_aliases(row_builder: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Читает карту алиасов ключей row_builder.key_aliases."""
    if not isinstance(row_builder, dict):
        return {}
    aliases_raw = row_builder.get("key_aliases") or {}
    out: Dict[str, List[str]] = {}
    if not isinstance(aliases_raw, dict):
        return out
    for key, vals in aliases_raw.items():
        if not isinstance(key, str):
            continue
        if isinstance(vals, list):
            out[key] = [str(v) for v in vals if isinstance(v, str)]
    return out


def _candidate_keys(field_name: str, aliases: Dict[str, List[str]]) -> List[str]:
    """
    Возвращает список возможных ключей для чтения значения:
    сам ключ + его алиасы + обратные алиасы.
    """
    result: List[str] = [field_name]
    result.extend(aliases.get(field_name, []))
    for base_key, alias_list in aliases.items():
        if field_name in alias_list and base_key not in result:
            result.append(base_key)
    uniq: List[str] = []
    seen: Set[str] = set()
    for k in result:
        if k not in seen:
            seen.add(k)
            uniq.append(k)
    return uniq


def _read_field_with_aliases(source: Dict[str, Any], field_name: str, aliases: Dict[str, List[str]]) -> Any:
    """Читает поле из объекта с учётом алиасов (employeeId/empId и т.п.)."""
    for key in _candidate_keys(field_name, aliases):
        if key in source:
            return source.get(key)
    return None


def _is_empty_match_value(value: Any) -> bool:
    """Пустое значение для join-ключа (не участвует в сопоставлении)."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def _matches_by_rules(
    base_obj: Dict[str, Any],
    candidate_obj: Dict[str, Any],
    match_rules: List[Dict[str, Any]],
    aliases: Dict[str, List[str]],
) -> bool:
    """
    Проверяет совпадение base и candidate по всем правилам match.
    Пример: [{"left":"employeeId","right":"employeeId"}]
    """
    if not match_rules:
        return False
    for rule in match_rules:
        left = (rule.get("left") or "").strip()
        right = (rule.get("right") or "").strip()
        if not left or not right:
            return False
        lv = _read_field_with_aliases(base_obj, left, aliases)
        rv = _read_field_with_aliases(candidate_obj, right, aliases)
        if _is_empty_match_value(lv) or _is_empty_match_value(rv):
            return False
        if str(lv) != str(rv):
            return False
    return True


def _extract_root_rows_for_builder(data: Any) -> List[Dict[str, Any]]:
    """
    Для row_builder берём «корневые» записи:
    - если корень список -> словари списка;
    - если корень словарь -> одна запись.
    """
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _build_rows_with_row_builder(data: Any, row_builder: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Строит строки по декларативному правилу row_builder:
      - base_path/base_key: базовые записи (обычно cards[*]);
      - joins: присоединение массивов по match (employeeId/empId);
      - carry_root_fields: поля из корня запроса в каждую строку.
    """
    base_path = _split_path_spec(row_builder.get("base_path"))
    if not base_path:
        return None

    aliases = _extract_aliases(row_builder)
    joins_raw = row_builder.get("joins") or []
    carry_raw = row_builder.get("carry_root_fields") or []

    roots = _extract_root_rows_for_builder(data)
    if not roots:
        return None

    out_rows: List[Dict[str, Any]] = []
    for root in roots:
        base_nodes = _resolve_path_nodes(root, base_path)
        base_dicts = [x for x in base_nodes if isinstance(x, dict)]
        if not base_dicts:
            continue

        # Подготавливаем join-кандидаты для текущего root один раз.
        prepared_joins: List[Dict[str, Any]] = []
        for join in joins_raw:
            if not isinstance(join, dict):
                continue
            join_path = _split_path_spec(join.get("path"))
            if not join_path:
                continue
            join_match = join.get("match") or []
            if not isinstance(join_match, list):
                join_match = []
            join_mode = str(join.get("mode") or "first_match").strip().lower()
            join_prefix = (join.get("prefix") or "").strip()
            if not join_prefix:
                join_prefix = join_path[-1].replace("*", "items")
            candidates = [x for x in _resolve_path_nodes(root, join_path) if isinstance(x, dict)]
            prepared_joins.append(
                {
                    "match": [x for x in join_match if isinstance(x, dict)],
                    "mode": join_mode,
                    "prefix": join_prefix,
                    "candidates": candidates,
                }
            )

        for base in base_dicts:
            row = deepcopy(base)

            # Поля из корневой записи (input/searchText/ошибки/статистика и т.д.)
            for carry in carry_raw:
                if isinstance(carry, str):
                    carry_path = _split_path_spec(carry)
                    carry_name = carry_path[-1] if carry_path else carry
                elif isinstance(carry, dict):
                    carry_path = _split_path_spec(carry.get("path"))
                    carry_name = str(carry.get("as") or (carry_path[-1] if carry_path else "")).strip()
                else:
                    continue
                if not carry_path or not carry_name:
                    continue
                row[carry_name] = _get_by_path_spec(root, carry_path)

            # Join по ключам (employeeId/empId и др.)
            for join in prepared_joins:
                match_rules = join.get("match") or []
                prefix = join.get("prefix") or ""
                candidates = join.get("candidates") or []
                mode = join.get("mode") or "first_match"
                matched = [c for c in candidates if _matches_by_rules(row, c, match_rules, aliases)]
                if mode == "all":
                    row[prefix] = matched
                else:
                    row[prefix] = matched[0] if matched else {}

            out_rows.append(row)

    return out_rows


def _is_missing_value(value: Any) -> bool:
    """
    Проверяет, что значение считается отсутствующим для цепочки подстановок:
    None, пустая строка или строка из пробелов.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _resolve_key_value(
    row: Dict[str, Any],
    sources: List[str],
    default_value: Any,
) -> Any:
    """
    Возвращает первое непустое значение из источников sources.
    Если ничего не найдено — default_value.
    """
    for source in sources:
        src = (source or "").strip()
        if not src:
            continue
        candidate = row.get(src)
        if not _is_missing_value(candidate):
            return candidate
    return default_value


def _apply_key_fields(
    rows: List[Dict[str, Any]],
    key_fields: Optional[List[Dict[str, Any]]],
) -> List[str]:
    """
    Добавляет/пересчитывает ключевые поля в каждой строке.

    Формат key_fields:
      [
        {
          "name": "ТАБ",
          "sources": ["employeeNumber", "tn"],
          "default": "-"
        }
      ]

    Возвращает список имён ключевых полей в том порядке, как они заданы в конфиге.
    """
    if not key_fields:
        return []

    target_names: List[str] = []
    for rule in key_fields:
        if not isinstance(rule, dict):
            continue
        target = (rule.get("name") or "").strip()
        if not target:
            continue
        sources_raw = rule.get("sources") or []
        sources = [str(x) for x in sources_raw if isinstance(x, str)]
        default_value = rule.get("default", "-")

        for row in rows:
            row[target] = _resolve_key_value(row, sources, default_value)
        target_names.append(target)

    return target_names


def _flatten_one(
    obj: Any,
    prefix: str,
    sep: str,
    out: Dict[str, Any],
    exclude_keys: Optional[Set[str]] = None,
    exclude_keys_in_path: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Рекурсивно обходит узел JSON и записывает скалярные значения в словарь out.
    exclude_keys: имена ключей или полные пути, которые не попадают в out.
    exclude_keys_in_path: список { "path": "full", "keys": ["info"] } — исключать keys только внутри пути path.
    """
    exclude = exclude_keys or set()
    exclude_in_path = exclude_keys_in_path or []
    # Исключение по полному пути: не добавляем и не спускаемся
    if prefix in exclude:
        return
    # Пустое значение — пустая ячейка
    if obj is None:
        out[prefix] = ""
        return
    # Булево
    if isinstance(obj, bool):
        out[prefix] = str(obj).lower() if isinstance(obj, bool) else obj
        return
    # Числа
    if isinstance(obj, (int, float)):
        out[prefix] = obj
        return
    # Строка
    if isinstance(obj, str):
        out[prefix] = obj
        return
    # Массив в ячейке
    if isinstance(obj, list):
        if not obj:
            out[prefix] = ""
        elif all(type(x) in (str, int, float, bool) or x is None for x in obj):
            out[prefix] = ", ".join(str(x) for x in obj)
        elif all(isinstance(x, dict) for x in obj):
            # Массив объектов: каждая позиция — отдельные колонки с индексом (1), (2), …
            # Имя колонки: prefix - ключ - (N); значение — значение этого ключа в N-м элементе массива.
            for idx, item in enumerate(obj):
                if not isinstance(item, dict):
                    continue
                idx_suffix = f" - ({idx + 1})"
                for k, v in item.items():
                    if k in exclude:
                        continue
                    # Исключение ключа только внутри этого пути (например id только в agileManagers)
                    skip = False
                    for rule in exclude_in_path:
                        path_part = (rule.get("path") or "").strip()
                        keys = rule.get("keys") or []
                        if path_part and keys and k in keys:
                            if prefix.endswith(path_part) or path_part in prefix:
                                skip = True
                                break
                    if skip:
                        continue
                    part = str(k).replace(sep.strip(), "_").strip()
                    col_name = f"{prefix}{sep}{part}{idx_suffix}" if prefix else f"{part}{idx_suffix}"
                    if col_name in exclude:
                        continue
                    if v is None:
                        out[col_name] = ""
                    elif isinstance(v, bool):
                        out[col_name] = str(v).lower()
                    elif isinstance(v, (int, float)):
                        out[col_name] = v
                    elif isinstance(v, str):
                        out[col_name] = v
                    elif isinstance(v, (dict, list)):
                        _flatten_one(
                            v, col_name, sep, out,
                            exclude_keys=exclude_keys,
                            exclude_keys_in_path=exclude_in_path,
                        )
                    else:
                        out[col_name] = str(v)
        else:
            try:
                out[prefix] = json.dumps(obj, ensure_ascii=False)
            except (TypeError, ValueError):
                out[prefix] = str(obj)
        return
    # Вложенный объект — обходим ключи, пропуская исключённые
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in exclude:
                continue
            part = str(k).replace(sep.strip(), "_").strip()
            new_prefix = f"{prefix}{sep}{part}" if prefix else part
            if new_prefix in exclude:
                continue
            # Исключение только внутри заданного пути: если текущий prefix заканчивается на path, не добавлять keys
            skip = False
            for rule in exclude_in_path:
                path_part = (rule.get("path") or "").strip()
                keys = rule.get("keys") or []
                if path_part and keys and k in keys:
                    if prefix.endswith(path_part) or path_part in prefix or new_prefix.endswith(path_part):
                        skip = True
                        break
            if skip:
                continue
            _flatten_one(
                v, new_prefix, sep, out,
                exclude_keys=exclude_keys,
                exclude_keys_in_path=exclude_keys_in_path,
            )
        return
    # Прочие типы
    out[prefix] = str(obj)


def flatten_row(
    row: Dict[str, Any],
    path_sep: str = DEFAULT_PATH_SEP,
    exclude_keys: Optional[Set[str]] = None,
    exclude_keys_in_path: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Преобразует одну запись (словарь) в одну плоскую строку таблицы.
    exclude_keys: имена ключей (и подпутей), которые не попадают в результат.
    exclude_keys_in_path: список { "path": "full", "keys": ["info"] } — исключать keys только внутри path.
    """
    flat: Dict[str, Any] = {}
    for key, value in row.items():
        if key in (exclude_keys or set()):
            continue
        new_prefix = key.replace(path_sep.strip(), "_").strip()
        _flatten_one(
            value, new_prefix, path_sep, flat,
            exclude_keys=exclude_keys,
            exclude_keys_in_path=exclude_keys_in_path,
        )
    return flat


def _drill_into(row: Dict[str, Any], path_start: List[str]) -> Optional[Dict[str, Any]]:
    """
    Спускается по цепочке ключей path_start вглубь строки.
    Возвращает вложенный словарь или None, если путь не найден.
    """
    current: Any = row
    for key in path_start:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current if isinstance(current, dict) else None


def _follow_path(row: Dict[str, Any], path_start: List[str]) -> Optional[Any]:
    """
    Спуск по цепочке ключей; возвращает значение на конце пути (dict, list или скаляр).
    None — если путь оборван (нет ключа или не dict на промежуточном шаге).
    """
    current: Any = row
    for key in path_start:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def extract_rows(data: Any) -> List[Dict[str, Any]]:
    """
    Определяет, что считать строками таблицы, по структуре корня JSON.
    """
    if isinstance(data, list):
        rows = []
        for item in data:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append({"_": item})
        return rows
    if isinstance(data, dict):
        if len(data) == 1:
            single_value = next(iter(data.values()))
            if isinstance(single_value, list):
                return extract_rows(single_value)
        for key in ("results", "data", "items", "rows"):
            if key in data and isinstance(data[key], list):
                return extract_rows(data[key])
        # Несколько ключей корня, каждый — список объектов (напр. турниры по id): объединяем в одну таблицу строк
        if len(data) > 1:
            merged_multi: List[Dict[str, Any]] = []
            all_values_are_lists_of_dicts = True
            for value in data.values():
                if not isinstance(value, list):
                    all_values_are_lists_of_dicts = False
                    break
                for x in value:
                    if not isinstance(x, dict):
                        all_values_are_lists_of_dicts = False
                        break
            if all_values_are_lists_of_dicts:
                for value in data.values():
                    for item in value:
                        if isinstance(item, dict):
                            merged_multi.append(item)
                if merged_multi:
                    return merged_multi
        for value in data.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return extract_rows(value)
        return [data]
    return [{"_": data}]


def flatten_json_data(
    data: Any,
    path_sep: str = DEFAULT_PATH_SEP,
    path_start: Optional[List[str]] = None,
    path_starts: Optional[List[List[str]]] = None,
    exclude_keys: Optional[List[str]] = None,
    exclude_keys_in_path: Optional[List[Dict[str, Any]]] = None,
    include_only_keys: Optional[List[str]] = None,
    column_order: Optional[List[str]] = None,
    key_fields: Optional[List[Dict[str, Any]]] = None,
    row_builder: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Разворачивает загруженные данные в плоскую таблицу.

    path_start: одна цепочка ключей для старта (если path_starts не задан).
        Если на конце цепочки — список словарей, строк таблицы будет столько, сколько элементов в списке.
    path_starts: несколько цепочек: первая — источник строк, остальные — доп. данные, мержатся в каждую строку с префиксом.
    exclude_keys: ключи (имена или пути), которые не попадают в выход.
    exclude_keys_in_path: список { "path": "full", "keys": ["info"] } — исключать keys только внутри path.
    include_only_keys: если не пусто — в выход только эти колонки.
    column_order: порядок колонок (указанные первыми, остальные — в алфавитном порядке после).
    key_fields: вычисляемые ключевые поля с цепочкой источников и default.
    row_builder: правила сборки строки из нескольких узлов JSON с join по ключам.
    """
    built_rows = _build_rows_with_row_builder(data, row_builder or {}) if row_builder else None
    using_row_builder = built_rows is not None
    rows_raw = built_rows if built_rows is not None else extract_rows(data)
    # Определяем основной path_start и дополнительные path_starts для слияния
    if using_row_builder:
        # При row_builder строка уже сформирована (с join/carry), повторный path_start не применяем.
        main_path = []
        extra_paths = []
    elif path_starts and len(path_starts) > 0:
        main_path = path_starts[0]
        extra_paths = path_starts[1:] if len(path_starts) > 1 else []
    else:
        main_path = path_start or []
        extra_paths = []

    exclude_set: Set[str] = set(exclude_keys or [])
    include_set: Optional[Set[str]] = None
    if include_only_keys:
        include_set = set(include_only_keys)
    excl_in_path = exclude_keys_in_path or []

    # Сохраняем исходные строки для слияния по extra_paths (drill по ним из корня записи)
    rows_original = list(rows_raw)
    # Спуск по основной цепочке: если на конце пути — список объектов, каждый элемент становится отдельной строкой
    if main_path:
        new_rows: List[Dict[str, Any]] = []
        new_originals: List[Dict[str, Any]] = []
        for i, row in enumerate(rows_raw):
            orig = rows_original[i] if i < len(rows_original) else row
            if not isinstance(row, dict):
                new_rows.append(row)
                new_originals.append(orig)
                continue
            tail = _follow_path(row, main_path)
            if tail is None:
                new_rows.append(row)
                new_originals.append(orig)
            elif isinstance(tail, list):
                if not tail:
                    new_rows.append(row)
                    new_originals.append(orig)
                else:
                    for item in tail:
                        if isinstance(item, dict):
                            new_rows.append(item)
                            new_originals.append(orig)
                        else:
                            new_rows.append({"_": item})
                            new_originals.append(orig)
            elif isinstance(tail, dict):
                new_rows.append(tail)
                new_originals.append(orig)
            else:
                # Скаляр на конце пути — одна строка-обёртка
                new_rows.append({"_value": tail})
                new_originals.append(orig)
        rows_raw = new_rows
        rows_original = new_originals

    all_flat: List[Dict[str, Any]] = []
    all_keys: set = set()
    for i, row in enumerate(rows_raw):
        flat = flatten_row(
            row, path_sep,
            exclude_keys=exclude_set,
            exclude_keys_in_path=excl_in_path,
        )
        # Дополнительные пути: взять значение по пути из исходной строки (до спуска), развернуть и слить
        if extra_paths and i < len(rows_original):
            orig = rows_original[i]
            for extra in extra_paths:
                sub = _drill_into(orig, extra)
                if sub is not None:
                    extra_flat = flatten_row(
                        sub, path_sep,
                        exclude_keys=exclude_set,
                        exclude_keys_in_path=excl_in_path,
                    )
                    prefix = (extra[-1] if extra else "").replace(path_sep.strip(), "_").strip()
                    for k, v in extra_flat.items():
                        col = f"{prefix}{path_sep}{k}" if prefix else k
                        flat[col] = v
        all_flat.append(flat)
        all_keys.update(flat.keys())

    # Ключевые поля добавляем после flatten: они могут ссылаться на уже развёрнутые колонки.
    keys_before_derived = set(all_keys)
    derived_key_names = _apply_key_fields(all_flat, key_fields)
    for row in all_flat:
        all_keys.update(row.keys())

    # Порядок: сначала колонки без индекса массива, затем по (1), (2), (3)… — все поля (1), потом все (2) и т.д.
    columns = sorted(all_keys, key=_column_sort_key)
    if include_set is not None:
        columns = [c for c in columns if c in include_set]
        columns = sorted(columns, key=_column_sort_key)
    if column_order:
        # Точное совпадение или префикс: "emails" подтягивает все колонки emails - ... - (1), (2) в нужном порядке
        ordered: List[str] = []
        used: Set[str] = set()
        prefix_sep = path_sep if path_sep else " - "
        for item in column_order:
            item_str = (item or "").strip()
            if not item_str:
                continue
            if item_str in columns and item_str not in used:
                ordered.append(item_str)
                used.add(item_str)
                continue
            # Префикс: все колонки, начинающиеся с item_str + разделитель пути, в порядке _column_sort_key
            prefix = item_str + prefix_sep
            matching = [c for c in columns if c.startswith(prefix) and c not in used]
            matching.sort(key=_column_sort_key)
            for c in matching:
                ordered.append(c)
                used.add(c)
        rest = [c for c in columns if c not in used]
        columns = ordered + rest

    # Если вычисляемого ключа не было в исходном JSON и его нет в column_order —
    # такие поля показываем самыми первыми в порядке описания key_fields.
    if derived_key_names:
        ordered_config = [x.strip() for x in (column_order or []) if isinstance(x, str) and x.strip()]
        to_front: List[str] = []
        for name in derived_key_names:
            if name not in columns:
                continue
            if name in ordered_config:
                continue
            if name in keys_before_derived:
                continue
            to_front.append(name)
        if to_front:
            columns = to_front + [c for c in columns if c not in set(to_front)]

    return all_flat, columns


def load_and_flatten(
    json_path: Path,
    path_sep: str = DEFAULT_PATH_SEP,
    path_start: Optional[List[str]] = None,
    path_starts: Optional[List[List[str]]] = None,
    exclude_keys: Optional[List[str]] = None,
    exclude_keys_in_path: Optional[List[Dict[str, Any]]] = None,
    include_only_keys: Optional[List[str]] = None,
    column_order: Optional[List[str]] = None,
    key_fields: Optional[List[Dict[str, Any]]] = None,
    row_builder: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Читает JSON из файла и разворачивает в плоскую таблицу с учётом
    path_start/path_starts, exclude_keys, exclude_keys_in_path, include_only_keys,
    column_order, key_fields и row_builder.
    """
    log = logger or logging.getLogger(__name__)
    try:
        text = json_path.read_text(encoding="utf-8")
    except OSError as e:
        log.error("Ошибка чтения файла %s: %s [def: load_and_flatten]", json_path, e)
        raise
    log.debug("Файл прочитан, размер %s байт [def: load_and_flatten]", len(text))
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        log.error("Ошибка разбора JSON в %s: %s [def: load_and_flatten]", json_path, e)
        raise
    log.debug(
        "JSON разобран, path_start=%s, path_starts=%s, exclude_keys=%s [def: load_and_flatten]",
        path_start,
        bool(path_starts),
        exclude_keys,
    )
    rows, columns = flatten_json_data(
        data,
        path_sep=path_sep,
        path_start=path_start,
        path_starts=path_starts,
        exclude_keys=exclude_keys,
        exclude_keys_in_path=exclude_keys_in_path,
        include_only_keys=include_only_keys,
        column_order=column_order,
        key_fields=key_fields,
        row_builder=row_builder,
    )
    log.info(
        "Файл %s: получено строк=%s, колонок=%s [def: load_and_flatten]",
        json_path.name,
        len(rows),
        len(columns),
    )
    log.debug("Колонки (первые 5): %s [def: load_and_flatten]", columns[:5] if columns else [])
    return rows, columns
