# -*- coding: utf-8 -*-
"""
Модуль развёртывания (flatten) произвольной структуры JSON в плоскую таблицу.
Имя колонки = путь от корня до значения (или от path_start) в виде "ключ1 - ключ2 - ключ3".
Поддержка: старт с заданной вложенности (path_start), исключение ключей (exclude_keys),
вывод только указанных колонок (include_only_keys).
"""

from __future__ import annotations

import json
import logging
import re
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
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Разворачивает загруженные данные в плоскую таблицу.

    path_start: одна цепочка ключей для старта (если path_starts не задан).
    path_starts: несколько цепочек: первая — источник строк, остальные — доп. данные, мержатся в каждую строку с префиксом.
    exclude_keys: ключи (имена или пути), которые не попадают в выход.
    exclude_keys_in_path: список { "path": "full", "keys": ["info"] } — исключать keys только внутри path.
    include_only_keys: если не пусто — в выход только эти колонки.
    column_order: порядок колонок (указанные первыми, остальные — в алфавитном порядке после).
    """
    rows_raw = extract_rows(data)
    # Определяем основной path_start и дополнительные path_starts для слияния
    if path_starts and len(path_starts) > 0:
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
    # Спуск до нужной вложенности по основной цепочке
    if main_path:
        new_rows: List[Dict[str, Any]] = []
        for row in rows_raw:
            drilled = _drill_into(row, main_path)
            if drilled is not None:
                new_rows.append(drilled)
            else:
                new_rows.append(row)
        rows_raw = new_rows

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
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Читает JSON из файла и разворачивает в плоскую таблицу с учётом
    path_start/path_starts, exclude_keys, exclude_keys_in_path, include_only_keys, column_order.
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
    )
    log.info(
        "Файл %s: получено строк=%s, колонок=%s [def: load_and_flatten]",
        json_path.name,
        len(rows),
        len(columns),
    )
    log.debug("Колонки (первые 5): %s [def: load_and_flatten]", columns[:5] if columns else [])
    return rows, columns
