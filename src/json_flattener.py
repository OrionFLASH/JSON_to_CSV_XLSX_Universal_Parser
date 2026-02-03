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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Разделитель между ключами в имени колонки (переопределяется из config.json)
DEFAULT_PATH_SEP = " - "


def _flatten_one(
    obj: Any,
    prefix: str,
    sep: str,
    out: Dict[str, Any],
    exclude_keys: Optional[Set[str]] = None,
) -> None:
    """
    Рекурсивно обходит узел JSON и записывает скалярные значения в словарь out.
    exclude_keys: имена ключей или полные пути (например "photoData" или "colorCode - primary"),
    которые не попадают в out и не обходятся рекурсивно.
    """
    exclude = exclude_keys or set()
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
        else:
            try:
                out[prefix] = json.dumps(obj, ensure_ascii=False)
            except (TypeError, ValueError):
                out[prefix] = str(obj)
        return
    # Вложенный объект — обходим ключи, пропуская исключённые по имени или по полному пути
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in exclude:
                continue
            part = str(k).replace(sep.strip(), "_").strip()
            new_prefix = f"{prefix}{sep}{part}" if prefix else part
            if new_prefix in exclude:
                continue
            _flatten_one(v, new_prefix, sep, out, exclude_keys=exclude_keys)
        return
    # Прочие типы
    out[prefix] = str(obj)


def flatten_row(
    row: Dict[str, Any],
    path_sep: str = DEFAULT_PATH_SEP,
    exclude_keys: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Преобразует одну запись (словарь) в одну плоскую строку таблицы.
    exclude_keys: имена ключей (и подпутей), которые не попадают в результат.
    """
    flat: Dict[str, Any] = {}
    for key, value in row.items():
        if key in (exclude_keys or set()):
            continue
        new_prefix = key.replace(path_sep.strip(), "_").strip()
        _flatten_one(value, new_prefix, path_sep, flat, exclude_keys=exclude_keys)
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
    exclude_keys: Optional[List[str]] = None,
    include_only_keys: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Разворачивает загруженные данные в плоскую таблицу.

    path_start: цепочка ключей, с которой начинать разбор (например ["data", "body"]).
                Имена колонок и данные строятся только от этой вложенности.
    exclude_keys: ключи (имена или пути с разделителем), которые не попадают в выход.
    include_only_keys: если не пусто — в выход попадают только эти колонки; если пусто — все.
    """
    rows_raw = extract_rows(data)
    path_start = path_start or []
    exclude_set: Set[str] = set(exclude_keys or [])
    include_set: Optional[Set[str]] = None
    if include_only_keys:
        include_set = set(include_only_keys)

    # Спуск до нужной вложенности: каждая строка заменяется на подобъект по path_start
    if path_start:
        new_rows: List[Dict[str, Any]] = []
        for row in rows_raw:
            drilled = _drill_into(row, path_start)
            if drilled is not None:
                new_rows.append(drilled)
            else:
                new_rows.append(row)
        rows_raw = new_rows

    all_flat: List[Dict[str, Any]] = []
    all_keys: set = set()
    for row in rows_raw:
        flat = flatten_row(row, path_sep, exclude_keys=exclude_set)
        all_flat.append(flat)
        all_keys.update(flat.keys())

    columns = sorted(all_keys)
    if include_set is not None:
        columns = [c for c in columns if c in include_set]
        columns = sorted(columns)

    return all_flat, columns


def load_and_flatten(
    json_path: Path,
    path_sep: str = DEFAULT_PATH_SEP,
    path_start: Optional[List[str]] = None,
    exclude_keys: Optional[List[str]] = None,
    include_only_keys: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Читает JSON из файла и разворачивает в плоскую таблицу с учётом
    path_start, exclude_keys и include_only_keys.
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
        "JSON разобран, path_start=%s, exclude_keys=%s, include_only=%s [def: load_and_flatten]",
        path_start,
        exclude_keys,
        bool(include_only_keys),
    )
    rows, columns = flatten_json_data(
        data,
        path_sep=path_sep,
        path_start=path_start,
        exclude_keys=exclude_keys,
        include_only_keys=include_only_keys,
    )
    log.info(
        "Файл %s: получено строк=%s, колонок=%s [def: load_and_flatten]",
        json_path.name,
        len(rows),
        len(columns),
    )
    log.debug("Колонки (первые 5): %s [def: load_and_flatten]", columns[:5] if columns else [])
    return rows, columns
