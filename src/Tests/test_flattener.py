# -*- coding: utf-8 -*-
"""
Проверка развёртывания JSON: extract_rows, flatten_row, flatten_json_data.
Запуск: python src/Tests/test_flattener.py
"""

import sys
from pathlib import Path

# Корень проекта в путь для импорта src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.json_flattener import extract_rows, flatten_row, flatten_json_data


def test_extract_rows_list() -> None:
    """Корень — список объектов: каждая элемент списка становится отдельной строкой."""
    data = [{"a": 1}, {"a": 2}]
    rows = extract_rows(data)
    assert len(rows) == 2
    assert rows[0]["a"] == 1 and rows[1]["a"] == 2


def test_extract_rows_results() -> None:
    """Корень — объект с ключом results (массив): строки = элементы results."""
    data = {"results": [{"id": 1}, {"id": 2}], "errors": []}
    rows = extract_rows(data)
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[1]["id"] == 2


def test_flatten_row_nested() -> None:
    """Вложенный объект разворачивается в путь ключей через разделитель."""
    row = {"a": {"b": {"c": "val"}}}
    flat = flatten_row(row, path_sep=" - ")
    assert "a - b - c" in flat
    assert flat["a - b - c"] == "val"


def test_flatten_json_data() -> None:
    """Полный цикл: данные с ключом results -> плоская таблица и список колонок."""
    data = {
        "results": [
            {"x": 1, "y": {"z": "hello"}},
            {"x": 2, "y": {"z": "world"}},
        ]
    }
    rows, columns = flatten_json_data(data, path_sep=" - ")
    assert len(rows) == 2
    assert "x" in columns and "y - z" in columns
    assert rows[0]["x"] == 1 and rows[0]["y - z"] == "hello"


def test_path_start() -> None:
    """path_start: имена колонок и данные начинаются с заданной вложенности."""
    data = {
        "results": [
            {"data": {"body": {"a": 1, "b": 2}}},
            {"data": {"body": {"a": 3, "b": 4}}},
        ]
    }
    rows, columns = flatten_json_data(data, path_sep=" - ", path_start=["data", "body"])
    assert len(rows) == 2
    assert "a" in columns and "b" in columns
    assert "data" not in str(columns)
    assert rows[0]["a"] == 1 and rows[0]["b"] == 2


def test_exclude_keys() -> None:
    """exclude_keys: указанные ключи не попадают в выход."""
    data = {"results": [{"a": 1, "b": 2, "secret": 999}]}
    rows, columns = flatten_json_data(data, exclude_keys=["secret"])
    assert "secret" not in columns
    assert "a" in columns and "b" in columns
    assert rows[0]["a"] == 1 and "secret" not in rows[0]


def test_exclude_keys_by_path() -> None:
    """exclude_keys: можно исключить по полному пути (например вложенное поле)."""
    data = {"results": [{"a": 1, "nested": {"x": 10, "y": 20}}]}
    rows, columns = flatten_json_data(data, path_sep=" - ", exclude_keys=["nested - y"])
    assert "nested - x" in columns and rows[0]["nested - x"] == 10
    assert "nested - y" not in columns and "nested - y" not in rows[0]


def test_include_only_keys() -> None:
    """include_only_keys: в выход попадают только указанные колонки."""
    data = {"results": [{"a": 1, "b": 2, "c": 3}]}
    rows, columns = flatten_json_data(data, include_only_keys=["a", "c"])
    assert columns == ["a", "c"]
    assert rows[0]["a"] == 1 and rows[0]["c"] == 3


if __name__ == "__main__":
    test_extract_rows_list()
    test_extract_rows_results()
    test_flatten_row_nested()
    test_flatten_json_data()
    test_path_start()
    test_exclude_keys()
    test_exclude_keys_by_path()
    test_include_only_keys()
    print("Все проверки пройдены.")
