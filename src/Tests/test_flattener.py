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


def test_path_starts_merge() -> None:
    """path_starts: первый путь — строки, остальные — доп. данные мержатся в строку с префиксом."""
    data = {
        "results": [
            {"data": {"body": {"a": 1}, "absences": {"isLong": False, "info": "x"}}},
            {"data": {"body": {"a": 2}, "absences": {"isLong": True}}},
        ]
    }
    rows, columns = flatten_json_data(
        data, path_sep=" - ", path_starts=[["data", "body"], ["data", "absences"]]
    )
    assert len(rows) == 2
    assert "a" in columns
    assert "absences - isLong" in columns
    assert rows[0]["a"] == 1 and rows[0]["absences - isLong"] == "false"
    assert rows[1]["absences - isLong"] == "true"


def test_exclude_keys_in_path() -> None:
    """exclude_keys_in_path: исключать ключ только внутри заданного пути (например full - info)."""
    data = {"results": [{"full": {"info": 1, "keep": 2}, "other": {"info": 3}}]}
    rows, columns = flatten_json_data(
        data, path_sep=" - ", exclude_keys_in_path=[{"path": "full", "keys": ["info"]}]
    )
    assert "full - keep" in columns and rows[0]["full - keep"] == 2
    assert "other - info" in columns and rows[0]["other - info"] == 3
    assert "full - info" not in columns


def test_exclude_keys_in_path_array() -> None:
    """exclude_keys_in_path в массиве объектов: id только в agileManagers исключается, в agileTree остаётся."""
    data = {
        "results": [
            {
                "agileManagers": [{"id": "uuid-1", "role": "R1"}],
                "agileTree": [{"id": "tree-id-1", "name": "Дерево"}],
            }
        ]
    }
    rows, columns = flatten_json_data(
        data, path_sep=" - ", exclude_keys_in_path=[{"path": "agileManagers", "keys": ["id"]}]
    )
    assert "agileManagers - role - (1)" in columns and rows[0]["agileManagers - role - (1)"] == "R1"
    assert "agileManagers - id - (1)" not in columns
    assert "agileTree - id - (1)" in columns and rows[0]["agileTree - id - (1)"] == "tree-id-1"
    assert "agileTree - name - (1)" in columns


def test_array_of_objects_flatten() -> None:
    """Массив объектов разворачивается в колонки с индексом (1), (2): порядок — сначала все (1), потом все (2)."""
    data = {"results": [{"emails": [{"address": "a@b.ru", "type": "External"}, {"address": "b@b.ru", "type": "Internal"}]}]}
    rows, columns = flatten_json_data(data, path_sep=" - ")
    assert "emails - address - (1)" in columns and "emails - address - (2)" in columns
    assert "emails - type - (1)" in columns and "emails - type - (2)" in columns
    # Порядок: сначала все колонки (1), затем (2)
    idx_addr_1 = columns.index("emails - address - (1)")
    idx_type_1 = columns.index("emails - type - (1)")
    idx_addr_2 = columns.index("emails - address - (2)")
    idx_type_2 = columns.index("emails - type - (2)")
    assert idx_addr_1 < idx_addr_2 and idx_type_1 < idx_type_2
    assert max(idx_addr_1, idx_type_1) < min(idx_addr_2, idx_type_2)
    assert rows[0]["emails - address - (1)"] == "a@b.ru"
    assert rows[0]["emails - address - (2)"] == "b@b.ru"
    assert rows[0]["emails - type - (1)"] == "External"
    assert rows[0]["emails - type - (2)"] == "Internal"


def test_array_columns_grouped_by_key() -> None:
    """Колонки массивов группируются по ключу: сначала все agileManagers (1),(2), затем agileRoles (1),(2)."""
    data = {
        "results": [
            {
                "agileManagers": [
                    {"id": "id1", "role": "R1"},
                    {"id": "id2", "role": "R2"},
                ],
                "agileRoles": [
                    {"name": "N1"},
                    {"name": "N2"},
                ],
            }
        ]
    }
    rows, columns = flatten_json_data(data, path_sep=" - ")
    # Все колонки agileManagers подряд (с (1), затем (2)), затем agileRoles
    idx_am_id_1 = columns.index("agileManagers - id - (1)")
    idx_am_role_1 = columns.index("agileManagers - role - (1)")
    idx_am_id_2 = columns.index("agileManagers - id - (2)")
    idx_ar_name_1 = columns.index("agileRoles - name - (1)")
    idx_ar_name_2 = columns.index("agileRoles - name - (2)")
    assert max(idx_am_id_1, idx_am_role_1) < idx_am_id_2
    assert idx_am_id_2 < idx_ar_name_1
    assert idx_ar_name_1 < idx_ar_name_2


def test_column_order() -> None:
    """column_order: указанные колонки идут первыми, остальные — после."""
    data = {"results": [{"a": 1, "b": 2, "c": 3}]}
    rows, columns = flatten_json_data(data, column_order=["c", "a"])
    assert columns == ["c", "a", "b"]
    assert rows[0]["c"] == 3 and rows[0]["a"] == 1 and rows[0]["b"] == 2


def test_column_order_with_prefix() -> None:
    """column_order с префиксом: 'emails' подтягивает все колонки emails - ... - (1), (2) в порядке (1),(2)."""
    data = {"results": [{"name": "Ivan", "emails": [{"address": "a@b.ru", "type": "W"}, {"address": "b@b.ru", "type": "H"}]}]}
    rows, columns = flatten_json_data(data, path_sep=" - ", column_order=["name", "emails"])
    assert columns[0] == "name"
    # Дальше идут все emails в порядке (1), (2)
    assert "emails - address - (1)" in columns and "emails - address - (2)" in columns
    idx_addr_1 = columns.index("emails - address - (1)")
    idx_addr_2 = columns.index("emails - address - (2)")
    assert idx_addr_1 < idx_addr_2
    assert columns.index("name") < idx_addr_1


if __name__ == "__main__":
    test_extract_rows_list()
    test_extract_rows_results()
    test_flatten_row_nested()
    test_flatten_json_data()
    test_path_start()
    test_exclude_keys()
    test_exclude_keys_by_path()
    test_include_only_keys()
    test_path_starts_merge()
    test_exclude_keys_in_path()
    test_exclude_keys_in_path_array()
    test_array_of_objects_flatten()
    test_array_columns_grouped_by_key()
    test_column_order()
    test_column_order_with_prefix()
    print("Все проверки пройдены.")
