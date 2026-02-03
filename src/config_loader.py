# -*- coding: utf-8 -*-
"""
Загрузка и разбор конфигурации из config.json.
Список входных файлов, параметры CSV/XLSX, форматирование колонок.
Логирование: WARNING при отсутствии или ошибке чтения конфига.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

# Значения по умолчанию при отсутствии конфига или ключей
DEFAULT_INPUT_DIR = "IN"
DEFAULT_OUTPUT_DIR = "OUT"
DEFAULT_PATH_SEP = " - "
DEFAULT_CSV = {
    "encoding": "utf-8-sig",
    "delimiter": ";",
    "lineterminator": "\n",
}
DEFAULT_XLSX = {
    "freeze_first_row": True,
    "autofilter": True,
}


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Загружает config.json. Если путь не передан — ищет config.json в корне проекта.
    При ошибке или отсутствии файла возвращает конфиг по умолчанию с пустым input_files.
    """
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config.json"
    path = Path(config_path)
    if not path.is_file():
        logging.getLogger(__name__).debug(
            "Файл конфига не найден %s, используются значения по умолчанию [def: load_config]",
            path,
        )
        return {
            "input_dir": DEFAULT_INPUT_DIR,
            "output_dir": DEFAULT_OUTPUT_DIR,
            "input_files": [],
            "path_separator": DEFAULT_PATH_SEP,
            "path_start": [],
            "exclude_keys": [],
            "include_only_keys": [],
            "csv": DEFAULT_CSV.copy(),
            "xlsx": DEFAULT_XLSX.copy(),
            "column_formats": {},
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logging.getLogger(__name__).warning(
            "Не удалось загрузить конфиг %s: %s [def: load_config]",
            path,
            e,
        )
        return {
            "input_dir": DEFAULT_INPUT_DIR,
            "output_dir": DEFAULT_OUTPUT_DIR,
            "input_files": [],
            "path_separator": DEFAULT_PATH_SEP,
            "path_start": [],
            "exclude_keys": [],
            "include_only_keys": [],
            "csv": DEFAULT_CSV.copy(),
            "xlsx": DEFAULT_XLSX.copy(),
            "column_formats": {},
        }
    if not isinstance(data, dict):
        data = {}
    # Слияние с умолчаниями для вложенных словарей csv/xlsx
    return {
        "input_dir": data.get("input_dir", DEFAULT_INPUT_DIR),
        "output_dir": data.get("output_dir", DEFAULT_OUTPUT_DIR),
        "input_files": data.get("input_files", []),
        "path_separator": data.get("path_separator", DEFAULT_PATH_SEP),
        "path_start": data.get("path_start", []),
        "exclude_keys": data.get("exclude_keys", []),
        "include_only_keys": data.get("include_only_keys", []),
        "csv": {**DEFAULT_CSV, **data.get("csv", {})},
        "xlsx": {**DEFAULT_XLSX, **data.get("xlsx", {})},
        "column_formats": data.get("column_formats", {}),
    }


def get_sheet_options(
    config: Dict[str, Any],
    file_index: int,
) -> Dict[str, Any]:
    """
    Возвращает настройки листа для файла с индексом file_index из config.xlsx.sheets.
    Если листов меньше чем файлов — повторяется первый лист с подставленным именем.
    """
    xlsx = config.get("xlsx", {})
    sheets = xlsx.get("sheets", [])
    if not sheets:
        return {"name": f"Лист{file_index + 1}", "columns": [], "column_format": {}}
    if file_index < len(sheets):
        return sheets[file_index]
    return {**sheets[0], "name": sheets[0].get("name", f"Лист{file_index + 1}")}
