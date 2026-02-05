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
    "column_width_mode": "auto",
    "auto_row_height": False,
}


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Загружает config.json. Если путь не передан — ищет config.json в корне проекта.
    При ошибке или отсутствии файла возвращает конфиг по умолчанию с пустым files.
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
            "files": [],
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
            "files": [],
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
        "files": data.get("files", []),
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


def get_files_list(config: Dict[str, Any]) -> List[str]:
    """
    Возвращает список имён файлов для обработки из config.files (массив объектов с полем file).
    """
    files = config.get("files")
    if files and isinstance(files, list):
        return [f.get("file") for f in files if f.get("file")]
    return []


def get_file_options(
    config: Dict[str, Any],
    file_index: int,
    default_sheet_name: str,
) -> Dict[str, Any]:
    """
    Настройки для одного файла (листа): path_start/path_starts, exclude_keys,
    exclude_keys_in_path, output (csv/xlsx), sheet_name, column_order.
    Объединяет config.files[file_index] с глобальными и xlsx.sheets.
    """
    files = config.get("files")
    opts: Dict[str, Any] = {
        "sheet_name": default_sheet_name,
        "output": ["csv", "xlsx"],
        "path_start": config.get("path_start") or [],
        "path_starts": None,
        "exclude_keys": config.get("exclude_keys") or [],
        "exclude_keys_in_path": [],
        "column_order": None,
    }
    if files and file_index < len(files):
        f = files[file_index]
        if isinstance(f, dict):
            if f.get("sheet_name"):
                opts["sheet_name"] = str(f["sheet_name"])[:31]
            if f.get("output") is not None:
                out = f["output"]
                opts["output"] = [out] if isinstance(out, str) else list(out) if isinstance(out, (list, tuple)) else ["csv", "xlsx"]
            if f.get("path_start") is not None:
                opts["path_start"] = f["path_start"] if isinstance(f["path_start"], list) else []
            if f.get("path_starts") is not None:
                opts["path_starts"] = f["path_starts"] if isinstance(f["path_starts"], list) else None
            if f.get("exclude_keys") is not None:
                opts["exclude_keys"] = f["exclude_keys"] if isinstance(f["exclude_keys"], list) else []
            if f.get("exclude_keys_in_path") is not None:
                opts["exclude_keys_in_path"] = f["exclude_keys_in_path"] if isinstance(f["exclude_keys_in_path"], list) else []
            if f.get("column_order") is not None:
                opts["column_order"] = f["column_order"] if isinstance(f["column_order"], list) else None
    sheet_opts = get_sheet_options(config, file_index)
    opts["sheet_format"] = sheet_opts
    return opts
