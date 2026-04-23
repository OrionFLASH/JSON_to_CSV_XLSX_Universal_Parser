# -*- coding: utf-8 -*-
"""
Загрузка и разбор конфигурации из config.json.
Список входных файлов (files[]), фильтр enabled/disabled, параметры CSV/XLSX.
Оформление листа: вложенный files[].sheet или устаревший xlsx.sheets по индексу.
Логирование: WARNING при отсутствии или ошибке чтения конфига.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Значения по умолчанию при отсутствии конфига или ключей
DEFAULT_INPUT_DIR = "IN"
DEFAULT_OUTPUT_DIR = "OUT"
DEFAULT_OUTPUT_FILE_BASE_NAME = "output"
DEFAULT_OUTPUT_TIMESTAMP_FORMAT = "%Y%m%d-%H%M"
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
    Загружает config.json (только рабочие ключи; пояснения к параметрам — в README).
    Если путь не передан — ищет config.json в корне проекта.
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
            "output_file_base_name": DEFAULT_OUTPUT_FILE_BASE_NAME,
            "output_timestamp_format": DEFAULT_OUTPUT_TIMESTAMP_FORMAT,
            "files": [],
            "path_separator": DEFAULT_PATH_SEP,
            "path_start": [],
            "exclude_keys": [],
            "include_only_keys": [],
            "key_fields": [],
            "row_builder": {},
            "csv": DEFAULT_CSV.copy(),
            "xlsx": DEFAULT_XLSX.copy(),
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
            "output_file_base_name": DEFAULT_OUTPUT_FILE_BASE_NAME,
            "output_timestamp_format": DEFAULT_OUTPUT_TIMESTAMP_FORMAT,
            "files": [],
            "path_separator": DEFAULT_PATH_SEP,
            "path_start": [],
            "exclude_keys": [],
            "include_only_keys": [],
            "key_fields": [],
            "row_builder": {},
            "csv": DEFAULT_CSV.copy(),
            "xlsx": DEFAULT_XLSX.copy(),
        }
    if not isinstance(data, dict):
        data = {}
    # Слияние с умолчаниями для вложенных словарей csv/xlsx
    return {
        "input_dir": data.get("input_dir", DEFAULT_INPUT_DIR),
        "output_dir": data.get("output_dir", DEFAULT_OUTPUT_DIR),
        "output_file_base_name": data.get("output_file_base_name", DEFAULT_OUTPUT_FILE_BASE_NAME),
        "output_timestamp_format": data.get("output_timestamp_format", DEFAULT_OUTPUT_TIMESTAMP_FORMAT),
        "files": data.get("files", []),
        "path_separator": data.get("path_separator", DEFAULT_PATH_SEP),
        "path_start": data.get("path_start", []),
        "exclude_keys": data.get("exclude_keys", []),
        "include_only_keys": data.get("include_only_keys", []),
        "key_fields": data.get("key_fields", []),
        "row_builder": data.get("row_builder", {}),
        "csv": {**DEFAULT_CSV, **data.get("csv", {})},
        "xlsx": {**DEFAULT_XLSX, **data.get("xlsx", {})},
    }


def _is_file_entry_enabled(file_entry: Dict[str, Any]) -> bool:
    """
    Запись files[] считается включённой, если enabled не false и disabled не true.
    По умолчанию (ключи отсутствуют) — включено.
    """
    if file_entry.get("disabled") is True:
        return False
    dis = str(file_entry.get("disabled", "")).strip().lower()
    if dis in ("1", "true", "yes", "on"):
        return False
    if file_entry.get("enabled") is False:
        return False
    en = str(file_entry.get("enabled", "")).strip().lower()
    if en in ("0", "false", "no", "off"):
        return False
    return True


def get_sheet_options(
    config: Dict[str, Any],
    file_index: int,
) -> Dict[str, Any]:
    """
    Настройки оформления листа XLSX для config.files[file_index].
    Приоритет: вложенный объект files[].sheet; иначе (устар.) config.xlsx.sheets[file_index].
    """
    files = config.get("files", [])
    if isinstance(files, list) and 0 <= file_index < len(files):
        f = files[file_index]
        if isinstance(f, dict):
            nested = f.get("sheet")
            if isinstance(nested, dict) and nested:
                sheet_title = f.get("sheet_name") or nested.get("name") or f"Лист{file_index + 1}"
                merged: Dict[str, Any] = {**nested, "name": str(sheet_title)[:31]}
                merged.setdefault("columns", [])
                merged.setdefault("column_format", {})
                merged.setdefault("default_column_format", {})
                return merged
    xlsx = config.get("xlsx", {})
    sheets = xlsx.get("sheets", [])
    if not sheets:
        return {"name": f"Лист{file_index + 1}", "columns": [], "column_format": {}}
    if file_index < len(sheets):
        return sheets[file_index]
    return {**sheets[0], "name": sheets[0].get("name", f"Лист{file_index + 1}")}


def get_enabled_files_with_indices(config: Dict[str, Any]) -> List[Tuple[str, int]]:
    """
    Список (имя_файла, индекс_в_config.files) только для записей с enabled (и не disabled).
    Индекс нужен, чтобы get_file_options/get_sheet_options брали настройки той же записи.
    """
    files = config.get("files")
    out: List[Tuple[str, int]] = []
    if not files or not isinstance(files, list):
        return out
    for i, f in enumerate(files):
        if not isinstance(f, dict) or not f.get("file"):
            continue
        if not _is_file_entry_enabled(f):
            continue
        out.append((str(f["file"]), i))
    return out


def get_files_list(config: Dict[str, Any]) -> List[str]:
    """
    Имена файлов для обработки (только включённые записи files[]).
    """
    return [name for name, _ in get_enabled_files_with_indices(config)]


def get_file_options(
    config: Dict[str, Any],
    file_index: int,
    default_sheet_name: str,
) -> Dict[str, Any]:
    """
    Настройки для одного файла (листа): path_start/path_starts, exclude_keys,
    exclude_keys_in_path, output (csv/xlsx), sheet_name, column_order.
    Объединяет config.files[file_index] с глобальными и вложенным files[].sheet (или xlsx.sheets).
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
        "key_fields": config.get("key_fields") or [],
        "row_builder": config.get("row_builder") or {},
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
            if f.get("key_fields") is not None:
                opts["key_fields"] = f["key_fields"] if isinstance(f["key_fields"], list) else []
            if f.get("row_builder") is not None:
                opts["row_builder"] = f["row_builder"] if isinstance(f["row_builder"], dict) else {}
    sheet_opts = get_sheet_options(config, file_index)
    opts["sheet_format"] = sheet_opts
    return opts
