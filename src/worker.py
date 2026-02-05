# -*- coding: utf-8 -*-
"""
Обработка одного JSON-файла: загрузка, flatten, запись CSV.
Возвращает данные для добавления листа в общий XLSX (имя листа, строки, колонки, путь к CSV).
Используется в многопроцессном пуле; логирование на двух уровнях: INFO и DEBUG.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config_loader
from . import csv_exporter
from . import json_flattener


def process_one_file(
    json_path: Path,
    base_dir: Path,
    config: Dict[str, Any],
    file_index: int = 0,
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, List[Dict[str, Any]], List[str], Path]:
    """
    Обрабатывает один JSON-файл: загрузка и разворот в таблицу, запись CSV (если output содержит "csv").
    Возвращает (имя_листа, строки, колонки, путь_к_csv) для последующей сборки XLSX.
    file_index используется для получения настроек из config.files[file_index] (sheet_name, output, path_start и т.д.).
    """
    log = logger or logging.getLogger(__name__)
    path_sep = config.get("path_separator", " - ")
    include_only_keys = config.get("include_only_keys") or []
    output_dir = Path(config.get("output_dir", "OUT"))
    csv_opts = config.get("csv", {})

    stem = json_path.stem
    default_sheet_name = stem.replace("\\", "_").replace("/", "_").replace("*", "_").replace("?", "_")[:31]
    file_opts = config_loader.get_file_options(config, file_index, default_sheet_name)
    sheet_name = file_opts.get("sheet_name", default_sheet_name)
    output = file_opts.get("output") or ["csv", "xlsx"]
    path_start = file_opts.get("path_start") or []
    path_starts = file_opts.get("path_starts")
    exclude_keys = file_opts.get("exclude_keys") or []
    exclude_keys_in_path = file_opts.get("exclude_keys_in_path") or []
    column_order = file_opts.get("column_order")

    log.debug("Начало обработки файла %s (file_index=%s) [def: process_one_file]", json_path.name, file_index)
    rows, columns = json_flattener.load_and_flatten(
        json_path,
        path_sep=path_sep,
        path_start=path_start if path_start else None,
        path_starts=path_starts,
        exclude_keys=exclude_keys if exclude_keys else None,
        exclude_keys_in_path=exclude_keys_in_path if exclude_keys_in_path else None,
        include_only_keys=include_only_keys if include_only_keys else None,
        column_order=column_order,
        logger=log,
    )
    log.debug("Файл %s: развёрнут, запись CSV=%s [def: process_one_file]", json_path.name, "csv" in output)

    run_ts = config.get("_run_timestamp", "")
    csv_name = f"{stem}_{run_ts}.csv" if run_ts else f"{stem}.csv"
    out_csv = base_dir / output_dir / csv_name
    if "csv" in output:
        csv_exporter.write_csv(
            rows,
            columns,
            out_csv,
            encoding=csv_opts.get("encoding", "utf-8-sig"),
            delimiter=csv_opts.get("delimiter", ";"),
            lineterminator=csv_opts.get("lineterminator", "\n"),
            logger=log,
        )
    else:
        out_csv = base_dir / output_dir / csv_name

    log.debug("Файл %s обработан, возврат данных листа [def: process_one_file]", json_path.name)
    return sheet_name, rows, columns, out_csv
