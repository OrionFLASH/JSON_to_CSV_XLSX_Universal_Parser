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
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, List[Dict[str, Any]], List[str], Path]:
    """
    Обрабатывает один JSON-файл: загрузка и разворот в таблицу, запись CSV в output_dir.
    Возвращает (имя_листа, строки, колонки, путь_к_csv) для последующей сборки XLSX.
    """
    log = logger or logging.getLogger(__name__)
    path_sep = config.get("path_separator", " - ")
    path_start = config.get("path_start") or []
    exclude_keys = config.get("exclude_keys") or []
    include_only_keys = config.get("include_only_keys") or []
    output_dir = Path(config.get("output_dir", "OUT"))
    csv_opts = config.get("csv", {})

    # Имя листа = имя файла без расширения, без недопустимых символов Excel, макс 31 символ
    stem = json_path.stem
    sheet_name = stem.replace("\\", "_").replace("/", "_").replace("*", "_").replace("?", "_")[:31]

    log.debug("Начало обработки файла %s [def: process_one_file]", json_path.name)
    # Загрузка и разворот JSON в плоскую таблицу (с учётом path_start, exclude_keys, include_only_keys)
    rows, columns = json_flattener.load_and_flatten(
        json_path,
        path_sep=path_sep,
        path_start=path_start if path_start else None,
        exclude_keys=exclude_keys if exclude_keys else None,
        include_only_keys=include_only_keys if include_only_keys else None,
        logger=log,
    )
    log.debug("Файл %s: развёрнут, запись CSV [def: process_one_file]", json_path.name)

    # Имя CSV с таймштампом (год, месяц, день — час, минуты), если передан в конфиге
    run_ts = config.get("_run_timestamp", "")
    csv_name = f"{stem}_{run_ts}.csv" if run_ts else f"{stem}.csv"
    out_csv = base_dir / output_dir / csv_name
    csv_exporter.write_csv(
        rows,
        columns,
        out_csv,
        encoding=csv_opts.get("encoding", "utf-8-sig"),
        delimiter=csv_opts.get("delimiter", ";"),
        lineterminator=csv_opts.get("lineterminator", "\n"),
        logger=log,
    )

    log.debug("Файл %s обработан, возврат данных листа [def: process_one_file]", json_path.name)
    return sheet_name, rows, columns, out_csv
