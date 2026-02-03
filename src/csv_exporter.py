# -*- coding: utf-8 -*-
"""
Экспорт плоской таблицы (список словарей с общими ключами) в CSV.
Один CSV-файл на каждый обработанный JSON (по требованию ТЗ).
Логирование: INFO — факт записи и число строк, DEBUG — параметры записи.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional


def write_csv(
    rows: List[Dict[str, Any]],
    columns: List[str],
    out_path: Path,
    *,
    encoding: str = "utf-8-sig",
    delimiter: str = ";",
    lineterminator: str = "\n",
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Записывает таблицу в CSV: первая строка — заголовки (columns),
    далее по одной строке на элемент rows. encoding=utf-8-sig даёт BOM для Excel.
    """
    log = logger or logging.getLogger(__name__)
    # Создаём родительскую папку при необходимости
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log.debug(
        "Запись CSV: путь=%s, строк=%s, колонок=%s, разделитель=%r [def: write_csv]",
        out_path,
        len(rows),
        len(columns),
        delimiter,
    )
    try:
        with open(out_path, "w", encoding=encoding, newline="") as f:
            writer = csv.writer(f, delimiter=delimiter, lineterminator=lineterminator)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(row.get(col, "") for col in columns)
    except OSError as e:
        log.error("Ошибка записи CSV %s: %s [def: write_csv]", out_path, e)
        raise
    log.info("Записан CSV: %s, строк: %s [def: write_csv]", out_path, len(rows))
