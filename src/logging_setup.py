# -*- coding: utf-8 -*-
"""
Настройка логирования на двух уровнях:
- INFO — основные события (старт, число файлов, запись CSV/XLSX, готово);
- DEBUG — детали (прочитанный файл, этапы разбора, параметры записи).
Файлы в папке log по шаблону: Уровень_логирования_(тема)_годмесяцдень_час.log
Строка DEBUG в файле: дата время - [уровень] - сообщение [class: имя_класса | def: имя_функции]
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logging(
    log_dir: Optional[Path] = None,
    theme: str = "parser",
    level_file: int = logging.DEBUG,
    level_console: int = logging.INFO,
) -> logging.Logger:
    """
    Создаёт папку log и настраивает два уровня логирования:
    - В файл DEBUG_... — все сообщения (DEBUG и выше), с контекстом [class: | def: ] для DEBUG;
    - В файл INFO_... — только INFO и выше;
    - В консоль — по умолчанию INFO (можно задать level_console).
    Возвращает корневой логгер приложения (имя "json_parser").
    """
    if log_dir is None:
        log_dir = Path(__file__).resolve().parent.parent / "log"
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Имена файлов по текущему часу
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H")
    info_name = f"INFO_{theme}_{stamp}.log"
    debug_name = f"DEBUG_{theme}_{stamp}.log"

    root = logging.getLogger("json_parser")
    root.setLevel(logging.DEBUG)

    # Для записей уровня DEBUG добавляем суффикс [class: ... | def: ...]
    class FormatterWithContext(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            base = super().format(record)
            if record.levelno == logging.DEBUG:
                extra = " [class: {} | def: {}]".format(
                    getattr(record, "classname", ""),
                    getattr(record, "funcname", record.funcName),
                )
                if extra in base:
                    return base
                return base.rstrip() + extra + "\n"
            return base

    fmt_console = "%(asctime)s - [%(levelname)s] - %(message)s"
    fmt_file = "%(asctime)s - [%(levelname)s] - %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    # Файл DEBUG: все уровни, с контекстом для DEBUG
    fh_debug = logging.FileHandler(log_dir / debug_name, encoding="utf-8")
    fh_debug.setLevel(level_file)
    fh_debug.setFormatter(FormatterWithContext(fmt_file, date_fmt))
    root.addHandler(fh_debug)

    # Файл INFO: только INFO и выше (основные события)
    fh_info = logging.FileHandler(log_dir / info_name, encoding="utf-8")
    fh_info.setLevel(logging.INFO)
    fh_info.setFormatter(logging.Formatter(fmt_file, date_fmt))
    root.addHandler(fh_info)

    # Консоль: по умолчанию INFO
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level_console)
    ch.setFormatter(logging.Formatter(fmt_console, date_fmt))
    root.addHandler(ch)

    return root
