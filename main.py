# -*- coding: utf-8 -*-
"""
Точка входа: универсальный парсер JSON в CSV и XLSX.
Читает config.json (список файлов в input_files), обрабатывает каждый файл из IN,
пишет CSV по одному на файл в OUT и один общий XLSX с листом на каждый файл.
Логирование: INFO — основные этапы, DEBUG — детали (список файлов, режим пула и т.д.).
"""

from __future__ import annotations

import logging
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Формат таймштампа в именах выходных файлов: ГГГГММДД-ЧЧММ (год, месяц, день — час, минуты)
OUTPUT_TIMESTAMP_FMT = "%Y%m%d-%H%M"

from src import config_loader
from src import logging_setup
from src import xlsx_exporter
from src import worker


def _process_one_file_standalone(
    json_path_str: str,
    base_dir_str: str,
    config: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]], List[str]]:
    """
    Обёртка для вызова из дочернего процесса (должна быть на уровне модуля для pickle).
    Возвращает (имя_листа, строки, колонки). Запись CSV выполняется внутри worker.
    """
    json_path = Path(json_path_str)
    base_dir = Path(base_dir_str)
    # В дочернем процессе логгер может быть не настроен — добавляем NullHandler
    log = logging.getLogger("json_parser.worker")
    if not log.handlers:
        log.addHandler(logging.NullHandler())
    _, rows, columns, _ = worker.process_one_file(json_path, base_dir, config, logger=log)
    sheet_name = json_path.stem[:31].replace("\\", "_").replace("/", "_").replace("*", "_").replace("?", "_")
    return sheet_name, rows, columns


def main() -> None:
    """Загрузка конфига, обработка файлов (параллельно или последовательно), запись XLSX."""
    base_dir = Path(__file__).resolve().parent
    # Настройка логов: два уровня — INFO и DEBUG в отдельные файлы
    log = logging_setup.setup_logging(log_dir=base_dir / "log", theme="parser")
    log.info("Старт парсера JSON -> CSV/XLSX [def: main]")

    # Загрузка конфигурации
    config = config_loader.load_config(base_dir / "config.json")
    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    input_files = config.get("input_files") or []
    path_sep = config.get("path_separator", " - ")
    # Таймштамп для имён выходных файлов (год, месяц, день — час, минуты)
    run_timestamp = datetime.now().strftime(OUTPUT_TIMESTAMP_FMT)
    config["_run_timestamp"] = run_timestamp
    log.debug("Конфиг загружен: input_dir=%s, output_dir=%s, файлов в списке=%s, таймштамп=%s [def: main]", input_dir, output_dir, len(input_files), run_timestamp)

    if not input_files:
        log.warning("В конфиге не заданы input_files; завершение [def: main]")
        return

    in_dir_abs = base_dir / input_dir
    if not in_dir_abs.is_dir():
        log.error("Папка входных файлов не найдена: %s [def: main]", in_dir_abs)
        return

    # Формируем список существующих файлов по именам из конфига
    to_process: List[Path] = []
    for name in input_files:
        p = in_dir_abs / name
        if p.is_file():
            to_process.append(p)
        else:
            log.warning("Файл из конфига не найден: %s [def: main]", p)
    log.debug("К обработке подготовлено файлов: %s [def: main]", len(to_process))

    if not to_process:
        log.warning("Нет файлов для обработки [def: main]")
        return

    log.info("К обработке: %s файлов [def: main]", len(to_process))

    # Решаем: параллельная обработка (пул процессов) или последовательная
    max_workers = max(1, min(multiprocessing.cpu_count() - 1, len(to_process)))
    sheets_data: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []

    if max_workers >= 2 and len(to_process) > 1:
        log.debug("Запуск пула процессов: workers=%s [def: main]", max_workers)
        with multiprocessing.Pool(processes=max_workers) as pool:
            args = [
                (str(p), str(base_dir), config)
                for p in to_process
            ]
            results = pool.starmap(_process_one_file_standalone, args)
        sheets_data = list(results)
        log.info("Параллельная обработка завершена, процессов: %s [def: main]", max_workers)
    else:
        log.debug("Последовательная обработка (один файл или один воркер) [def: main]")
        for p in to_process:
            try:
                sheet_name, rows, columns = _process_one_file_standalone(str(p), str(base_dir), config)
                sheets_data.append((sheet_name, rows, columns))
            except Exception as e:
                log.exception("Ошибка при обработке %s: %s [def: main]", p.name, e)
                raise

    if not sheets_data:
        log.warning("Нет данных для XLSX [def: main]")
        return

    # Запись одного XLSX со всеми листами (имя с таймштампом)
    xlsx_path = base_dir / output_dir / f"output_{run_timestamp}.xlsx"
    xlsx_opts = config.get("xlsx", {})
    # Форматы колонок по листам: для каждого листа — словарь {имя_колонки: {number_format: "integer", ...}}
    # default_column_format — формат по умолчанию для всех колонок, кроме перечисленных в column_format
    column_formats_per_sheet: List[Dict[str, Dict[str, Any]]] = []
    default_formats_per_sheet: List[Dict[str, Any]] = []
    for file_index in range(len(sheets_data)):
        sheet_opts = config_loader.get_sheet_options(config, file_index)
        column_formats_per_sheet.append(sheet_opts.get("column_format") or {})
        default_formats_per_sheet.append(sheet_opts.get("default_column_format") or {})
    has_any_format = any(cf for cf in column_formats_per_sheet) or any(df for df in default_formats_per_sheet)
    # Закрепление: общая ячейка по умолчанию и переопределение по листам
    freeze_cell = (xlsx_opts.get("freeze_cell") or "").strip() or None
    freeze_pane_per_sheet = xlsx_opts.get("freeze_pane_per_sheet") or []
    freeze_cell_per_sheet: List[Optional[str]] = [None] * len(sheets_data)
    for entry in freeze_pane_per_sheet:
        if isinstance(entry, dict) and "sheet_index" in entry and "cell" in entry:
            idx = entry["sheet_index"]
            if isinstance(idx, int) and 0 <= idx < len(sheets_data):
                cell = (entry.get("cell") or "").strip()
                if cell:
                    freeze_cell_per_sheet[idx] = cell
    log.debug("Запись XLSX: путь=%s, листов=%s [def: main]", xlsx_path, len(sheets_data))
    xlsx_exporter.write_xlsx(
        sheets_data,
        xlsx_path,
        freeze_first_row=xlsx_opts.get("freeze_first_row", True),
        freeze_cell=freeze_cell,
        freeze_cell_per_sheet=freeze_cell_per_sheet if any(freeze_cell_per_sheet) else None,
        autofilter=xlsx_opts.get("autofilter", True),
        column_width_mode=(xlsx_opts.get("column_width_mode") or "auto").strip().lower(),
        auto_row_height=bool(xlsx_opts.get("auto_row_height", False)),
        column_widths=None,
        column_formats_per_sheet=column_formats_per_sheet if has_any_format else None,
        default_formats_per_sheet=default_formats_per_sheet if has_any_format else None,
        logger=log,
    )

    log.info("Готово. XLSX: %s; CSV по одному на каждый файл в %s [def: main]", xlsx_path, base_dir / output_dir)


if __name__ == "__main__":
    main()
