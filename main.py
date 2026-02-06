# -*- coding: utf-8 -*-
"""
Точка входа: универсальный парсер JSON в CSV и XLSX.
Читает config.json (список файлов в files), обрабатывает каждый файл из IN,
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
    file_index: int = 0,
) -> Tuple[str, List[Dict[str, Any]], List[str]]:
    """
    Обёртка для вызова из дочернего процесса (должна быть на уровне модуля для pickle).
    Возвращает (имя_листа, строки, колонки). Запись CSV выполняется внутри worker при output "csv".
    file_index — индекс файла для настроек config.files[file_index].
    """
    json_path = Path(json_path_str)
    base_dir = Path(base_dir_str)
    log = logging.getLogger("json_parser.worker")
    if not log.handlers:
        log.addHandler(logging.NullHandler())
    sheet_name, rows, columns, _ = worker.process_one_file(
        json_path, base_dir, config, file_index=file_index, logger=log
    )
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
    file_names = config_loader.get_files_list(config)
    # Таймштамп для имён выходных файлов (год, месяц, день — час, минуты)
    run_timestamp = datetime.now().strftime(OUTPUT_TIMESTAMP_FMT)
    config["_run_timestamp"] = run_timestamp
    log.debug("Конфиг загружен: input_dir=%s, output_dir=%s, файлов в списке=%s, таймштамп=%s [def: main]", input_dir, output_dir, len(file_names), run_timestamp)

    if not file_names:
        log.warning("В конфиге не заданы files или список пуст; завершение [def: main]")
        return

    in_dir_abs = base_dir / input_dir
    if not in_dir_abs.is_dir():
        log.error("Папка входных файлов не найдена: %s [def: main]", in_dir_abs)
        return

    # Формируем список существующих файлов по именам из config.files (с индексом для настроек по файлу)
    to_process: List[Tuple[Path, int]] = []
    for idx, name in enumerate(file_names):
        p = in_dir_abs / name
        if p.is_file():
            to_process.append((p, idx))
        else:
            log.warning("Файл из конфига не найден: %s [def: main]", p)
    if len(to_process) < len(file_names):
        log.info("В конфиге файлов: %s, найдено в %s: %s [def: main]", len(file_names), input_dir, len(to_process))
    log.debug("К обработке подготовлено файлов: %s [def: main]", len(to_process))

    if not to_process:
        log.warning("Нет файлов для обработки [def: main]")
        return

    log.info("К обработке: %s файлов [def: main]", len(to_process))

    # Решаем: параллельная обработка (пул процессов) или последовательная
    max_workers = max(1, min(multiprocessing.cpu_count() - 1, len(to_process)))
    results: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []

    if max_workers >= 2 and len(to_process) > 1:
        log.debug("Запуск пула процессов: workers=%s [def: main]", max_workers)
        with multiprocessing.Pool(processes=max_workers) as pool:
            args = [
                (str(p), str(base_dir), config, file_index)
                for p, file_index in to_process
            ]
            results = pool.starmap(_process_one_file_standalone, args)
        log.info("Параллельная обработка завершена, процессов: %s [def: main]", max_workers)
    else:
        log.debug("Последовательная обработка (один файл или один воркер) [def: main]")
        for p, file_index in to_process:
            try:
                res = _process_one_file_standalone(str(p), str(base_dir), config, file_index)
                results.append(res)
            except Exception as e:
                log.exception("Ошибка при обработке %s: %s [def: main]", p.name, e)
                raise

    # В XLSX только листы, для которых в настройках файла указан output "xlsx"
    sheets_data: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []
    sheet_file_indices: List[int] = []
    for i, (sheet_name, rows, columns) in enumerate(results):
        file_index = to_process[i][1]
        opts = config_loader.get_file_options(config, file_index, sheet_name)
        if "xlsx" in opts.get("output", ["csv", "xlsx"]):
            sheets_data.append((sheet_name, rows, columns))
            sheet_file_indices.append(file_index)

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
    for j in range(len(sheets_data)):
        file_index = sheet_file_indices[j] if j < len(sheet_file_indices) else j
        sheet_opts = config_loader.get_sheet_options(config, file_index)
        column_formats_per_sheet.append(sheet_opts.get("column_format") or {})
        default_formats_per_sheet.append(sheet_opts.get("default_column_format") or {})
    has_any_format = any(cf for cf in column_formats_per_sheet) or any(df for df in default_formats_per_sheet)
    # Закрепление: по умолчанию freeze_cell (или A2 при freeze_first_row); для каждого листа — из xlsx.sheets[].freeze_cell
    default_freeze = (xlsx_opts.get("freeze_cell") or "").strip() or ("A2" if xlsx_opts.get("freeze_first_row", True) else None)
    freeze_cell = default_freeze
    freeze_pane_per_sheet = xlsx_opts.get("freeze_pane_per_sheet") or []
    freeze_cell_per_sheet: List[Optional[str]] = [None] * len(sheets_data)
    for j in range(len(sheets_data)):
        file_index = sheet_file_indices[j] if j < len(sheet_file_indices) else j
        sheet_opts = config_loader.get_sheet_options(config, file_index)
        cell = (sheet_opts.get("freeze_cell") or "").strip()
        freeze_cell_per_sheet[j] = cell if cell else default_freeze
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
