# -*- coding: utf-8 -*-
"""
Экспорт плоской таблицы в XLSX с использованием только стандартной библиотеки
(zipfile + xml.etree). Реализованы: заморозка первой строки, автофильтр, ширина колонок.
Логирование: INFO — факт записи и число листов, DEBUG — этапы сборки (shared strings, листы).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import zipfile


def _escape(s: str) -> str:
    """Экранирование спецсимволов для подстановки в XML-атрибуты."""
    s = str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    return s


def _excel_col(i: int) -> str:
    """Преобразует индекс колонки (0-based) в буквенный номер Excel: 0->A, 1->B, ..., 26->AA."""
    result = []
    while True:
        result.append(chr(ord("A") + (i % 26)))
        i = i // 26
        if i == 0:
            break
        i -= 1
    return "".join(reversed(result))


def _cell_ref(col: int, row: int) -> str:
    """Формирует ссылку на ячейку в формате A1."""
    return f"{_excel_col(col)}{row}"


def _parse_cell_ref(cell: str) -> Tuple[int, int]:
    """
    Парсит ссылку на ячейку в формате A1, B2, AA10 и т.д.
    Возвращает (col_0based, row_1based). Невалидная строка -> (0, 1).
    """
    cell = (cell or "").strip().upper()
    if not cell:
        return 0, 1
    col_part = []
    row_part = []
    for c in cell:
        if c.isalpha():
            col_part.append(c)
        elif c.isdigit():
            row_part.append(c)
        else:
            break
    if not col_part or not row_part:
        return 0, 1
    col = 0
    for c in col_part:
        col = col * 26 + (ord(c) - ord("A") + 1)
    col -= 1  # 0-based
    row = int("".join(row_part))
    return col, row


def _build_shared_strings(strings: List[str]) -> Tuple[str, Dict[str, int]]:
    """
    Строит XML sharedStrings и словарь «строка -> индекс».
    В XLSX повторяющиеся строки хранятся один раз, ячейки ссылаются по индексу.
    """
    idx_map: Dict[str, int] = {}
    unique: List[str] = []
    for s in strings:
        key = s if isinstance(s, str) else str(s)
        if key not in idx_map:
            idx_map[key] = len(unique)
            unique.append(key)
    root = ET.Element(
        "sst",
        xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        count=str(len(strings)),
        uniqueCount=str(len(unique)),
    )
    for u in unique:
        si = ET.SubElement(root, "si")
        t = ET.SubElement(si, "t")
        if len(u) > 0 and (u.strip() != u or "\n" in u):
            t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t.text = u
    rough = ET.tostring(root, encoding="unicode", default_namespace="")
    if "xmlns=" not in rough:
        rough = rough.replace("<sst ", '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" ', 1)
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + rough, idx_map


def _cell_value(v: Any) -> Tuple[str, str]:
    """Определяет тип ячейки ('s' — shared string, 'n' — number) и значение для записи."""
    if v is None or v == "":
        return "s", ""
    if isinstance(v, bool):
        return "s", "true" if v else "false"
    if isinstance(v, (int, float)):
        return "n", str(v)
    return "s", str(v)


# Приблизительная ширина одного символа в единицах Excel (колонка); для автоподбора
_CHAR_WIDTH_UNITS = 1.1


def _column_auto_widths(
    columns: List[str],
    rows: List[Dict[str, Any]],
    width_min: float,
    width_max: float,
    str_index: Dict[str, int],
) -> List[float]:
    """
    Вычисляет ширину каждой колонки по содержимому (заголовок + ячейки).
    Ограничивает width_min и width_max. Единицы — как в Excel (приблизительно символы).
    """
    widths: List[float] = []
    for col_name in columns:
        max_chars = len(col_name)
        for row in rows:
            val = row.get(col_name, "")
            s = str(val) if val is not None else ""
            max_chars = max(max_chars, len(s))
        w = max(width_min, min(width_max, max_chars * _CHAR_WIDTH_UNITS + 1))
        widths.append(round(w, 1))
    return widths


def _row_auto_heights(
    columns: List[str],
    rows: List[Dict[str, Any]],
    header_row: List[str],
    str_index: Dict[str, int],
    wrap_text: bool,
    default_height: float = 15.0,
) -> List[Optional[float]]:
    """
    Вычисляет высоту каждой строки по содержимому (при wrap_text — по числу строк).
    Возвращает список высот: [None] — не задавать, иначе [ht_row1, ht_row2, ...].
    """
    if not wrap_text:
        return [None] * (1 + len(rows))
    heights: List[Optional[float]] = []
    # Строка заголовков
    h_max = 1
    for h in header_row:
        lines = (h or "").count("\n") + 1
        h_max = max(h_max, lines)
    heights.append(max(default_height, h_max * default_height * 0.8))
    for row in rows:
        h_max = 1
        for col_name in columns:
            val = row.get(col_name, "")
            s = str(val) if val is not None else ""
            lines = s.count("\n") + 1
            h_max = max(h_max, min(lines, 10))
        heights.append(max(default_height, h_max * default_height * 0.8))
    return heights


def _to_integer_value(val: Any) -> Optional[float]:
    """Приводит значение к числу для отображения как целое (для формата integer в ячейке)."""
    if val is None or val == "":
        return None
    if isinstance(val, int):
        return float(val)
    if isinstance(val, float):
        return val
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(int(s))
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return None


# Excel serial date: 1900-01-01 = 1 (с учётом встроенной ошибки Excel: 1900 считается високосным)
_EXCEL_EPOCH = datetime(1899, 12, 30)


def _to_date_value(val: Any) -> Optional[float]:
    """
    Приводит значение к числовому формату даты Excel (дни с 1899-12-30).
    Поддерживает: datetime, число (уже сериал), строку ISO (YYYY-MM-DD) или dd.mm.yyyy / dd/mm/yyyy.
    """
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return float((val - _EXCEL_EPOCH).days)
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    # ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00")[:10])
        return float((dt - _EXCEL_EPOCH).days)
    except ValueError:
        pass
    # dd.mm.yyyy или dd/mm/yyyy
    for sep in (".", "/", "-"):
        if sep in s and len(s) >= 8:
            parts = s.split(sep)
            if len(parts) == 3:
                try:
                    d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
                    if y < 100:
                        y += 2000 if y < 50 else 1900
                    dt = datetime(y, m, d)
                    return float((dt - _EXCEL_EPOCH).days)
                except (ValueError, TypeError):
                    pass
    return None


def _date_format_to_excel(date_format: str) -> str:
    """Преобразует описание формата даты (DD.MM.YYYY и т.п.) в formatCode Excel (dd.mm.yyyy)."""
    s = (date_format or "dd.mm.yyyy").strip()
    s = re.sub(r"YYYY", "yyyy", s, flags=re.I)
    s = re.sub(r"YY(?![a-z])", "yy", s, flags=re.I)
    s = re.sub(r"DD", "dd", s, flags=re.I)
    s = re.sub(r"D(?![a-z])", "d", s, flags=re.I)
    s = re.sub(r"MM", "mm", s, flags=re.I)
    s = re.sub(r"M(?![a-z])", "m", s, flags=re.I)
    return s or "dd.mm.yyyy"


# Допустимые значения выравнивания в OOXML (SpreadsheetML)
_HORIZONTAL_MAP = {"left": "left", "center": "center", "right": "right", "general": "general"}
_VERTICAL_MAP = {"top": "top", "center": "center", "bottom": "bottom"}


def _normalize_horizontal(value: Any) -> str:
    """Приводит значение из конфига к OOXML horizontal: left | center | right | general."""
    s = (value or "").strip().lower()
    return _HORIZONTAL_MAP.get(s, "left")


def _normalize_vertical(value: Any) -> str:
    """Приводит значение из конфига к OOXML vertical: top | center | bottom."""
    s = (value or "").strip().lower()
    return _VERTICAL_MAP.get(s, "center")


def write_xlsx(
    sheets_data: List[Tuple[str, List[Dict[str, Any]], List[str]]],
    out_path: Path,
    *,
    freeze_first_row: bool = True,
    freeze_cell: Optional[str] = None,
    freeze_cell_per_sheet: Optional[List[Optional[str]]] = None,
    autofilter: bool = True,
    column_width_mode: str = "auto",
    auto_row_height: bool = False,
    column_widths: Optional[Dict[str, float]] = None,
    column_formats_per_sheet: Optional[List[Dict[str, Dict[str, Any]]]] = None,
    default_formats_per_sheet: Optional[List[Dict[str, Any]]] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Записывает несколько листов в один XLSX.
    column_width_mode: "auto" — автоподбор в пределах width_min..width_max; "minimum"/"maximum" — фиксированная ширина.
    auto_row_height: True — автоподбор высоты строк по содержимому (при wrap_text).
    column_widths: опционально {имя_колонки: ширина} (если задано — перекрывает режим по колонкам).
    """
    log = logger or logging.getLogger(__name__)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Стиль с форматом «целое число» и «дата»
    def _has_integer(cf: Optional[Dict], df: Optional[Dict]) -> bool:
        if cf:
            if any((opts or {}).get("number_format") == "integer" for opts in (cf or {}).values()):
                return True
        if df and (df or {}).get("number_format") == "integer":
            return True
        return False
    def _has_date(cf: Optional[Dict], df: Optional[Dict]) -> bool:
        if cf:
            if any((opts or {}).get("number_format") == "date" for opts in (cf or {}).values()):
                return True
        if df and (df or {}).get("number_format") == "date":
            return True
        return False
    use_integer_style = bool(
        (column_formats_per_sheet or default_formats_per_sheet)
        and any(
            _has_integer(
                (column_formats_per_sheet or [])[i] if i < len(column_formats_per_sheet or []) else None,
                (default_formats_per_sheet or [])[i] if i < len(default_formats_per_sheet or []) else None,
            )
            for i in range(max(len(column_formats_per_sheet or []), len(default_formats_per_sheet or [])))
        )
    )
    use_date_style = bool(
        (column_formats_per_sheet or default_formats_per_sheet)
        and any(
            _has_date(
                (column_formats_per_sheet or [])[i] if i < len(column_formats_per_sheet or []) else None,
                (default_formats_per_sheet or [])[i] if i < len(default_formats_per_sheet or []) else None,
            )
            for i in range(max(len(column_formats_per_sheet or []), len(default_formats_per_sheet or [])))
        )
    )
    # Формат даты для custom numFmt: из первой попавшейся колонки с number_format: "date"
    date_format_code = "dd.mm.yyyy"
    if use_date_style and default_formats_per_sheet:
        for sheet_def in default_formats_per_sheet or []:
            if (sheet_def or {}).get("number_format") == "date":
                date_format_code = _date_format_to_excel((sheet_def or {}).get("date_format") or "dd.mm.yyyy")
                break
    if use_date_style and column_formats_per_sheet:
        for sheet_cf in column_formats_per_sheet or []:
            for opts in (sheet_cf or {}).values():
                if (opts or {}).get("number_format") == "date":
                    date_format_code = _date_format_to_excel((opts or {}).get("date_format") or "dd.mm.yyyy")
                    break

    # Сбор всех строковых значений со всех листов для общей таблицы shared strings
    all_strings: List[str] = []
    for _name, rows, columns in sheets_data:
        all_strings.extend(columns)
        for row in rows:
            for c in columns:
                val = row.get(c, "")
                if isinstance(val, (str, bool)) or val is None or val == "":
                    all_strings.append(
                        "true" if val is True else "false" if val is False else (str(val) if val is not None else "")
                    )
                else:
                    all_strings.append(str(val))
    log.debug("Построение shared strings: всего значений %s [def: write_xlsx]", len(all_strings))
    sst_xml, str_index = _build_shared_strings(all_strings)
    log.debug("Уникальных строк: %s [def: write_xlsx]", len(str_index))

    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    ET.register_namespace("", ns)

    def make_worksheet_xml(
        name: str,
        rows: List[Dict[str, Any]],
        columns: List[str],
        sheet_idx: int,
        sheet_column_format: Optional[Dict[str, Dict[str, Any]]] = None,
        sheet_default_format: Optional[Dict[str, Any]] = None,
        sheet_freeze_cell: Optional[str] = None,
        sheet_column_widths: Optional[List[float]] = None,
        sheet_row_heights: Optional[List[Optional[float]]] = None,
        header_style_idx: int = 0,
        data_style_idx: int = 0,
    ) -> str:
        """Собирает XML одного листа. header_style_idx — стиль первой строки; data_style_idx — стиль ячеек данных (0 = без стиля)."""
        root = ET.Element("worksheet", xmlns=ns)
        dim = f"A1:{_excel_col(len(columns) - 1)}{len(rows) + 1}"
        ET.SubElement(root, "dimension", ref=dim)
        sheetViews = ET.SubElement(root, "sheetViews")
        sheetView = ET.SubElement(sheetViews, "sheetView", workbookViewId="0")
        if sheet_freeze_cell and (rows or columns):
            col_0, row_1 = _parse_cell_ref(sheet_freeze_cell)
            x_split = max(0, col_0)
            y_split = max(0, row_1 - 1)
            top_left = _excel_col(col_0) + str(row_1)
            if y_split > 0 and x_split > 0:
                active_pane = "bottomRight"
            elif y_split > 0:
                active_pane = "bottomLeft"
            elif x_split > 0:
                active_pane = "topRight"
            else:
                active_pane = "topLeft"
            pane_attrs = {"topLeftCell": top_left, "activePane": active_pane, "state": "frozen"}
            if x_split > 0:
                pane_attrs["xSplit"] = str(x_split)
            if y_split > 0:
                pane_attrs["ySplit"] = str(y_split)
            ET.SubElement(sheetView, "pane", **pane_attrs)
            ET.SubElement(sheetView, "selection", pane=active_pane, activeCell=top_left, sqref=top_left)
        sheetFormatPr = ET.SubElement(root, "sheetFormatPr", defaultRowHeight="15")
        sheetData = ET.SubElement(root, "sheetData")

        # Первая строка — заголовки (имена колонок); стиль заголовка если задан
        row0_attrs = {"r": "1"}
        if sheet_row_heights and len(sheet_row_heights) > 0 and sheet_row_heights[0] is not None:
            row0_attrs["ht"] = str(round(sheet_row_heights[0], 2))
            row0_attrs["customHeight"] = "1"
        row0 = ET.SubElement(sheetData, "row", **row0_attrs)
        for col_idx, col_name in enumerate(columns):
            c_attrs = {"r": _cell_ref(col_idx, 1), "t": "s"}
            if header_style_idx > 0:
                c_attrs["s"] = str(header_style_idx)
            c = ET.SubElement(row0, "c", **c_attrs)
            v = ET.SubElement(c, "v")
            v.text = str(str_index.get(col_name, str_index.get(str(col_name), 0)))
        # Строки данных
        for row_idx, row in enumerate(rows):
            r_attrs = {"r": str(row_idx + 2)}
            if sheet_row_heights and row_idx + 1 < len(sheet_row_heights) and sheet_row_heights[row_idx + 1] is not None:
                r_attrs["ht"] = str(round(sheet_row_heights[row_idx + 1], 2))
                r_attrs["customHeight"] = "1"
            r_el = ET.SubElement(sheetData, "row", **r_attrs)
            for col_idx, col_name in enumerate(columns):
                val = row.get(col_name, "")
                col_fmt = (sheet_column_format or {}).get(col_name) or (sheet_default_format or {})
                is_integer_format = use_integer_style and (col_fmt or {}).get("number_format") == "integer"
                is_date_format = use_date_style and (col_fmt or {}).get("number_format") == "date"
                # Правило: если значение не преобразуется в указанный тип формата колонки — ячейка
                # записывается как значение по умолчанию (текст) и со стилем по умолчанию/данных, не формата.
                cell_style_s = None
                if is_integer_format and integer_style_idx > 0:
                    cell_style_s = str(integer_style_idx)
                elif is_date_format and date_style_idx > 0:
                    cell_style_s = str(date_style_idx)
                elif data_style_idx > 0:
                    cell_style_s = str(data_style_idx)
                if is_integer_format:
                    num_val = _to_integer_value(val)
                    if num_val is not None:
                        c_attrs = {"r": _cell_ref(col_idx, row_idx + 2), "t": "n"}
                        if cell_style_s:
                            c_attrs["s"] = cell_style_s
                        c = ET.SubElement(r_el, "c", **c_attrs)
                        v = ET.SubElement(c, "v")
                        v.text = str(int(num_val) if num_val == int(num_val) else num_val)
                    else:
                        cell_type, cell_val = _cell_value(val)
                        if cell_type == "s":
                            cell_val = str(str_index.get(cell_val, 0))
                        c_attrs = {"r": _cell_ref(col_idx, row_idx + 2), "t": cell_type}
                        if data_style_idx > 0:
                            c_attrs["s"] = str(data_style_idx)
                        c = ET.SubElement(r_el, "c", **c_attrs)
                        v = ET.SubElement(c, "v")
                        v.text = cell_val
                elif is_date_format:
                    date_val = _to_date_value(val)
                    if date_val is not None:
                        c_attrs = {"r": _cell_ref(col_idx, row_idx + 2), "t": "n"}
                        if cell_style_s:
                            c_attrs["s"] = cell_style_s
                        c = ET.SubElement(r_el, "c", **c_attrs)
                        v = ET.SubElement(c, "v")
                        v.text = str(int(date_val) if date_val == int(date_val) else date_val)
                    else:
                        cell_type, cell_val = _cell_value(val)
                        if cell_type == "s":
                            cell_val = str(str_index.get(cell_val, 0))
                        c_attrs = {"r": _cell_ref(col_idx, row_idx + 2), "t": cell_type}
                        if data_style_idx > 0:
                            c_attrs["s"] = str(data_style_idx)
                        c = ET.SubElement(r_el, "c", **c_attrs)
                        v = ET.SubElement(c, "v")
                        v.text = cell_val
                else:
                    cell_type, cell_val = _cell_value(val)
                    if cell_type == "s":
                        idx = str_index.get(cell_val, 0)
                        cell_val = str(idx)
                    c_attrs = {"r": _cell_ref(col_idx, row_idx + 2), "t": cell_type}
                    if cell_style_s:
                        c_attrs["s"] = cell_style_s
                    c = ET.SubElement(r_el, "c", **c_attrs)
                    v = ET.SubElement(c, "v")
                    v.text = cell_val

        if autofilter and rows and columns:
            ET.SubElement(root, "autoFilter", ref=dim)
        ET.SubElement(
            root,
            "pageMargins",
            left="0.7",
            right="0.7",
            top="0.75",
            bottom="0.75",
            header="0.3",
            footer="0.3",
        )
        # Элемент cols (ширина колонок) по спецификации должен идти до sheetData
        cols_el = ET.SubElement(root, "cols")
        for col_idx in range(len(columns)):
            w = 12.0
            if sheet_column_widths and col_idx < len(sheet_column_widths):
                w = sheet_column_widths[col_idx]
            elif column_widths and columns[col_idx] in column_widths:
                w = max(0, min(column_widths[columns[col_idx]], 255))
            w = max(0, min(float(w), 255))
            ET.SubElement(cols_el, "col", min=str(col_idx + 1), max=str(col_idx + 1), width=str(round(w, 1)), customWidth="1")
        root.remove(cols_el)
        idx_sf = list(root).index(sheetFormatPr)
        root.insert(idx_sf + 1, cols_el)

        rough = ET.tostring(root, encoding="unicode", default_namespace="")
        return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + rough

    # Ячейка закрепления по умолчанию: freeze_cell или при freeze_first_row — "A2"
    default_freeze = (freeze_cell or "").strip() or ("A2" if freeze_first_row else "")
    width_mode = (column_width_mode or "auto").strip().lower()
    if width_mode not in ("auto", "minimum", "maximum"):
        width_mode = "auto"
    # Стили: строим до цикла по листам, чтобы знать индексы для заголовка и данных
    df0 = (default_formats_per_sheet or [{}])[0] if default_formats_per_sheet else {}
    header_style = bool(
        df0.get("header_bold")
        or (df0.get("header_fg_color") or "").strip()
        or (df0.get("header_bg_color") or "").strip()
    )
    data_style_needed = bool(
        (df0.get("data_horizontal_align") or df0.get("data_vertical_align") or df0.get("wrap_text"))
        or (df0.get("data_fg_color") or "").strip()
        or (df0.get("data_bg_color") or "").strip()
    )
    # Сборка styles.xml до цикла по листам, чтобы получить индексы стилей
    header_fg = "000000"
    header_bg = "C6EFCE"
    if default_formats_per_sheet and len(default_formats_per_sheet) > 0:
        df = default_formats_per_sheet[0] or {}
        if (df.get("header_fg_color") or "").strip():
            header_fg = (df.get("header_fg_color") or "").strip().lstrip("#")[:6]
        if (df.get("header_bg_color") or "").strip():
            header_bg = (df.get("header_bg_color") or "").strip().lstrip("#")[:6]
        if (df.get("data_fg_color") or "").strip():
            data_fg = (df.get("data_fg_color") or "").strip().lstrip("#")[:6]
        else:
            data_fg = "000000"
        if (df.get("data_bg_color") or "").strip():
            data_bg = (df.get("data_bg_color") or "").strip().lstrip("#")[:6]
        else:
            data_bg = ""
    else:
        data_fg = "000000"
        data_bg = ""
    # Выравнивание из конфига (заголовок и данные)
    h_hor = _normalize_horizontal(df0.get("header_horizontal_align"))
    h_ver = _normalize_vertical(df0.get("header_vertical_align"))
    d_hor = _normalize_horizontal(df0.get("data_horizontal_align"))
    d_ver = _normalize_vertical(df0.get("data_vertical_align"))
    wrap_text = bool(df0.get("wrap_text", False))
    # Шрифты: 0=обычный, 1=жирный заголовок, 2=данные (опционально с цветом)
    fonts_parts = ['<font/>', '<font><b/><color rgb="FF' + header_fg.upper() + '"/></font>']
    if data_style_needed and (data_fg != "000000" or data_bg or df0.get("data_bold") or df0.get("data_italic")):
        font_data = "<font>"
        if df0.get("data_bold"):
            font_data += "<b/>"
        if df0.get("data_italic"):
            font_data += "<i/>"
        if data_fg and data_fg != "000000":
            font_data += '<color rgb="FF' + data_fg.upper() + '"/>'
        font_data += "</font>"
        fonts_parts.append(font_data)
    fonts_count = len(fonts_parts)
    fonts_xml = "<fonts count=\"" + str(fonts_count) + "\">" + "".join(fonts_parts) + "</fonts>"
    # Заливки: 0=none, 1=gray125, 2=заголовок, 3=данные (если задана)
    fills_parts = [
        "<fill><patternFill patternType=\"none\"/></fill>",
        "<fill><patternFill patternType=\"gray125\"/></fill>",
        '<fill><patternFill patternType="solid"><fgColor rgb="FF' + header_bg.upper() + '"/></patternFill></fill>',
    ]
    if data_style_needed and data_bg:
        fills_parts.append('<fill><patternFill patternType="solid"><fgColor rgb="FF' + data_bg.upper() + '"/></patternFill></fill>')
    fills_count = len(fills_parts)
    fills_xml = "<fills count=\"" + str(fills_count) + "\">" + "".join(fills_parts) + "</fills>"
    borders_xml = '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
    # Пользовательский формат даты (numFmtId 164) — только если есть колонки с number_format: "date"
    DATE_NUMFMT_ID = 164
    numfmts_xml = ""
    if use_date_style:
        numfmts_xml = '<numFmts count="1"><numFmt numFmtId="' + str(DATE_NUMFMT_ID) + '" formatCode="' + _escape(date_format_code) + '"/></numFmts>'
    # cellXfs: 0=по умолчанию, 1=integer (если нужен), 2=date (если нужен), 3=заголовок, 4=данные
    xfs_list: List[str] = []
    idx = 0
    xfs_list.append('<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>')
    idx += 1
    if use_integer_style:
        integer_style_idx = idx
        if data_style_needed:
            xfs_list.append(
                '<xf numFmtId="1" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
                '<alignment horizontal="' + d_hor + '" vertical="' + d_ver + '" wrapText="' + ("1" if wrap_text else "0") + '"/>'
                "</xf>"
            )
        else:
            xfs_list.append('<xf numFmtId="1" fontId="0" fillId="0" borderId="0" xfId="0"/>')
        idx += 1
    else:
        integer_style_idx = 0
    if use_date_style:
        date_style_idx = idx
        if data_style_needed:
            xfs_list.append(
                '<xf numFmtId="' + str(DATE_NUMFMT_ID) + '" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
                '<alignment horizontal="' + d_hor + '" vertical="' + d_ver + '" wrapText="' + ("1" if wrap_text else "0") + '"/>'
                "</xf>"
            )
        else:
            xfs_list.append('<xf numFmtId="' + str(DATE_NUMFMT_ID) + '" fontId="0" fillId="0" borderId="0" xfId="0"/>')
        idx += 1
    else:
        date_style_idx = 0
    if header_style:
        style_header_idx = idx
        xfs_list.append(
            '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyAlignment="1">'
            '<alignment horizontal="' + h_hor + '" vertical="' + h_ver + '"/>'
            "</xf>"
        )
        idx += 1
    else:
        style_header_idx = 0
    if data_style_needed:
        style_data_idx = idx
        # Шрифт данных: индекс 2 только если добавлен третий шрифт (data_fg/bold/italic)
        font_id_data = "2" if fonts_count >= 3 else "0"
        fill_id_data = "3" if (data_bg and fills_count >= 4) else "0"
        xfs_list.append(
            '<xf numFmtId="0" fontId="' + font_id_data + '" fillId="' + fill_id_data + '" borderId="0" xfId="0" applyAlignment="1">'
            '<alignment horizontal="' + d_hor + '" vertical="' + d_ver + '" wrapText="' + ("1" if wrap_text else "0") + '"/>'
            "</xf>"
        )
        idx += 1
    else:
        style_data_idx = 0
    cellxfs_str = "".join(xfs_list)
    styles_xml_pre = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  """ + (numfmts_xml + "\n  " if numfmts_xml else "") + fonts_xml + """
  """ + fills_xml + """
  """ + borders_xml + """
  <cellXfs count=\"""" + str(len(xfs_list)) + "\">" + cellxfs_str + """</cellXfs>
</styleSheet>"""

    # Генерация XML для каждого листа
    sheet_xmls = []
    for sheet_idx, (name, rows, columns) in enumerate(sheets_data):
        sheet_cf = column_formats_per_sheet[sheet_idx] if column_formats_per_sheet and sheet_idx < len(column_formats_per_sheet) else None
        sheet_def = default_formats_per_sheet[sheet_idx] if default_formats_per_sheet and sheet_idx < len(default_formats_per_sheet) else None
        sheet_freeze = None
        if freeze_cell_per_sheet and sheet_idx < len(freeze_cell_per_sheet) and (freeze_cell_per_sheet[sheet_idx] or "").strip():
            sheet_freeze = (freeze_cell_per_sheet[sheet_idx] or "").strip()
        elif default_freeze:
            sheet_freeze = default_freeze
        # Ширина колонок по режиму (width_min/width_max из default_column_format листа)
        def_fmt = sheet_def or {}
        w_min = float(def_fmt.get("width_min", 8))
        w_max = float(def_fmt.get("width_max", 50))
        w_min = max(0, min(w_min, 255))
        w_max = max(0, min(w_max, 255))
        if w_min > w_max:
            w_min, w_max = w_max, w_min
        sheet_column_widths = None
        if columns:
            if width_mode == "auto":
                sheet_column_widths = _column_auto_widths(columns, rows, w_min, w_max, str_index)
            elif width_mode == "minimum":
                sheet_column_widths = [w_min] * len(columns)
            else:
                sheet_column_widths = [w_max] * len(columns)
        # Автовысота строк (при включении и при wrap_text в формате)
        wrap = bool(def_fmt.get("wrap_text", False))
        sheet_row_heights = None
        if auto_row_height and (rows or columns):
            header_row = list(columns)
            sheet_row_heights = _row_auto_heights(columns, rows, header_row, str_index, wrap)
        sheet_xmls.append(make_worksheet_xml(
            name, rows, columns, sheet_idx,
            sheet_column_format=sheet_cf, sheet_default_format=sheet_def, sheet_freeze_cell=sheet_freeze,
            sheet_column_widths=sheet_column_widths, sheet_row_heights=sheet_row_heights,
            header_style_idx=style_header_idx,
            data_style_idx=style_data_idx,
        ))
    log.debug("Сформировано листов: %s [def: write_xlsx]", len(sheet_xmls))

    # Связи workbook: листы, sharedStrings, styles
    wb_rels_parts = []
    for i in range(len(sheets_data)):
        wb_rels_parts.append(
            f'  <Relationship Id="rId{i+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i+1}.xml"/>'
        )
    wb_rels_parts.append(
        '  <Relationship Id="rId{}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'.format(
            len(sheets_data) + 1
        )
    )
    wb_rels_parts.append(
        '  <Relationship Id="rId{}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'.format(
            len(sheets_data) + 2
        )
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
        + "\n".join(wb_rels_parts)
        + "\n</Relationships>"
    )

    # workbook.xml: перечисление листов
    workbook_sheets = []
    for i, (name, _, _) in enumerate(sheets_data):
        safe_name = re.sub(r'[\\/*?:\[\]]', "_", name)[:31]
        workbook_sheets.append(f'  <sheet name="{_escape(safe_name)}" sheetId="{i+1}" r:id="rId{i+1}"/>')
    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
""" + "\n".join(workbook_sheets) + """
  </sheets>
</workbook>"""

    # [Content_Types].xml: типы частей пакета
    content_types_parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '  <Default Extension="xml" ContentType="application/xml"/>',
        '  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
    ]
    for i in range(1, len(sheets_data) + 1):
        content_types_parts.append(
            f'  <Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    content_types_parts.extend([
        '  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>',
        '  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        '  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
        '</Types>',
    ])
    content_types = "\n".join(content_types_parts)

    # Используем стили, собранные выше (styles_xml_pre)
    styles_xml = styles_xml_pre

    docProps_core = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>JSON Parser</dc:creator>
  <dcterms:created xsi:type="dcterms:W3CDTF">2020-01-01T00:00:00Z</dcterms:created>
</cp:coreProperties>"""
    docProps_app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
  <Application>JSON to XLSX</Application>
</Properties>"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""

    # Сборка ZIP-архива (XLSX = ZIP с фиксированной структурой)
    log.debug("Запись ZIP-архива XLSX [def: write_xlsx]")
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("docProps/core.xml", docProps_core)
        zf.writestr("docProps/app.xml", docProps_app)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/sharedStrings.xml", sst_xml)
        zf.writestr("xl/styles.xml", styles_xml)
        for i, xml in enumerate(sheet_xmls):
            zf.writestr(f"xl/worksheets/sheet{i+1}.xml", xml)

    log.info("Записан XLSX: %s, листов: %s [def: write_xlsx]", out_path, len(sheets_data))
