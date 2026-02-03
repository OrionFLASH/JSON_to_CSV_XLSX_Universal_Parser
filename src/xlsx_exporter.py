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


def write_xlsx(
    sheets_data: List[Tuple[str, List[Dict[str, Any]], List[str]]],
    out_path: Path,
    *,
    freeze_first_row: bool = True,
    freeze_cell: Optional[str] = None,
    freeze_cell_per_sheet: Optional[List[Optional[str]]] = None,
    autofilter: bool = True,
    column_widths: Optional[Dict[str, float]] = None,
    column_formats_per_sheet: Optional[List[Dict[str, Dict[str, Any]]]] = None,
    default_formats_per_sheet: Optional[List[Dict[str, Any]]] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Записывает несколько листов в один XLSX.
    sheets_data: список кортежей (имя_листа, строки, колонки).
    freeze_cell: ячейка-граница закрепления по умолчанию для всех листов (например "A2" — закрепить первую строку).
    freeze_cell_per_sheet: для каждого листа своя ячейка (переопределяет freeze_cell); null/отсутствие — использовать общую.
    column_widths: опционально {имя_колонки: ширина}.
    column_formats_per_sheet: для каждого листа словарь {имя_колонки: {number_format: "integer", ...}}.
    default_formats_per_sheet: для каждого листа формат по умолчанию для всех колонок (если колонка не в column_format).
    """
    log = logger or logging.getLogger(__name__)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Стиль с форматом «целое число»: numFmtId 1 в Excel = "0" (целое)
    def _has_integer(cf: Optional[Dict], df: Optional[Dict]) -> bool:
        if cf:
            if any((opts or {}).get("number_format") == "integer" for opts in cf.values()):
                return True
        if df and (df or {}).get("number_format") == "integer":
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
    ) -> str:
        """Собирает XML одного листа. Формат ячейки: column_format[col] или default_column_format. Закрепление: sheet_freeze_cell (граница)."""
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

        # Первая строка — заголовки (имена колонок)
        row0 = ET.SubElement(sheetData, "row", r="1")
        for col_idx, col_name in enumerate(columns):
            c = ET.SubElement(row0, "c", r=_cell_ref(col_idx, 1), t="s")
            v = ET.SubElement(c, "v")
            v.text = str(str_index.get(col_name, str_index.get(str(col_name), 0)))
        # Строки данных
        for row_idx, row in enumerate(rows):
            r_el = ET.SubElement(sheetData, "row", r=str(row_idx + 2))
            for col_idx, col_name in enumerate(columns):
                val = row.get(col_name, "")
                col_fmt = (sheet_column_format or {}).get(col_name) or (sheet_default_format or {})
                is_integer_format = use_integer_style and (col_fmt or {}).get("number_format") == "integer"
                if is_integer_format:
                    num_val = _to_integer_value(val)
                    if num_val is not None:
                        c = ET.SubElement(r_el, "c", r=_cell_ref(col_idx, row_idx + 2), t="n", s="1")
                        v = ET.SubElement(c, "v")
                        v.text = str(int(num_val) if num_val == int(num_val) else num_val)
                    else:
                        cell_type, cell_val = _cell_value(val)
                        if cell_type == "s":
                            cell_val = str(str_index.get(cell_val, 0))
                        c = ET.SubElement(r_el, "c", r=_cell_ref(col_idx, row_idx + 2), t=cell_type)
                        v = ET.SubElement(c, "v")
                        v.text = cell_val
                else:
                    cell_type, cell_val = _cell_value(val)
                    if cell_type == "s":
                        idx = str_index.get(cell_val, 0)
                        cell_val = str(idx)
                    c = ET.SubElement(r_el, "c", r=_cell_ref(col_idx, row_idx + 2), t=cell_type)
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
            if column_widths and columns[col_idx] in column_widths:
                w = max(0, min(column_widths[columns[col_idx]], 255))
            ET.SubElement(cols_el, "col", min=str(col_idx + 1), max=str(col_idx + 1), width=str(w), customWidth="1")
        root.remove(cols_el)
        idx_sf = list(root).index(sheetFormatPr)
        root.insert(idx_sf + 1, cols_el)

        rough = ET.tostring(root, encoding="unicode", default_namespace="")
        return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + rough

    # Ячейка закрепления по умолчанию: freeze_cell или при freeze_first_row — "A2"
    default_freeze = (freeze_cell or "").strip() or ("A2" if freeze_first_row else "")
    # Генерация XML для каждого листа
    sheet_xmls: List[str] = []
    for sheet_idx, (name, rows, columns) in enumerate(sheets_data):
        sheet_cf = column_formats_per_sheet[sheet_idx] if column_formats_per_sheet and sheet_idx < len(column_formats_per_sheet) else None
        sheet_def = default_formats_per_sheet[sheet_idx] if default_formats_per_sheet and sheet_idx < len(default_formats_per_sheet) else None
        sheet_freeze = None
        if freeze_cell_per_sheet and sheet_idx < len(freeze_cell_per_sheet) and (freeze_cell_per_sheet[sheet_idx] or "").strip():
            sheet_freeze = (freeze_cell_per_sheet[sheet_idx] or "").strip()
        elif default_freeze:
            sheet_freeze = default_freeze
        sheet_xmls.append(make_worksheet_xml(name, rows, columns, sheet_idx, sheet_column_format=sheet_cf, sheet_default_format=sheet_def, sheet_freeze_cell=sheet_freeze))
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

    # styles.xml: при формате integer добавляем второй стиль с numFmtId="1" (целое число)
    if use_integer_style:
        styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font/></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="1" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>"""
    else:
        styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font/></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>"""

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
