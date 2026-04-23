# Универсальный парсер JSON в CSV и XLSX

## Задача и ТЗ

Программа предназначена для пакетного преобразования произвольных JSON-файлов в табличный вид:

- **CSV** — отдельный CSV-файл на каждый входной JSON (в папке `OUT`), имена с таймштампом.
- **XLSX** — один файл `<base_name>_<timestamp>.xlsx` с несколькими листами: каждый обработанный JSON выводится на свой лист.

Требования: универсальность структуры JSON, настраиваемые имена колонок (path_start, exclude_keys, include_only_keys), заморозка областей и форматирование в XLSX, логирование (INFO/DEBUG), параллельная обработка, только стандартная библиотека Python (без pip).

---

## Требования к среде (без pip install)

Проект **не использует сторонние пакеты**. Достаточно:

- **Python 3.7+** из официальной поставки (python.org) или **Anaconda 3.10** (и любых версий Anaconda с Python 3.7+). Установка дополнительных модулей через `pip install` **запрещена и не требуется**.

Используются только модули стандартной библиотеки:

| Модуль | Назначение |
|--------|------------|
| `json` | Чтение и разбор JSON. |
| `csv` | Запись CSV. |
| `logging` | Логирование. |
| `pathlib` | Работа с путями. |
| `typing` | Аннотации типов. |
| `datetime` | Таймштамп в именах файлов. |
| `multiprocessing` | Параллельная обработка файлов. |
| `zipfile` | Сборка XLSX (архив). |
| `xml.etree.ElementTree` | Формирование XML листов и стилей XLSX. |
| `re` | Имена листов, экранирование. |
| `sys` | Путь в тестах. |

Файла `requirements.txt` в проекте нет — зависимости из стандартной библиотеки не перечисляются.

---

## Что делает программа (пошагово)

1. **Чтение конфига** — из `config.json` загружаются пути (`input_dir`, `output_dir`), список файлов из массива `files[]` (только записи с **`enabled`: true**), параметры разбора и экспорта (CSV, XLSX).
2. **Обработка каждого файла** (параллельно или последовательно):
   - JSON читается из `input_dir`/`имя_файла`;
   - определяется массив «строк» (записей): либо стандартно через `extract_rows`, либо через декларативный `row_builder` (join по ключам между ветками JSON);
   - каждая запись разворачивается в плоский словарь (flatten) с учётом `path_start`/`path_starts`, `exclude_keys`, `exclude_keys_in_path`, `include_only_keys`;
   - при наличии `key_fields` вычисляются дополнительные «ключевые» колонки с цепочками fallback (`sources`) и значением по умолчанию (`default`);
   - формируется общий список колонок по всем строкам (порядок: по ключам, внутри массива объектов — (1), (2), (3)…);
   - таблица записывается в CSV в `output_dir` с именем `{имя_файла}_{таймштамп}.csv`;
   - для XLSX возвращаются данные листа: имя листа, строки, колонки.
3. **Сборка XLSX** — один файл `{output_file_base_name}_{таймштамп}.xlsx` с листом на каждый обработанный JSON; к листам применяются настройки из **`files[].sheet`** (закрепление, форматы колонок). Устаревший вариант — глобальный массив `xlsx.sheets[]`, если у записи нет вложенного `sheet`.
4. **Логи** пишутся в папку `log` (INFO и DEBUG с указанием функции/класса).

---

## Структура проекта

```
.
├── config.json          # Конфигурация (только данные; описание ключей — в README)
├── main.py              # Точка входа
├── IN/                  # Входные JSON-файлы
├── OUT/                 # Результаты: CSV и <base_name>_<timestamp>.xlsx
├── log/                 # Логи INFO и DEBUG
├── src/
│   ├── config_loader.py # Загрузка config.json, get_enabled_files_with_indices, get_sheet_options
│   ├── csv_exporter.py  # Запись CSV
│   ├── json_flattener.py# Развёртывание JSON (path_start/path_starts, exclude_keys, exclude_keys_in_path, массивы объектов (1),(2)…)
│   ├── logging_setup.py # Настройка логов
│   ├── worker.py        # Обработка одного файла (flatten + CSV)
│   ├── xlsx_exporter.py # Запись XLSX (стили, выравнивание, форматы колонок)
│   └── Tests/
│       └── test_flattener.py
└── README.md
```

---

## Функции (подробно)

### main.py

- **`main()`**  
  Точка входа: настраивает логирование, загружает конфиг, формирует список файлов из `input_dir` по записям **`files[]`** с **`enabled`: true**, запускает обработку (пул процессов или цикл), собирает данные листов и вызывает `xlsx_exporter.write_xlsx`. При отсутствии файлов или папки завершает работу с сообщением в лог.

- **`_process_one_file_standalone(json_path_str, base_dir_str, config)`**  
  Обёртка для дочернего процесса: вызывает `worker.process_one_file`, возвращает `(имя_листа, строки, колонки)`. Используется в `multiprocessing.Pool.starmap`.

---

### src.config_loader

- **`load_config(config_path=None)`**  
  Загружает `config.json` по пути (по умолчанию — корень проекта). При ошибке или отсутствии файла возвращает конфиг по умолчанию с пустым **`files`**. Объединяет вложенные секции `csv` и `xlsx` с дефолтами (`DEFAULT_CSV`, `DEFAULT_XLSX`). Возвращает словарь с ключами: `input_dir`, `output_dir`, `output_file_base_name`, `output_timestamp_format`, `files`, `path_separator`, `path_start`, `exclude_keys`, `include_only_keys`, `csv`, `xlsx`.

- **`get_enabled_files_with_indices(config)`**  
  Возвращает список кортежей `(имя_файла, индекс_в_config["files"])` только для записей с **`enabled`: true** (и без **`disabled`: true**). Индекс совпадает с позицией в полном массиве `files[]`, чтобы `get_file_options` и `get_sheet_options` брали ту же запись.

- **`get_files_list(config)`**  
  Список имён файлов для обработки — по сути имена из `get_enabled_files_with_indices` (без индексов).

- **`get_file_options(config, file_index, default_sheet_name)`**  
  Настройки разбора для `config["files"][file_index]`: path_start, path_starts, exclude_keys, exclude_keys_in_path, column_order, `key_fields`, `row_builder`, output, sheet_name, объект `sheet_format` (результат `get_sheet_options` для XLSX).

- **`get_sheet_options(config, file_index)`**  
  Настройки оформления листа: приоритет у вложенного **`config["files"][file_index]["sheet"]`**; поле **`name`** в результате берётся из **`sheet_name`** записи файла. Если `sheet` нет — используется **`config["xlsx"]["sheets"][file_index]`** (обратная совместимость). Словарь: `name`, `freeze_cell`, `columns`, `default_column_format`, `column_format`.

---

### src.json_flattener

- **`extract_rows(data)`**  
  Определяет массив записей (строк таблицы) по структуре JSON. Если корень — массив, возвращает его. Если корень — объект, ищет ключи `results`, `data`, `items`, `rows`; если значение — массив, возвращает его. Иначе возвращает список из одного элемента `[data]`.  
  *Пример:* `{"data": {"body": [...]}}` — массив не на верхнем уровне; для извлечения строк используется `path_start: ["data", "body"]` при обходе.

- **`flatten_row(row, path_sep, exclude_keys, exclude_keys_in_path)`**  
  Разворачивает один объект (словарь) в плоский словарь «имя колонки → значение». Имя колонки = путь ключей через `path_sep` (например `"key1 - key2"`). Рекурсивно обходит вложенные объекты и массивы; скаляры записываются в ячейку, массивы примитивов — в строку через запятую, **массивы объектов** — в отдельные колонки с суффиксом ` - (1)`, ` - (2)` и т.д. Ключи из `exclude_keys` (по имени или по полному пути) пропускаются. **`exclude_keys_in_path`** — список правил `{"path": "имя_пути", "keys": ["key1", "key2"]}`: указанные ключи исключаются только внутри этого пути (в т.ч. внутри элементов массива объектов с таким именем, например `id` только в `agileManagers`, но не в `agileTree`).

- **`flatten_json_data(data, path_sep, path_start, path_starts, exclude_keys, exclude_keys_in_path, include_only_keys, column_order, key_fields, row_builder)`**  
  Полный разворот: извлекает строки через `extract_rows`, при наличии `path_starts` первый путь — источник строк, остальные мержатся в строку с префиксом; при одном `path_start` — спуск по цепочке. Разворачивает каждую запись с учётом `exclude_keys` и `exclude_keys_in_path`. Объединяет колонки по всем строкам, сортирует их: сначала по первому сегменту пути (ключу), внутри ключа для колонок из массивов объектов — по индексу (1), (2), (3)…; при непустом `include_only_keys` оставляет только указанные колонки. **`column_order`:** перечисленные элементы задают порядок колонок: если элемент совпадает с именем колонки — она ставится в указанную позицию; если нет — элемент трактуется как **префикс**: в этот блок подтягиваются все колонки, начинающиеся с `префикс + " - "` (например `emails` → `emails - address - (1)`, `emails - address - (2)` и т.д.), в порядке по ключам и (1),(2),(3). Остальные колонки идут после. Возвращает `(rows, columns)`.
  
  Дополнительно:
  - **`key_fields`** — вычисляемые колонки (несколько fallback-источников + default).
  - **`row_builder`** — декларативная сборка строк из разных узлов JSON с `join` по ключам (например `employeeId` ↔ `empId`), переносом полей корня (`carry_root_fields`) и алиасами ключей (`key_aliases`).

- **`load_and_flatten(json_path, path_sep, path_start, path_starts, exclude_keys, exclude_keys_in_path, include_only_keys, column_order, key_fields, row_builder, logger)`**  
  Читает JSON из файла, вызывает `flatten_json_data` с переданными параметрами, логирует число строк и колонок. Возвращает `(rows, columns)`.

---

### src.csv_exporter

- **`write_csv(rows, columns, out_path, encoding=..., delimiter=..., lineterminator=..., logger=...)`**  
  Записывает таблицу в CSV: первая строка — заголовки (`columns`), далее по одной строке на элемент `rows`. Значения берутся как `row.get(col, "")` в порядке колонок. Создаёт родительскую папку при необходимости. По умолчанию `encoding="utf-8-sig"` (BOM для Excel), `delimiter=";"`, `lineterminator="\n"`.

---

### src.xlsx_exporter

- **`write_xlsx(sheets_data, out_path, freeze_first_row, freeze_cell, freeze_cell_per_sheet, autofilter, column_width_mode, auto_row_height, column_widths, column_formats_per_sheet, default_formats_per_sheet, logger)`**  
  Записывает один XLSX с несколькими листами. `sheets_data` — список кортежей `(имя_листа, rows, columns)`. Собирает общую таблицу shared strings, строит для каждого листа XML листа (заголовок, строки данных, закрепление, автофильтр, ширина колонок, высота строк при `auto_row_height`). Стили (шрифты, заливки, выравнивание) берутся из `default_formats_per_sheet[0]` и применяются к заголовку и ячейкам данных. Для колонок из `column_formats_per_sheet` / `default_formats_per_sheet` применяются **`number_format: "integer"`** (целое число) и **`number_format: "date"`** (дата; при задании **`date_format`** в формате YYYY-MM-DD, DD.MM.YYYY и т.д. — отображение в ячейке по этому шаблону). **Обработка исключений:** если значение ячейки не удаётся преобразовать в указанный тип формата колонки (integer, date или текст в числовой/датовой колонке), ячейка записывается как значение по умолчанию (текст) и со стилем по умолчанию/данных, без применения формата колонки. **Закрепление:** для каждого листа используется `freeze_cell_per_sheet[j]`, если задан, иначе общий `freeze_cell` (или A2 при `freeze_first_row`). В ZIP попадают `[Content_Types].xml`, `xl/workbook.xml`, `xl/styles.xml`, `xl/sharedStrings.xml`, `xl/worksheets/sheetN.xml`.

Вспомогательные функции (внутри модуля):

- **`_normalize_horizontal(value)`** — приводит значение из конфига к OOXML: `left` | `center` | `right` (по умолчанию `left`).
- **`_normalize_vertical(value)`** — приводит к OOXML: `top` | `center` | `bottom` (по умолчанию `center`).
- **`_column_auto_widths(...)`** — вычисляет ширину колонок по содержимому в пределах `width_min`..`width_max`.
- **`_row_auto_heights(...)`** — при `wrap_text` вычисляет высоту строк по числу переносов.
- **`_to_integer_value(val)`** — приводит значение к числу для формата «целое число» в ячейке; при неудаче возвращает `None` (тогда ячейка записывается как текст по умолчанию).
- **`_to_date_value(val)`** — приводит значение к числовому формату даты Excel (дни с 1899-12-30). Поддерживает: `datetime`, число, строку в форматах ISO (YYYY-MM-DD), dd.mm.yyyy, dd/mm/yyyy. При неудаче возвращает `None`.
- **`_date_format_to_excel(date_format)`** — преобразует описание формата (DD.MM.YYYY, YYYY-MM-DD и т.п.) в `formatCode` Excel для стиля ячейки.

---

### src.worker

- **`process_one_file(json_path, base_dir, config, logger)`**  
  Обрабатывает один JSON: вызывает `json_flattener.load_and_flatten` с параметрами из конфига, записывает CSV через `csv_exporter.write_csv` в `output_dir` с именем `{stem}_{run_timestamp}.csv`. Возвращает `(имя_листа, rows, columns, путь_к_csv)`.

---

## Конфиг (config.json) — подробно с примерами

Файл **`config.json`** содержит только рабочие данные (без блока **`__comments`** и без справочника **`column_formats`**): весь текст с пояснениями и допустимыми значениями параметров собран **в этом разделе README**.

### Корень конфига: ключи верхнего уровня

| Ключ | Назначение |
|------|------------|
| **input_dir** | Каталог с входными JSON (относительно корня проекта), например `"IN"`. |
| **output_dir** | Каталог для CSV и общего XLSX, например `"OUT"`. |
| **output_file_base_name** | Базовое имя выходного XLSX (без расширения и timestamp), например `"json_to_xlsx"`. |
| **output_timestamp_format** | Формат времени для имён выходных файлов (`strftime`), например `"%Y%m%d-%H%M%S"`. |
| **path_separator** | Строка-разделитель сегментов в имени колонки (часто `" - "`). |
| **path_start** | Глобальная цепочка ключей старта разбора; переопределяется в **`files[].path_start`**. |
| **exclude_keys** | Глобальный список исключаемых ключей/путей; переопределяется в **`files[].exclude_keys`**. |
| **include_only_keys** | Если не пусто — в выход только перечисленные колонки (глобально для всех файлов без переопределения в `files`). |
| **key_fields** | Глобальные вычисляемые ключевые поля (fallback-цепочки источников и default). Можно переопределить в `files[]`. |
| **row_builder** | Глобальные правила сборки строк с join по ключам между ветками JSON. Можно переопределить в `files[]`. |
| **files** | Массив настроек по файлам: **`file`**, **`sheet_name`**, **`enabled`**, **`sheet`**, **`output`**, **`path_start`**, **`path_starts`**, **`exclude_keys`**, **`exclude_keys_in_path`**, **`column_order`**, **`key_fields`**, **`row_builder`** — см. подраздел ниже. |
| **csv** | **`encoding`**, **`delimiter`**, **`lineterminator`** — см. таблицу CSV. |
| **xlsx** | **`freeze_first_row`**, **`freeze_cell`**, **`freeze_pane_per_sheet`**, **`column_width_mode`**, **`auto_row_height`**, **`autofilter`**, **`sheets`** (устаревший глобальный список листов, если нет **`files[].sheet`**). |

---

### Входные данные и разбор (глобальные поля)

| Параметр | Тип | Примеры | Влияние на результат |
|----------|-----|---------|----------------------|
| **input_dir** | строка | `"IN"`, `"input"`, `"data/json"` | Папка, из которой читаются файлы по именам **`files[].file`** (только записи с **`enabled`: true**). |
| **output_dir** | строка | `"OUT"`, `"export"` | Папка для CSV и одного XLSX. Создаётся при записи. |
| **output_file_base_name** | строка | `"json_to_xlsx"`, `"report"`, `"addressbook"` | Базовое имя XLSX. Итог: `<base_name>_<timestamp>.xlsx`. |
| **output_timestamp_format** | строка | `"%Y%m%d-%H%M%S"`, `"%Y-%m-%d_%H-%M"` | Формат timestamp для имён XLSX/CSV (`datetime.strftime`). |
| **path_separator** | строка | `" - "`, `"|"`, `"."`, `"__"` | Разделитель между ключами в имени колонки. Например при пути `a.b.c` и разделителе `" - "` колонка будет `a - b - c`. |
| **path_start** | массив строк | `[]`, `["data","body"]`, `["results","item"]` | Цепочка ключей, с которой начинать разбор. Имена колонок и данные строятся от этого поддерева; префикс «data - body -» в именах колонок не появляется. Если в записи путь не найден, строка разворачивается от корня записи. |
| **exclude_keys** | массив строк | `[]`, `["photoData"]`, `["colorCode - secondary"]` | Ключи или полные пути (после path_start), которые не попадают в выход. По имени ключа исключается весь поддерево по этому ключу; по пути — только эта ветка. |
| **include_only_keys** | массив строк | `[]`, `["firstName","lastName","employeeNumber"]` | Если не пусто — в выход идут только эти колонки (имена после path_start). Пустой массив = все колонки с учётом exclude_keys. |

**Пример влияния path_start:**

- JSON: `{"data":{"body":[{"name":"A","meta":{"id":1}}]}}`, `path_start: ["data","body"]` → строки из `body`, колонки `name`, `meta - id`.
- Тот же JSON, `path_start: []` → одна строка, колонки вида `data - body - 0 - name`, `data - body - 0 - meta - id` (если extract_rows вернёт массив по ключу `data` и т.д., зависит от структуры).

---

### Настройки по файлам (опционально)

Если в конфиге задан массив **`files`**, список **обрабатываемых** файлов строится только из записей с **`enabled`: true** (по умолчанию, если ключ не указан, запись считается включённой). Записи с **`enabled`: false** или **`disabled`: true** хранятся в конфиге, но не читаются из `input_dir` и не попадают в XLSX.

Каждый элемент **`files[]`** может содержать:

| Параметр | Примеры | Влияние |
|----------|---------|---------|
| **enabled** | `true`, `false` | `false` — пропустить файл. Альтернатива: **`disabled`: true**. |
| **file** | `"profiles.json"` | Имя файла из input_dir (обязательное поле в элементе). |
| **sheet_name** | `"Профили"`, `"Лист A"` | Имя листа в XLSX для этого файла (до 31 символа). Если не задано — из имени файла. |
| **sheet** | объект | Оформление листа XLSX для этого файла: **`freeze_cell`**, **`columns`**, **`default_column_format`**, **`column_format`** (см. ниже). Имя листа в интерфейсе Excel — из **`sheet_name`**; поле **`name`** внутри `sheet` в JSON можно не дублировать (подставляется из `sheet_name`). |
| **output** | `["csv"]`, `["xlsx"]`, `["csv","xlsx"]` | Какие выходы создавать: только CSV, только XLSX или оба (по умолчанию оба). |
| **path_start** | `["data","body"]` | Стартовая цепочка ключей для этого файла (переопределяет глобальный path_start). |
| **path_starts** | `[["data","body"],["data","body","absences"]]` | Несколько стартов: первый — источник строк, остальные — доп. данные мержатся в каждую строку с префиксом (например колонки `absences - isLong`, `absences - info`). |
| **exclude_keys** | `["photoData","info"]` | Исключаемые ключи для этого файла (переопределяет глобальный exclude_keys). |
| **exclude_keys_in_path** | `[{"path":"full","keys":["info"]}, {"path":"agileManagers","keys":["id"]}]` | Исключать указанные ключи только внутри заданного пути. Работает и во вложенных объектах, и **внутри массивов объектов**: `path` — имя поля-массива (например `agileManagers`, `emails`), `keys` — поля элементов массива или вложенного объекта. Так можно убрать `id` только в `agileManagers`, оставив `id` в `agileTree`. |
| **column_order** | `["empName","emails","jobTitle"]` | Порядок колонок: перечисленные элементы задают позиции. Элемент может быть **именем колонки** (точное совпадение) или **префиксом** — тогда в этот блок подтягиваются все колонки, начинающиеся с `префикс + " - "` (например `emails` → все `emails - address - (1)`, `emails - address - (2)` и т.д. в порядке (1),(2),(3)). Остальные колонки — после, с сохранением порядка по ключам. |
| **key_fields** | `[{"name":"ТАБ","sources":["employeeNumber","tn"],"default":"-"}]` | Вычисляемые ключевые колонки. Для каждого элемента берётся первое непустое значение по `sources` (последовательно). Если все пусты/отсутствуют — используется `default`. Поддерживаются несколько key_fields сразу. |
| **row_builder** | см. подробный пример ниже | Правила сборки строки из нескольких веток JSON: `base_path`, `joins`, `carry_root_fields`, `key_aliases`. Нужен, когда данные одной строки разбросаны по разным массивам и должны матчиться по ID. |

**Пример `files` с `enabled` и вложенным `sheet`:**

```json
"files": [
  {
    "enabled": true,
    "file": "profiles.json",
    "sheet_name": "Профили",
    "output": ["csv", "xlsx"],
    "path_starts": [["data", "body"], ["data", "body", "absences"]],
    "exclude_keys": ["photoData", "reactions", "colorCode"],
    "exclude_keys_in_path": [{"path": "full", "keys": ["info"]}],
    "column_order": ["empName", "empFamilyName", "jobTitle"],
    "sheet": {
      "freeze_cell": "D2",
      "columns": [],
      "default_column_format": { "number_format": "text", "width_min": 10, "width_max": 100, "wrap_text": true },
      "column_format": { "gosbCode": { "number_format": "integer" } }
    }
  },
  {
    "enabled": false,
    "file": "reserve.json",
    "sheet_name": "Резерв",
    "output": ["xlsx"],
    "path_start": ["data"],
    "sheet": { "freeze_cell": "A2", "columns": [], "default_column_format": {}, "column_format": {} }
  }
]
```

**Массивы объектов:** если значение поля — массив объектов `[{key: value}, ...]`, он разворачивается в **отдельные колонки** с индексом в имени: `родитель - ключ - (1)`, `родитель - ключ - (2)` и т.д. В ячейке — одно значение (не через запятую). **Порядок колонок:** сначала по ключу (первый сегмент пути): все колонки `agileManagers`, затем `agileRoles`, затем `agileTree` и т.д.; внутри каждого такого ключа — сначала все поля с индексом (1), затем (2), (3). Это позволяет держать блоки по смыслу (например все три колонки первого agileManager, затем второго).

**Исключение ключа только в определённом пути (`exclude_keys_in_path`):** правило `{"path": "agileManagers", "keys": ["id"]}` убирает поле `id` только внутри массива `agileManagers`; в других местах (например в `agileTree` или в корне) поле `id` остаётся. Удобно, чтобы убрать служебные UUID в одних блоках и оставить в других.

---

### Вычисляемые ключевые поля (`key_fields`)

`key_fields` позволяет объявить одну или несколько «служебных» колонок, которые вычисляются одинаково для всех строк файла.

Формат элемента:

```json
{"name":"<имя_новой_колонки>","sources":["поле1","поле2","..."],"default":"<значение_по_умолчанию>"}
```

Правила:

1. Проверка `sources` идёт **строго по порядку**.
2. Берётся первое непустое значение (не `null`, не `""`, не строка из пробелов).
3. Если ни один источник не дал значение — ставится `default`.
4. Для составного набора key_fields порядок в конфиге сохраняется.
5. Если вычисляемого поля не было в исходном JSON и оно не указано в `column_order`, поле автоматически ставится в начало таблицы.

Пример:

```json
"key_fields": [
  {"name":"ТАБ","sources":["employeeNumber","tn"],"default":"-"},
  {"name":"ФИО","sources":["fullName","empFamilyName"],"default":"(нет ФИО)"}
]
```

---

### Декларативная сборка строк (`row_builder`)

`row_builder` нужен для сложных JSON, где одна итоговая строка собирается из нескольких веток и массивов, связанных общим ID.

Ключи:

- `base_path` — путь до базовых объектов строк (из них строится «скелет» строки).
- `key_aliases` — алиасы ключей для join (например `employeeId` и `empId` считаются одинаковым идентификатором).
- `carry_root_fields` — какие поля из корневой записи дублировать в каждую строку.
- `joins` — список присоединений:
  - `path` — путь до кандидатов для join;
  - `match` — список условий `left/right`;
  - `prefix` — префикс колонок joined-объекта;
  - `mode`:
    - `first_match` — взять первый совпавший объект;
    - `all` — сохранить все совпавшие объекты списком.

Пример (ваш кейс `addressbook_empInfoFull_...`):

```json
"row_builder": {
  "base_path": ["cards","*","empInfoFull","data"],
  "key_aliases": {"employeeId":["empId"],"empId":["employeeId"]},
  "carry_root_fields": [
    {"path":"input","as":"input"},
    {"path":"searchText","as":"searchText"},
    {"path":"error","as":"error"},
    {"path":"searchStats.totalPages","as":"searchTotalPages"},
    {"path":"searchStats.totalHits","as":"searchTotalHits"},
    {"path":"searchStats.uniqueEmployeeIds","as":"searchUniqueEmployeeIds"},
    {"path":"searchStats.stopReason","as":"searchStopReason"}
  ],
  "joins": [
    {
      "path": ["search","data","hits","*"],
      "match": [{"left":"empId","right":"employeeId"}],
      "prefix": "searchHit",
      "mode": "first_match"
    },
    {
      "path": ["searchPages","*","data","hits","*"],
      "match": [{"left":"empId","right":"employeeId"}],
      "prefix": "searchPageHit",
      "mode": "first_match"
    }
  ]
}
```

Что это даёт:

- базовая строка = один сотрудник из `cards[*].empInfoFull.data`;
- `search`/`searchPages` подтягиваются по ID (а не по позиции в массиве);
- `employeeId` и `empId` считаются эквивалентными ключами.

---

### Практические шаблоны для `addressbook_*`

Чтобы проще настраивать разные варианты выгрузок AddressBook, используйте такие шаблоны:

1. **Полный формат (`cards + search + searchPages`)**
   - `row_builder.base_path`: `["cards","*","empInfoFull","data"]`
   - `joins`: из `search.data.hits[*]` и `searchPages[*].data.hits[*]` по `empId -> employeeId`.

2. **`empInfoFull_only` (без cards/searchPages)**
   - `row_builder.base_path`: `["empInfoFull","data"]`
   - `carry_root_fields`: `employeeId`
   - `joins`: `[]`.

3. **`search_only`**
   - Если в `hits` есть сотрудники: `row_builder.base_path` можно ставить на `searchPages[*].data.hits[*]`.
   - Если `hits` пустые (частый кейс no results): лучше **не использовать row_builder** и разбирать корневой объект (`path_start: []`), чтобы сохранить служебные поля (`input`, `searchText`, `searchStats`, `error`) одной строкой.

---

### CSV

| Параметр | Расположение | Примеры | Влияние |
|----------|--------------|---------|---------|
| **encoding** | `csv.encoding` | `"utf-8-sig"`, `"utf-8"`, `"cp1251"` | Кодировка файла. `utf-8-sig` даёт BOM для корректного открытия в Excel. |
| **delimiter** | `csv.delimiter` | `";"`, `","`, `"\t"` | Разделитель полей. `";"` удобен для локалей с запятой в числах. |
| **lineterminator** | `csv.lineterminator` | `"\n"`, `"\r\n"` | Окончание строки (Unix / Windows). |

---

### XLSX — общие параметры

| Параметр | Примеры | Влияние |
|----------|---------|---------|
| **freeze_first_row** | `true`, `false` | Если `true` и не задан `freeze_cell`, закрепление первой строки через ячейку `A2`. |
| **freeze_cell** | `"A2"`, `"B1"`, `"B2"`, `""` | Ячейка-граница закрепления по умолчанию для всех листов. `A2` — закрепить первую строку, `B1` — первый столбец, `B2` — и то и другое. Пусто — используется логика freeze_first_row. |
| **freeze_pane_per_sheet** | `[{"sheet_index":0,"cell":"B2"}]` | Опционально: переопределение закрепления по индексу листа в итоговом XLSX. **Рекомендуется** задавать **`freeze_cell`** в **`files[].sheet`** для каждого файла; для листа без своего `freeze_cell` используется общий `freeze_cell` из секции xlsx. |
| **column_width_mode** | `"auto"`, `"minimum"`, `"maximum"` | **auto** — ширина по содержимому в пределах width_min..width_max из default_column_format; **minimum** — всегда width_min; **maximum** — всегда width_max. |
| **auto_row_height** | `true`, `false` | При `true` высота строк подбирается по содержимому (с учётом wrap_text). |
| **autofilter** | `true`, `false` | Включение автофильтра по первой строке. |

---

### XLSX — листы и форматирование

- **`files[].sheet`** (основной способ) — объект настроек листа для **этого же** элемента `files[]` (рядом с `file`, `sheet_name`, `path_start` и т.д.):
  - **freeze_cell** — опционально. Ячейка закрепления для листа (например `"D2"`, `"F2"`). Если не задано — общий `freeze_cell` из секции xlsx (или A2 при `freeze_first_row`).
  - **columns** — список колонок для листа; пусто = все колонки.
  - **default_column_format** — формат по умолчанию для всех колонок листа (см. таблицу ниже).
  - **column_format** — объект «имя колонки → настройки». Пример: `{"gosbCode":{"number_format":"integer"},"birthday":{"number_format":"date","date_format":"YYYY-MM-DD"}}`.

- **`xlsx.sheets`** (устаревший способ) — глобальный массив; используется только если у записи `files[i]` **нет** непустого объекта **`sheet`**, тогда берётся `xlsx.sheets[i]` по индексу. В новых конфигах `xlsx.sheets` может быть пустым массивом `[]`.

Имена полей в следующей таблице — это ключи внутри **`files[].sheet.default_column_format`** и **`files[].sheet.column_format`** (или устаревших аналогов в **`xlsx.sheets[]`**).

**Параметры default_column_format и column_format**

| Параметр | Относится к | Примеры | Влияние на результат |
|----------|-------------|---------|----------------------|
| **number_format** | общее | `"text"`, `"integer"`, `"date"` | **text** — ячейка как строка/общее (по умолчанию); **integer** — целое число; **date** — дата (значение преобразуется в числовой формат даты Excel; отображение задаётся **date_format**). При неудачном преобразовании (текст в integer/date) ячейка записывается как текст со стилем по умолчанию. |
| **date_format** | общее (при number_format: "date") | `"YYYY-MM-DD"`, `"DD.MM.YYYY"` | Формат отображения даты в ячейке. Преобразуется в formatCode Excel (yyyy-mm-dd, dd.mm.yyyy). Используется только при `number_format: "date"` для этой колонки. |
| **width_min** | общее | `8`, `10` | Минимальная ширина колонки (в единицах Excel). Используется при column_width_mode auto/minimum. |
| **width_max** | общее | `50`, `100` | Максимальная ширина колонки. Используется при column_width_mode auto/maximum. |
| **wrap_text** | общее | `true`, `false` | Перенос по словам в ячейке; при auto_row_height влияет на расчёт высоты строк. |
| **header_horizontal_align** | заголовок | `"left"`, `"center"`, `"right"` | Горизонтальное выравнивание текста в первой строке (заголовки). Применяется в XLSX. |
| **header_vertical_align** | заголовок | `"top"`, `"center"`, `"bottom"` | Вертикальное выравнивание заголовка. Применяется в XLSX. |
| **header_fg_color** | заголовок | `"000000"`, `"FFFFFF"`, `""` | Цвет текста заголовка (RRGGBB, без #). Пусто — по умолчанию (чёрный). Применяется. |
| **header_bg_color** | заголовок | `"C6EFCE"`, `""` | Заливка ячеек заголовка (RRGGBB). Пример C6EFCE — светло-салатовый. Применяется. |
| **header_bold** | заголовок | `true`, `false` | Жирный шрифт заголовка. Применяется. |
| **header_italic** | заголовок | `true`, `false` | Курсив заголовка. Зарезервировано (можно расширить). |
| **header_underline** | заголовок | `true`, `false` | Подчёркивание заголовка. Зарезервировано. |
| **data_horizontal_align** | данные | `"left"`, `"center"`, `"right"` | Горизонтальное выравнивание в ячейках данных. Применяется в XLSX. |
| **data_vertical_align** | данные | `"top"`, `"center"`, `"bottom"` | Вертикальное выравнивание в ячейках данных. Применяется в XLSX. |
| **data_fg_color** | данные | `"000000"`, `""` | Цвет текста в ячейках данных (RRGGBB). Применяется при задании. |
| **data_bg_color** | данные | `""`, `"E0E0E0"` | Заливка ячеек данных. Пусто — без заливки. Применяется при задании. |
| **data_bold** | данные | `true`, `false` | Жирный текст в данных. Применяется. |
| **data_italic** | данные | `true`, `false` | Курсив в данных. Применяется. |
| **data_underline** | данные | `true`, `false` | Подчёркивание в данных. Зарезервировано. |
| **float_decimals** | общее | `2` | Число знаков после запятой (зарезервировано). |
| **decimal_separator** | общее | `","` | Разделитель дробной части (зарезервировано). |
| **thousands_separator** | общее | `" "` | Разделитель тысяч (зарезервировано). |
| **date_format** | общее | `"DD.MM.YYYY"`, `"YYYY-MM-DD"` | При `number_format: "date"` в column_format — формат отображения даты в XLSX. |

**Пример фрагмента вложенного `files[].sheet` с форматированием** (имя листа задаётся в **`sheet_name`** у записи файла):

```json
"sheet": {
  "freeze_cell": "D2",
  "columns": [],
  "default_column_format": {
    "number_format": "text",
    "width_min": 10,
    "width_max": 100,
    "wrap_text": true,
    "header_horizontal_align": "left",
    "header_vertical_align": "center",
    "header_fg_color": "000000",
    "header_bg_color": "C6EFCE",
    "header_bold": true,
    "data_horizontal_align": "left",
    "data_vertical_align": "center",
    "data_fg_color": "000000",
    "data_bg_color": "",
    "data_bold": false,
    "data_italic": false
  },
  "column_format": {
    "gosbCode": { "number_format": "integer" },
    "groupingCode": { "number_format": "integer" },
    "birthday": { "number_format": "date", "date_format": "YYYY-MM-DD" }
  }
}
```

В результате: при `sheet_name`: `"Профили"` лист в XLSX называется «Профили»; заголовки и данные оформляются по `default_column_format`; колонки `gosbCode` и `groupingCode` — как целые числа; `birthday` — как дата; ширина — по режиму `column_width_mode` в пределах из конфига.

**Закрепление по листам:** в **`files[].sheet`** задайте `"freeze_cell": "D2"` (или другую ячейку); при отсутствии — общий `freeze_cell` из секции xlsx.

**Обработка исключений при форматировании колонок:** если для ячейки задан формат колонки (`number_format: "integer"` или `"date"`), но значение не удаётся преобразовать в этот тип (текст в числовой колонке, неверный формат даты и т.п.), ячейка записывается как значение по умолчанию (текст) и со стилем по умолчанию для данных, без применения формата колонки. «Неподходящие» значения отображаются как есть, без искажения.

**Пустые листы (0 строк / 0 колонок):** поддерживаются корректно. Генерация XLSX не зависает, даже если отдельные источники дают «no results» и в листе нет данных.

---

## Запуск и тесты

Из корня проекта:

```bash
python main.py
```

Конфиг по умолчанию: `./config.json`. Если задан массив **`files`**, в работу попадают только записи с **`enabled`: true** и существующим файлом `input_dir` / `files[].file`.

Тесты развёртывания JSON:

```bash
python src/Tests/test_flattener.py
```

---

## История версий

- **0.5.1**
  - Добавлена поддержка корневых параметров конфига: `output_file_base_name` и `output_timestamp_format` для формирования имён выходных XLSX/CSV.
  - Исправлена обработка пустых листов в `xlsx_exporter` (случаи 0 строк/0 колонок): устранено зависание при сборке XLSX.
  - Документация дополнена практическими шаблонами настройки `addressbook_*` для разных форматов JSON (`full`, `empInfoFull_only`, `search_only`).

- **0.5.0**
  - Добавлена поддержка **`key_fields`**: вычисляемые ключевые колонки с последовательной цепочкой fallback-источников и `default`.
  - Добавлена поддержка **`row_builder`**: декларативная сборка строки из нескольких веток JSON с `join` по ключам, `carry_root_fields`, алиасами ключей (`key_aliases`) и wildcard-путями.
  - Для кейсов `employeeId`/`empId` реализован универсальный join без привязки к позиции элементов в массивах.
  - Обновлены тесты `src/Tests/test_flattener.py`: проверки `key_fields` и `row_builder`.

- **0.4.4**
  - Из **`config.json`** удалены блок **`__comments`** и справочный объект **`column_formats`** (description/options); описание всех параметров перенесено и структурировано в **README** (корневые ключи, таблицы CSV/XLSX).
  - **`load_config`** больше не возвращает ключ **`column_formats`**.

- **0.4.3**
  - **Конфиг по файлам:** в каждой записи `files[]` вложенный объект **`sheet`** (freeze_cell, columns, default_column_format, column_format) — настройки листа рядом с `file` и `sheet_name`; глобальный **`xlsx.sheets`** опционален (fallback по индексу для старых конфигов).
  - **`enabled` / `disabled`:** обрабатываются только записи с `enabled: true` (по умолчанию включено); `enabled: false` или `disabled: true` оставляет настройки в конфиге без обработки.
  - **config_loader:** `get_enabled_files_with_indices()`, обновлены `get_files_list`, `get_sheet_options`. **main.py:** итерация по включённым файлам с сохранением индекса в полном массиве `files`.
  - В корневой `config.json` перенесены профили из `ToDo/config.json` как дополнительные записи с **`enabled`: false** (шаблоны без запуска до включения).

- **0.4.2**
  - **Формат даты в XLSX:** в column_format и default_column_format добавлены `number_format: "date"` и `date_format` (YYYY-MM-DD, DD.MM.YYYY и т.д.). Значение ячейки преобразуется в числовой формат даты Excel; поддерживаются строки ISO, dd.mm.yyyy, dd/mm/yyyy и объекты datetime. При неудачном разборе — запись как текст (аналогично integer). В xlsx_exporter: _to_date_value, _date_format_to_excel, пользовательский numFmt и стиль даты в styles.xml.
  - **column_order с префиксом:** элемент column_order может быть префиксом (например `emails`): тогда в эту позицию подтягиваются все колонки, начинающиеся с `префикс + " - "` (emails - address - (1), (2) и т.д.), в порядке по ключам и (1),(2),(3). Позволяет упорядочивать блоки полей с несколькими значениями без перечисления каждой колонки.
  - **Закрепление по листам:** в каждом элементе **xlsx.sheets[]** (тогдашний способ) можно задать **freeze_cell**; с версии 0.4.3 рекомендуется **files[].sheet.freeze_cell**. get_sheet_options возвращает freeze_cell; main.py формирует freeze_cell_per_sheet.
  - Тест test_column_order_with_prefix.

- **0.4.1**
  - Разворот массивов объектов в отдельные колонки с индексом: имя колонки `родитель - ключ - (1)`, `(2)`, …; в ячейке — одно значение (не список через запятую).
  - Порядок колонок: по ключам (первый сегмент пути), внутри ключа — сначала все поля (1), затем (2), (3). Не смешиваются все (1) из разных массивов в кучу.
  - `exclude_keys_in_path` применяется и внутри массивов объектов: можно исключить, например, `id` только в `agileManagers`, оставив `id` в `agileTree`.
  - Тесты: test_array_of_objects_flatten (порядок (1) перед (2)), test_array_columns_grouped_by_key (два массива — блоки по ключам), test_exclude_keys_in_path_array.

- **0.4.0**
  - Настройки по файлам: опциональный массив `files` (file, sheet_name, output, path_start, path_starts, exclude_keys, exclude_keys_in_path, column_order). Если задан — список файлов и опции берутся из него.
  - Выбор выхода: для каждого файла можно указать `output`: только CSV, только XLSX или оба.
  - Имя листа в XLSX задаётся в конфиге (`sheet_name`) или по имени файла.
  - Несколько стартовых путей (`path_starts`): первый — источник строк, остальные — доп. данные мержатся в строку с префиксом.
  - Исключение ключей только внутри пути (`exclude_keys_in_path`): например убрать поле `info` только внутри раздела `full` (и внутри массивов объектов с таким путём).
  - Порядок колонок (`column_order`): указанные поля идут первыми на листе.
  - Разбор массивов объектов: колонки с индексом (1), (2), … по одному значению на колонку.
  - config_loader: get_files_list(), get_file_options(), get_sheet_options. worker: file_index, запись CSV только при "csv" в output. main: сбор листов только при "xlsx" в output. (С 0.4.3: get_enabled_files_with_indices, files[].sheet, enabled.)

- **0.3.1**
  - Обработка исключений при форматировании колонок: если значение не преобразуется в указанный тип (например integer или в будущем date), ячейка записывается как значение по умолчанию (текст) и со стилем по умолчанию, без применения формата колонки. Документация обновлена: правило fallback и описание _to_integer_value.

- **0.3.0**
  - Реализовано применение в XLSX: выравнивание заголовка и данных (header/data_horizontal_align, header/data_vertical_align), стиль данных (data_fg_color, data_bg_color, data_bold, data_italic), wrap_text в стилях. Стили собираются в styles.xml с элементами alignment; к ячейкам данных применяется отдельный стиль при задании любых параметров данных.
  - Документация: подробное описание работы программы, всех функций и конфига с примерами и вариантами значений.

- **0.2.0**
  - Закрепление областей по ячейке: freeze_cell и freeze_pane_per_sheet. Формат колонок: default_column_format и column_format (в т.ч. number_format: integer). Режимы ширины колонок (column_width_mode), авто-высота строк (auto_row_height). (Позже описание параметров вынесено в README, см. 0.4.4.)

- **0.1.0**
  - Универсальный flatten JSON, path_start, exclude_keys, include_only_keys. Экспорт в CSV и один XLSX с листом на файл. Закрепление первой строки, автофильтр, формат «целое число». Логирование, параллельная обработка, только stdlib.
