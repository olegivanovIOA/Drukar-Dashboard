"""
generate.py — читает Google Sheets → генерирует index.html
Поддерживает данные за любой месяц с Ноябрь 2025 по Декабрь 2026.
Таблицы должны быть публичными (Поділитися → Всі з посиланням → Переглядач).
"""
import os, requests, io, csv, json
from datetime import datetime

SHEET_ID      = os.environ.get("SHEET_ID",      "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")
SHEET_ID2     = os.environ.get("SHEET_ID2",     "1NJkxtyha_oSpeaB7Jzmf440-kOF2gHBB0xsaMfKPRsI")
LINES_SHEET_ID  = os.environ.get("LINES_SHEET_ID",  "1SewXdbiFVIUPCESo5XDrRzvvG5rut4vuQTDyBDg3qp4")
LINES_SHEET_ID2 = os.environ.get("LINES_SHEET_ID2", "1NJkxtyha_oSpeaB7Jzmf440-kOF2gHBB0xsaMfKPRsI")
CALC_SHEET_ID = os.environ.get("CALC_SHEET_ID", "1U8dZJ_2niv5eYp0VGHvUHThQP6Ts4WaxeR10SEKIBvM")
STRATEGY_SHEET_ID = os.environ.get("STRATEGY_SHEET_ID", "1ASrf0kKP_0uIBdLCB__hoYp6GPjW5bNyzauMIDcbSWk")
STRATEGY_FILE = "Друкар_стратегия_2026.xlsx"
RETAIL_SHEET_ID   = os.environ.get("RETAIL_SHEET_ID",   "1W4mHhbIy43xxTuTQA2nl1atYag8ynlkHpbY_Fx6JUdk")
# Журнал Відвантажень — об'єднаний реєстр ТОВ(Стрім)+ФОП(Роздріб)+Easy(Изи), лист "Відвантаження".
# З 07.2026 вкладка "Продажі" рахується САМЕ з нього (замість _AllData_$), бо _AllData_$
# відстає від бухгалтера (той самий баг, що давав 0 доходу за минулі місяці в ПнЛ).
SHIP_SHEET_ID     = os.environ.get("SHIP_SHEET_ID",     "1oy23YacYq6O7MajCcBf_HIWnRyFSyRgdrI5_uPE9ZVk")

# ── Middle dashboard config ─────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "tabs": {
        "overview":   {"top": True, "mid": False},
        "production": {"top": True, "mid": True},
        "ito":        {"top": True, "mid": True},
        "quality":    {"top": True, "mid": True},
        "costs":      {"top": True, "mid": False},
        "operators":  {"top": True, "mid": True},
        "calculator": {"top": True, "mid": False},
        "lines":      {"top": True, "mid": True},
        "sales":      {"top": True, "mid": False},
        "forecast":   {"top": True, "mid": False},
        "okr":        {"top": True, "mid": True},
        "inventory":  {"top": True, "mid": True},
        "product":    {"top": True, "mid": False},
    }
}

def load_config():
    try:
        with open("middle_config.json", "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        print("OK middle_config.json loaded")
        return cfg
    except Exception as e:
        print(f"WARN middle_config.json not found ({e}), using defaults")
        import copy
        return copy.deepcopy(DEFAULT_CONFIG)

# 14 месяцев: Ноябрь 2025 — Декабрь 2026
# Позиция в массиве (0-based): Nov25=0, Dec25=1, Jan26=2 ... Dec26=13
MONTH_COUNT = 14
MONTH_ORDER = [
    '2025-11','2025-12',
    '2026-01','2026-02','2026-03','2026-04','2026-05','2026-06',
    '2026-07','2026-08','2026-09','2026-10','2026-11','2026-12'
]

def fetch_csv(sheet_id, sheet_name, retries=3, timeout=60):
    url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
           f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}")
    print(f"Fetching: {sheet_name}")
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return list(csv.reader(io.StringIO(r.text)))
        except Exception as e:
            last_err = e
            print(f"  Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                import time; time.sleep(5 * attempt)
    raise last_err

def fetch_xlsx(sheet_id, dest_path, retries=3, timeout=90):
    """Скачивает Google Sheets как .xlsx файл."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    print(f"Fetching xlsx: {sheet_id}")
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                f.write(r.content)
            print(f"  Saved {len(r.content):,} bytes → {dest_path}")
            return
        except Exception as e:
            last_err = e
            print(f"  Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                import time; time.sleep(5 * attempt)
    raise last_err

def f(v, default=None):
    try:
        s = str(v).strip().replace(',','.').replace(' ','').replace('\xa0','').replace('%','')
        return float(s) if s else default
    except:
        return default

def get_row(rows, keyword):
    kw = keyword.lower()
    for row in rows:
        if any(kw in str(c).lower() for c in row):
            return row
    return None

def pct(v):
    if v is None: return None
    return round(v*100, 2) if abs(v) <= 1.5 else round(v, 2)

def detect_month_columns(rows):
    """
    Определяет маппинг: MONTH_ORDER[i] → col_index в таблице.
    Ищет строку с заголовками месяцев (ноябрь/листопад, декабрь и т.д.)
    Возвращает dict {month_key: col_index} и col_start (первый столбец с данными).
    """
    # Порядок важливий: більш специфічні ключі — першими,
    # щоб 'ноябрь 2026' не матчився як 'ноябрь' (2025-11)
    month_keywords = [
        ('ноябрь 2026',   '2026-11'), ('листопад 2026', '2026-11'),
        ('декабрь 2026',  '2026-12'), ('грудень 2026',  '2026-12'),
        ('ноябрь',        '2025-11'), ('листопад',      '2025-11'),
        ('декабрь',       '2025-12'), ('грудень',       '2025-12'),
        ('январь',        '2026-01'), ('січень',        '2026-01'),
        ('февраль',       '2026-02'), ('лютий',         '2026-02'),
        ('март',          '2026-03'), ('березень',      '2026-03'),
        ('апрель',        '2026-04'), ('квітень',       '2026-04'),
        ('май',           '2026-05'), ('травень',       '2026-05'),
        ('июнь',          '2026-06'), ('червень',       '2026-06'),
        ('июль',          '2026-07'), ('липень',        '2026-07'),
        ('август',        '2026-08'), ('серпень',       '2026-08'),
        ('сентябрь',      '2026-09'), ('вересень',      '2026-09'),
        ('октябрь',       '2026-10'), ('жовтень',       '2026-10'),
    ]
    mapping = {}
    for row in rows[:10]:  # шукаємо в перших 10 рядках (на випадок merge cells)
        for ci, cell in enumerate(row):
            cell_l = str(cell).lower().strip()
            for kw, month_key in month_keywords:
                if kw in cell_l and month_key not in mapping:
                    mapping[month_key] = ci
                    break  # знайшли для цієї клітинки — далі не перевіряємо
    col_start = min(mapping.values()) if mapping else 2
    print(f"  Month columns detected: {mapping}")
    return mapping, col_start

def extract_row_by_month(row, col_map):
    """Извлекает значения по месяцам в порядке MONTH_ORDER (14 элементов)."""
    result = [None] * MONTH_COUNT
    for i, month_key in enumerate(MONTH_ORDER):
        ci = col_map.get(month_key)
        if ci is not None and ci < len(row):
            result[i] = f(row[ci])
    return result


def parse_production_from_alldata(rows):
    """
    Читає _AllData_Product (агрегований лист з обох локацій).
    Структура: A=Дата, B=Смена, C=Оператор, D=Вклад%, E=Лінія,
               F=Вид, G=Кол-во шт(вклад), H=Вес кг(вклад),
               I=НФ Вес кг(вклад), J=Відхід кг(вклад), K=Запаковано шт,
               L=Автоподсчет вес кг, ..., Q=Упаковщик, R=Старший зміни, S=Локація
    
    Оскільки H вже помножено на Вклад%, реальний вес = H/D.
    Агрегуємо по місяцях і виду (PETG/PLA).
    """
    from collections import defaultdict
    import re as _re
    from datetime import datetime as _dt, date as _date, timedelta

    UA_SHORT = {
        '01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
        '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'
    }

    if not rows or len(rows) < 2:
        print("  WARNING: _AllData_Product — no data")
        return _empty_production()

    # Заголовок рядок 1 (індекс 0)
    # Дані з рядка 2 (індекс 1)

    # Агрегація: унікальні (дата, зміна, лінія) → беремо H/D = реальний вес лінії
    # key = (ym, вид) → {petg_kg, pla_kg, petg_nf, pla_nf, petg_waste, pla_waste}
    monthly = defaultdict(lambda: {
        'petg': 0.0, 'pla': 0.0,
        'petg_packed': 0.0, 'pla_packed': 0.0,
        'petg_nf': 0.0, 'pla_nf': 0.0,
        'petg_waste': 0.0, 'pla_waste': 0.0,
    })

    # Щоб не дублювати для H/D: зберігаємо унікальні (дата, зміна, лінія) вже оброблені
    seen_lines = set()

    for row in rows[1:]:
        if not row or len(row) < 8: continue

        # Парсимо дату
        date_val = row[0]
        ym = None
        date_str = None  # повна дата для ключа унікальності
        if hasattr(date_val, 'strftime'):
            ym = date_val.strftime('%Y-%m')
            date_str = date_val.strftime('%Y-%m-%d')
        elif isinstance(date_val, (int, float)) and 40000 < date_val < 60000:
            from datetime import date as _date, timedelta as _td
            dt = _date(1899, 12, 30) + _td(days=int(date_val))
            ym = dt.strftime('%Y-%m')
            date_str = dt.strftime('%Y-%m-%d')
        elif isinstance(date_val, str) and len(date_val) >= 7:
            try:
                ym = _dt.strptime(date_val[:10], '%Y-%m-%d').strftime('%Y-%m')
                date_str = date_val[:10]
            except:
                try:
                    ym = _dt.strptime(date_val[:10], '%d.%m.%Y').strftime('%Y-%m')
                    date_str = _dt.strptime(date_val[:10], '%d.%m.%Y').strftime('%Y-%m-%d')
                except: pass
        if not ym or ym < '2025-11': continue

        shift   = str(row[1]).strip() if row[1] else ''
        line    = str(row[4]).strip().upper() if len(row) > 4 and row[4] else ''  # E=Лінія
        vid     = str(row[5]).strip().upper() if len(row) > 5 and row[5] else ''  # F=Вид

        # Ключ унікальності: повна дата + зміна + лінія (не місяць!)
        line_key = (date_str, shift, line)
        if line_key in seen_lines: continue
        seen_lines.add(line_key)

        # Вклад % (колонка D, індекс 3)
        try:
            contrib = float(str(row[3]).replace('%','').replace(',','.').strip())
            if contrib > 1.5: contrib /= 100.0
            if contrib <= 0: contrib = 1.0
        except:
            contrib = 1.0

        # Вага кг (вклад) → реальний вес = H/D
        def safe_f(val):
            try:
                return float(str(val).replace(',','.').replace(' ','').strip()) if val else 0.0
            except: return 0.0

        weight = safe_f(row[7]) / contrib if contrib > 0 else 0.0  # H=Вес кг (вклад)
        nf     = safe_f(row[8]) / contrib if len(row) > 8 else 0.0  # I=НФ кг (вклад)
        waste  = safe_f(row[9]) / contrib if len(row) > 9 else 0.0  # J=Відхід кг (вклад)
        packed = safe_f(row[11]) / contrib if len(row) > 11 and contrib > 0 else 0.0  # L=Автопідрахунок вес кг (вклад)

        is_petg = 'PETG' in vid
        is_pla  = 'PLA' in vid and 'PETG' not in vid

        if is_petg:
            monthly[ym]['petg']        += weight
            monthly[ym]['petg_packed'] += packed
            monthly[ym]['petg_nf']     += nf
            monthly[ym]['petg_waste']  += waste
        elif is_pla:
            monthly[ym]['pla']         += weight
            monthly[ym]['pla_packed']  += packed
            monthly[ym]['pla_nf']      += nf
            monthly[ym]['pla_waste']   += waste

    # Формуємо масиви по MONTH_ORDER
    petg_prod   = [round(monthly[m]['petg'],1)        if m in monthly else None for m in MONTH_ORDER]
    pla_prod    = [round(monthly[m]['pla'],1)         if m in monthly else None for m in MONTH_ORDER]
    petg_packed = [round(monthly[m]['petg_packed'],1) if m in monthly else None for m in MONTH_ORDER]
    pla_packed  = [round(monthly[m]['pla_packed'],1)  if m in monthly else None for m in MONTH_ORDER]
    petg_nf_kg  = [round(monthly[m]['petg_nf'],1)     if m in monthly else None for m in MONTH_ORDER]
    pla_nf_kg   = [round(monthly[m]['pla_nf'],1)      if m in monthly else None for m in MONTH_ORDER]
    petg_w_kg   = [round(monthly[m]['petg_waste'],1)  if m in monthly else None for m in MONTH_ORDER]
    pla_w_kg    = [round(monthly[m]['pla_waste'],1)   if m in monthly else None for m in MONTH_ORDER]

    # НФ % і Брак %
    nf_pct = []; waste_pct = []
    for i in range(MONTH_COUNT):
        pp = (petg_prod[i] or 0) + (pla_prod[i] or 0)
        nf_pct.append(round(((petg_nf_kg[i] or 0)+(pla_nf_kg[i] or 0))/pp*100,2) if pp else None)
        waste_pct.append(round(((petg_w_kg[i] or 0)+(pla_w_kg[i] or 0))/pp*100,2) if pp else None)

    petg_nf_r = [round((petg_nf_kg[i] or 0)/(petg_prod[i] or 1)*100,2) if petg_prod[i] else None for i in range(MONTH_COUNT)]
    pla_nf_r  = [round((pla_nf_kg[i]  or 0)/(pla_prod[i]  or 1)*100,2) if pla_prod[i]  else None for i in range(MONTH_COUNT)]
    petg_w_r  = [round((petg_w_kg[i]  or 0)/(petg_prod[i] or 1)*100,2) if petg_prod[i] else None for i in range(MONTH_COUNT)]
    pla_w_r   = [round((pla_w_kg[i]   or 0)/(pla_prod[i]  or 1)*100,2) if pla_prod[i]  else None for i in range(MONTH_COUNT)]

    total_prod = [
        round((petg_prod[i] or 0)+(pla_prod[i] or 0), 1)
        if petg_prod[i] is not None or pla_prod[i] is not None else None
        for i in range(MONTH_COUNT)
    ]

    data = {
        "updated":      datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC'),
        "petg_prod":    petg_prod,
        "pla_prod":     pla_prod,
        "petg_packed":  petg_packed,
        "pla_packed":   pla_packed,
        "total_prod":   total_prod,
        "nf_pct":       nf_pct, "waste_pct": waste_pct,
        "petg_nf":      petg_nf_r,
        "pla_nf":       pla_nf_r,
        "petg_nf_kg":   petg_nf_kg,
        "pla_nf_kg":    pla_nf_kg,
        "petg_waste":   petg_w_r,
        "pla_waste":    pla_w_r,
        # Фінансові дані — залишаємо порожніми (беруться з _Drukar_Product)
        "income":       [None]*MONTH_COUNT,
        "expenses":     [None]*MONTH_COUNT,
        "profit":       [None]*MONTH_COUNT,
        "cost_petg_kg": [None]*MONTH_COUNT,
        "cost_pla_kg":  [None]*MONTH_COUNT,
    }

    print(f"\n  PETG prod (from _AllData_Product): {data['petg_prod']}")
    print(f"  PLA prod:  {data['pla_prod']}")
    return data


def parse_production_from_journals(rows_list):
    """
    Читає основні Журнали локацій (Журнал.Локація1, Журнал.Локація2, ...) напряму.
    Використовується для розрахунку ВИРОБНИЦТВО / НФ / БРАК для ПнЛ і собівартості.
    НЕ замінює _AllData_Product — вклади/мотивація операторів залишаються як є.

    Структура журналу (рядки з індексу 2, тобто пропускаємо 2 рядки заголовків):
      col 0  = Дата (datetime, ffill по блоках)
      col 6  = Зміна (День/Ніч)
      col 7  = Лінія
      col 8  = Вид (PETG 2.5 / PETG 3.0 / PLA 3.0 / ...)
      col 9  = Кількість шт (вироблено)
      col 10 = Вага кг (вироблено)
      col 11 = НФ кг
      col 12 = Відхід (Брак) кг

    rows_list: список списків рядків — по одному на кожен журнал локації.
    """
    from collections import defaultdict
    from datetime import datetime as _dt, date as _date, timedelta

    def _safe(val):
        try:
            v = str(val).replace(',', '.').replace(' ', '').replace('\xa0', '').strip()
            return float(v) if v else 0.0
        except:
            return 0.0

    def _parse_date(val):
        if hasattr(val, 'strftime'):
            return val.strftime('%Y-%m')
        if isinstance(val, (int, float)) and 40000 < val < 60000:
            return (_date(1899, 12, 30) + timedelta(days=int(val))).strftime('%Y-%m')
        if isinstance(val, str):
            for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
                try:
                    return _dt.strptime(str(val)[:10], fmt).strftime('%Y-%m')
                except:
                    pass
        return None

    # Маппінг назв Видів з журналу → назва SKU для дашборду (має збігатися з продажами)
    SKU_DISPLAY_MAP = {
        'PETG 2.5':     'PETG 2.5кг',
        'PETG 3.0':     'PETG 3кг',
        'PETG 3.0 RED': 'PETG 3кг RED',
        'PETG 1.0':     'PETG 1кг',
        'PETG 1.0 ':    'PETG 1кг',
        'PLA 2.5':      'PLA 2.5кг',
        'PLA 3.0':      'PLA 3кг',
    }

    monthly = defaultdict(lambda: {
        'petg': 0.0, 'pla': 0.0,
        'petg_nf': 0.0, 'pla_nf': 0.0,
        'petg_waste': 0.0, 'pla_waste': 0.0,
        'petg_pcs': 0.0, 'pla_pcs': 0.0,
    })
    # per-SKU: sku_display → ym → kg
    from collections import defaultdict as _dd
    sku_monthly = _dd(lambda: _dd(float))

    total_rows = 0
    for rows in rows_list:
        if not rows or len(rows) < 3:
            continue
        # Рядки даних починаються з індексу 2 (пропускаємо 2 рядки заголовків)
        last_ym = None
        for row in rows[2:]:
            if not row or len(row) < 9:
                continue

            # col 0 = дата (може бути NaT/порожньо — тоді ffill)
            raw_date = row[0]
            ym = _parse_date(raw_date)
            if ym:
                last_ym = ym
            else:
                ym = last_ym
            if not ym or ym < '2025-11':
                continue

            # col 8 = Вид продукту
            vid = str(row[8]).strip() if len(row) > 8 else ''
            if not vid or vid in ('nan', 'None', ''):
                continue
            is_petg = 'PETG' in vid.upper()
            is_pla  = 'PLA'  in vid.upper() and not is_petg
            if not is_petg and not is_pla:
                continue

            pcs   = _safe(row[9])  if len(row) > 9  else 0.0  # col 9  Кількість шт
            kg    = _safe(row[10]) if len(row) > 10 else 0.0  # col 10 Вага кг
            nf    = _safe(row[11]) if len(row) > 11 else 0.0  # col 11 НФ кг
            waste = _safe(row[12]) if len(row) > 12 else 0.0  # col 12 Відхід кг

            if is_petg:
                monthly[ym]['petg']       += kg
                monthly[ym]['petg_pcs']   += pcs
                monthly[ym]['petg_nf']    += nf
                monthly[ym]['petg_waste'] += waste
            else:
                monthly[ym]['pla']        += kg
                monthly[ym]['pla_pcs']    += pcs
                monthly[ym]['pla_nf']     += nf
                monthly[ym]['pla_waste']  += waste
            # per-SKU accumulation
            sku_disp = SKU_DISPLAY_MAP.get(vid, vid.strip())
            sku_monthly[sku_disp][ym] += kg
            total_rows += 1

    print(f"  parse_production_from_journals: {total_rows} data rows, {len(monthly)} months")

    petg_prod  = [round(monthly[m]['petg'],  1) if m in monthly else None for m in MONTH_ORDER]
    pla_prod   = [round(monthly[m]['pla'],   1) if m in monthly else None for m in MONTH_ORDER]
    petg_nf_kg = [round(monthly[m]['petg_nf'],   1) if m in monthly else None for m in MONTH_ORDER]
    pla_nf_kg  = [round(monthly[m]['pla_nf'],    1) if m in monthly else None for m in MONTH_ORDER]
    petg_w_kg  = [round(monthly[m]['petg_waste'], 1) if m in monthly else None for m in MONTH_ORDER]
    pla_w_kg   = [round(monthly[m]['pla_waste'],  1) if m in monthly else None for m in MONTH_ORDER]
    petg_pcs   = [round(monthly[m]['petg_pcs']) if m in monthly else None for m in MONTH_ORDER]
    pla_pcs    = [round(monthly[m]['pla_pcs'])  if m in monthly else None for m in MONTH_ORDER]

    total_prod = [
        round((petg_prod[i] or 0) + (pla_prod[i] or 0), 1)
        if petg_prod[i] is not None or pla_prod[i] is not None else None
        for i in range(MONTH_COUNT)
    ]

    nf_pct = []; waste_pct = []
    for i in range(MONTH_COUNT):
        pp = (petg_prod[i] or 0) + (pla_prod[i] or 0)
        nf_pct.append(   round(((petg_nf_kg[i] or 0) + (pla_nf_kg[i] or 0)) / pp * 100, 2) if pp else None)
        waste_pct.append(round(((petg_w_kg[i]  or 0) + (pla_w_kg[i]  or 0)) / pp * 100, 2) if pp else None)

    petg_nf_r = [round((petg_nf_kg[i] or 0) / (petg_prod[i] or 1) * 100, 2) if petg_prod[i] else None for i in range(MONTH_COUNT)]
    pla_nf_r  = [round((pla_nf_kg[i]  or 0) / (pla_prod[i]  or 1) * 100, 2) if pla_prod[i]  else None for i in range(MONTH_COUNT)]
    petg_w_r  = [round((petg_w_kg[i]  or 0) / (petg_prod[i] or 1) * 100, 2) if petg_prod[i] else None for i in range(MONTH_COUNT)]
    pla_w_r   = [round((pla_w_kg[i]   or 0) / (pla_prod[i]  or 1) * 100, 2) if pla_prod[i]  else None for i in range(MONTH_COUNT)]

    # Будуємо PROD_BY_SKU: {sku_display: [14 значень по MONTH_ORDER]}
    all_skus_sorted = sorted(sku_monthly.keys(), key=lambda s: (
        0 if s.startswith('PETG 2') else
        1 if s.startswith('PETG 3к') and 'RED' not in s else
        2 if 'RED' in s else
        3 if s.startswith('PETG 1') else
        4 if s.startswith('PLA 2') else
        5
    ))
    prod_by_sku = {
        sku: [round(sku_monthly[sku].get(m, 0.0), 1) if sku_monthly[sku].get(m, 0) > 0 else None
              for m in MONTH_ORDER]
        for sku in all_skus_sorted
    }

    result = {
        # кг вироблено (для дашборду і ПнЛ)
        'petg_prod':    petg_prod,
        'pla_prod':     pla_prod,
        'total_prod':   total_prod,
        # штуки вироблено (для якісної таблиці)
        'petg_pcs':     petg_pcs,
        'pla_pcs':      pla_pcs,
        # НФ
        'petg_nf_kg':   petg_nf_kg,
        'pla_nf_kg':    pla_nf_kg,
        'petg_nf':      petg_nf_r,
        'pla_nf':       pla_nf_r,
        'nf_pct':       nf_pct,
        # Брак (Відхід)
        'petg_waste_kg': petg_w_kg,
        'pla_waste_kg':  pla_w_kg,
        'petg_waste':   petg_w_r,
        'pla_waste':    pla_w_r,
        'waste_pct':    waste_pct,
        # per-SKU для таба "Товар"
        'prod_by_sku':    prod_by_sku,
        'prod_sku_list':  all_skus_sorted,
    }

    print(f"  PETG prod (journals): {petg_prod}")
    print(f"  PLA  prod (journals): {pla_prod}")
    print(f"  PETG НФ kg:           {petg_nf_kg}")
    print(f"  PLA  НФ kg:           {pla_nf_kg}")
    print(f"  PETG Брак kg:         {petg_w_kg}")
    print(f"  PLA  Брак kg:         {pla_w_kg}")
    return result


def _empty_production():
    return {
        "updated": datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC'),
        "petg_prod": [None]*MONTH_COUNT, "pla_prod": [None]*MONTH_COUNT,
        "petg_packed": [None]*MONTH_COUNT, "pla_packed": [None]*MONTH_COUNT,
        "total_prod": [None]*MONTH_COUNT, "nf_pct": [None]*MONTH_COUNT,
        "waste_pct": [None]*MONTH_COUNT, "petg_nf": [None]*MONTH_COUNT,
        "pla_nf": [None]*MONTH_COUNT, "petg_nf_kg": [None]*MONTH_COUNT,
        "pla_nf_kg": [None]*MONTH_COUNT, "petg_waste": [None]*MONTH_COUNT,
        "pla_waste": [None]*MONTH_COUNT, "income": [None]*MONTH_COUNT,
        "expenses": [None]*MONTH_COUNT, "profit": [None]*MONTH_COUNT,
        "cost_petg_kg": [None]*MONTH_COUNT, "cost_pla_kg": [None]*MONTH_COUNT,
        "petg_waste_kg": [None]*MONTH_COUNT, "pla_waste_kg": [None]*MONTH_COUNT,
        "petg_pcs": [None]*MONTH_COUNT, "pla_pcs": [None]*MONTH_COUNT,
        "prod_by_sku": {}, "prod_sku_list": [],
    }

def parse_production(rows):
    col_map, _ = detect_month_columns(rows)

    def vals(kw):
        row = get_row(rows, kw)
        return extract_row_by_month(row, col_map) if row else [None]*MONTH_COUNT

    petg_prod  = vals('PETG, Продукции, кг')
    pla_prod   = vals('PLA, Продукции, кг')
    petg_nf_kg = vals('PETG, НФ, кг')
    pla_nf_kg  = vals('PLA, НФ, кг')
    petg_w_kg  = vals('PETG, Брак, кг')
    pla_w_kg   = vals('PLA, Брак, кг')
    expenses   = vals('Разом (всі витрати)')
    income     = vals('ДОХОД, грн')
    profit     = vals('Операційний')
    petg_nf_r  = vals('PETG, НФ, %')
    pla_nf_r   = vals('PLA, НФ, %')
    petg_w_r   = vals('PETG, Брак, %')
    pla_w_r    = vals('PLA, Брак, %')

    # Себестоимость/кг — строки после заголовка
    cpkg_petg = [None]*MONTH_COUNT
    cpkg_pla  = [None]*MONTH_COUNT
    found_sebest = False
    for i, row in enumerate(rows):
        row_str = ' '.join(str(c) for c in row).lower()
        if 'себестоимость 1 кг' in row_str or 'себестоимість 1 кг' in row_str:
            found_sebest = True
            print(f"  Себестоимость header found at row {i}: {row[:6]}")
            if i+1 < len(rows):
                cpkg_petg = extract_row_by_month(rows[i+1], col_map)
                print(f"  PETG raw row[{i+1}]: {rows[i+1][:20]}")
            if i+2 < len(rows):
                cpkg_pla  = extract_row_by_month(rows[i+2], col_map)
                print(f"  PLA  raw row[{i+2}]: {rows[i+2][:20]}")
            break
    if not found_sebest:
        print(f"  WARNING: 'Себестоимость 1 кг' row NOT FOUND in {len(rows)} rows!")
        print(f"  First 30 rows col A: {[str(r[0])[:30] if r else '' for r in rows[:30]]}")

    # Суммарный НФ% и Брак%
    nf_pct=[]; waste_pct=[]
    for i in range(MONTH_COUNT):
        pp = (petg_prod[i] or 0) + (pla_prod[i] or 0)
        nf_pct.append(round(((petg_nf_kg[i] or 0)+(pla_nf_kg[i] or 0))/pp*100,2) if pp else None)
        waste_pct.append(round(((petg_w_kg[i] or 0)+(pla_w_kg[i] or 0))/pp*100,2) if pp else None)

    total_prod = [
        round((petg_prod[i] or 0)+(pla_prod[i] or 0), 1)
        if petg_prod[i] is not None or pla_prod[i] is not None else None
        for i in range(MONTH_COUNT)
    ]

    data = {
        "updated":      datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC'),
        "petg_prod":    [round(v,1) if v else None for v in petg_prod],
        "pla_prod":     [round(v,1) if v else None for v in pla_prod],
        "total_prod":   total_prod,
        "nf_pct":       nf_pct, "waste_pct": waste_pct,
        "petg_nf":      [pct(v) for v in petg_nf_r],
        "pla_nf":       [pct(v) for v in pla_nf_r],
        "petg_waste":   [pct(v) for v in petg_w_r],
        "pla_waste":    [pct(v) for v in pla_w_r],
        "income":       [round(v) if v else None for v in income],
        "expenses":     [round(v) if v else None for v in expenses],
        "profit":       [round(v) if v else None for v in profit],
        "cost_petg_kg": [round(v,2) if v else None for v in cpkg_petg],
        "cost_pla_kg":  [round(v,2) if v else None for v in cpkg_pla],
    }

    # Print summary
    print(f"\n  PETG prod: {data['petg_prod']}")
    print(f"  Cost PETG: {data['cost_petg_kg']}")
    print(f"  Cost PLA:  {data['cost_pla_kg']}")
    print(f"  PLA prod:  {data['pla_prod']}")
    print(f"  Income:    {data['income']}")
    return data

def parse_calculator(rows):
    result = {"petg_price": 146.4, "pla_price": 175.5, "waste_pct": 5.0}
    try:
        r1 = get_row(rows, 'цена сырья PETG')
        if r1:
            v = f(r1[1])
            if v: result["petg_price"] = v
        r2 = get_row(rows, 'цена сырья PLA')
        if r2:
            v = f(r2[1])
            if v: result["pla_price"] = v
        r3 = get_row(rows, 'Брака и НФ')
        if r3:
            v = f(r3[1])
            if v: result["waste_pct"] = round(v*100 if v<1 else v, 1)
        print(f"  Calc: PETG={result['petg_price']}, PLA={result['pla_price']}, waste={result['waste_pct']}%")
    except Exception as e:
        print(f"  Calc error: {e}")
    return result

def parse_calc_extended(rows):
    result = {"granule": 112.2}
    try:
        r = get_row(rows, 'гранул')
        if r:
            v = f(r[1])
            if v: result["granule"] = v
        print(f"  Extended calc: granule={result['granule']}")
    except Exception as e:
        print(f"  Extended calc error: {e}")
    return result



def parse_lines_heatmap_from_alldata(rows):
    """
    Агрегує кг готової продукції по лінії × місяць з _AllData_Product.
    Структура: A=Дата(0), B=Зміна(1), C=Оператор(2), D=Вклад%(3),
               E=Лінія(4), F=Вид(5), H=Вес кг(вклад)(7)
    Локація визначається по LINES_SHEET_ID/LINES_SHEET_ID2 порівнянням.
    """
    import re as _re
    from collections import defaultdict
    from datetime import date as _date, timedelta, datetime as _dt

    UA_SHORT = {
        '01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
        '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'
    }

    if not rows or len(rows) < 2:
        print("  HM: _AllData_Product empty")
        return [], {}

    monthly = defaultdict(lambda: defaultdict(float))
    seen = set()

    for row in rows[1:]:
        if not row or len(row) < 8: continue

        # Дата
        date_val = row[0]
        dt = None; date_str = None
        if hasattr(date_val, 'strftime'):
            dt = date_val; date_str = dt.strftime('%Y-%m-%d')
        elif isinstance(date_val, (int, float)) and 40000 < date_val < 60000:
            dt = _date(1899, 12, 30) + timedelta(days=int(date_val))
            date_str = dt.strftime('%Y-%m-%d')
        elif isinstance(date_val, str):
            for fmt in ('%d.%m.%Y','%Y-%m-%d'):
                try:
                    dt = _dt.strptime(str(date_val)[:10], fmt).date()
                    date_str = dt.strftime('%Y-%m-%d'); break
                except: pass
        if not dt or not date_str: continue
        ym = date_str[:7]
        if ym < '2025-11': continue

        shift = str(row[1]).strip() if row[1] else ''
        line  = str(row[4]).strip().upper() if len(row) > 4 and row[4] else ''
        if not line: continue
        m = _re.match(r'.*?(\d+)$', line)
        if m: line = f'ЛІНІЯ {m.group(1)}'

        # Дедупликація (дата, зміна, лінія)
        key = (date_str, shift, line)
        if key in seen: continue
        seen.add(key)

        try:
            contrib = float(str(row[3]).replace('%','').replace(',','.').strip())
            if contrib > 1.5: contrib /= 100.0
            if contrib <= 0: contrib = 1.0
        except: contrib = 1.0

        try: weight = float(str(row[7]).replace(',','.').strip()) / contrib
        except: continue
        if weight <= 0: continue

        monthly[line][ym] += weight

    if not monthly:
        print("  HM from _AllData_Product: no data")
        return [], {}

    all_months = sorted(
        ym for ym in set(m for d in monthly.values() for m in d)
        if ym >= '2025-11'
    )

    hm_labels = [f"{UA_SHORT[ym.split('-')[1]]} {ym.split('-')[0][2:]}" for ym in all_months]

    def line_num(ln):
        m = _re.search(r'(\d+)', ln)
        return int(m.group(1)) if m else 999

    hm_data = {}
    for line in sorted(monthly.keys(), key=line_num):
        hm_data[line] = [round(monthly[line].get(ym, 0)) for ym in all_months]

    total_kg = sum(sum(v.values()) for v in monthly.values())
    print(f"  HM from _AllData_Product: {len(hm_data)} lines x {len(all_months)} months, total={round(total_kg):,} kg")
    return hm_labels, hm_data

def parse_norms(rows):
    """
    Читає лист НОРМЫ з Журнал.Локация1.
    Структура: B=Дата(1), C=Лінія(2), D=Продукція(3), E=МАКСИМУМ(4), F=Норма готовой прод. кг(5)
    Повертає dict: {лінія: {вид: норма_кг_за_зміну}}
    Беремо найсвіжіші норми для кожної пари лінія+вид.
    """
    from collections import defaultdict
    import re as _re
    from datetime import datetime as _dt, date as _date, timedelta

    if not rows or len(rows) < 2:
        return {}

    norms = {}  # (line, vid) -> (date, norm_kg)

    for row in rows[1:]:
        if not row or len(row) < 6: continue
        # Дата (col B = index 1)
        date_val = row[1]
        dt = None
        if hasattr(date_val, 'strftime'):
            dt = date_val
        elif isinstance(date_val, (int, float)) and 40000 < date_val < 60000:
            dt = _date(1899, 12, 30) + timedelta(days=int(date_val))
        elif isinstance(date_val, str):
            for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
                try: dt = _dt.strptime(str(date_val)[:10], fmt).date(); break
                except: pass
        if dt is None: continue

        line = str(row[2]).strip().upper() if row[2] else ''
        vid  = str(row[3]).strip() if row[3] else ''
        if not line or not vid: continue

        # Норма готовой прод. кг (col F = index 5)
        try:
            norm = float(str(row[5]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
            if norm <= 0: continue
        except: continue

        # Беремо найсвіжіші норми
        key = (line, vid)
        if key not in norms or dt > norms[key][0]:
            norms[key] = (dt, norm)

    # Формуємо результат
    result = {}
    for (line, vid), (dt, norm) in norms.items():
        if line not in result: result[line] = {}
        result[line][vid] = norm

    print(f"  Norms parsed: {sum(len(v) for v in result.values())} entries for {len(result)} lines")
    return result


def parse_lines_heatmap(rows_list):
    """
    Агрегує кг готової продукції по лінії × місяць.
    Джерело: лист Журнал.Локация1 / Журнал.Локация2
    Структура: рядок зміни — дата вказана тільки в першому рядку блоку,
    далі None (forward-fill). Кожен рядок = одна лінія за зміну.
    col A=Дата(0), G=Лінія(6), J=Вага кг(9)
    """
    from collections import defaultdict
    import re as _re

    UA_SHORT = {
        '01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
        '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'
    }

    monthly = defaultdict(lambda: defaultdict(float))

    for rows in rows_list:
        if not rows: continue

        # Detect header rows (first 3) and find col indexes dynamically
        # Default known positions after inserting new column F:
        #   date=A(0), line=H(7), weight=K(10)
        ci_date = 0; ci_line = 7; ci_weight = 10
        data_start = 2  # data starts at row index 2 (0-based)

        # Auto-detect from headers — scan all header rows
        # Журнал has 2 header rows: row0=merged headers, row1=sub-headers with column names
        for hi in range(min(3, len(rows))):
            hdr = [str(c).lower().strip() if c else '' for c in rows[hi]]
            if any('лін' in h or 'лини' in h for h in hdr):
                for i, h in enumerate(hdr):
                    if 'дата' in h or h == 'date': ci_date = i
                    if 'лін' in h or 'лини' in h: ci_line = i
                    if 'вага' in h or ('вес' in h and 'кг' in h): ci_weight = i
                # data_start = рядок після останнього заголовку
                # Для журналу завжди 2 рядки заголовків → data_start=2
                data_start = max(hi + 1, 2)
                break

        # Detect if this is "Аналіз вкладів" (has contrib col) or "Журнал" (no contrib, forward-fill date)
        ci_contrib = -1
        for hi2 in range(min(3, len(rows))):
            hdr2 = [str(c).lower().strip() if c else '' for c in rows[hi2]]
            for i, h in enumerate(hdr2):
                if 'вклад %' in h or h == 'вклад%':
                    ci_contrib = i
                    break
            if ci_contrib >= 0:
                break
        is_analiz = ci_contrib >= 0
        print(f"  Lines HM: source={'Аналіз вкладів' if is_analiz else 'Журнал'}, cols: date={ci_date} line={ci_line} weight={ci_weight} contrib={ci_contrib}")

        current_date = None
        current_date_str = None
        seen_lines = set()  # dedup: (date_str, shift, line) — один запис на лінію-зміну-день
        for row in rows[data_start:]:
            if not row: continue
            # Forward-fill date
            d = row[ci_date] if ci_date < len(row) else None
            if d is not None and hasattr(d, 'strftime'):
                current_date = d
                current_date_str = d.strftime('%Y-%m-%d')
            elif d is not None and isinstance(d, (int, float)) and 40000 < d < 60000:
                from datetime import date as _date, timedelta
                current_date = _date(1899, 12, 30) + timedelta(days=int(d))
                current_date_str = current_date.strftime('%Y-%m-%d')
            elif d is not None and isinstance(d, str) and len(d) >= 7:
                try:
                    from datetime import datetime as _dt
                    current_date = _dt.strptime(d[:10], '%Y-%m-%d')
                    current_date_str = d[:10]
                except: pass
            if current_date is None:
                continue

            w = row[ci_weight] if ci_weight < len(row) else None
            if not isinstance(w, (int, float)) or w <= 0:
                continue

            line = str(row[ci_line] if ci_line < len(row) else '').upper().strip()
            if not line:
                continue
            m = _re.match(r'.*?(\d+)$', line)
            if m:
                line = f'ЛІНІЯ {m.group(1)}'

            # Дедупликація: (дата, зміна, лінія) — тільки перший оператор
            ci_shift_hm = 1 if is_analiz else 6
            shift_hm = str(row[ci_shift_hm]).strip() if ci_shift_hm < len(row) and row[ci_shift_hm] else ''
            line_key = (current_date_str, shift_hm, line)
            if line_key in seen_lines:
                continue
            seen_lines.add(line_key)

            # For "Аналіз вкладів": w = вага×вклад%, need w/contrib to get full line weight
            # For "Журнал": w = full line weight already
            if is_analiz and ci_contrib >= 0:
                contrib = row[ci_contrib] if ci_contrib < len(row) else None
                if isinstance(contrib, (int, float)) and contrib > 0:
                    if contrib > 1.5: contrib /= 100
                    w = w / contrib

            ym = current_date.strftime('%Y-%m') if hasattr(current_date, 'strftime') else str(current_date)[:7]
            monthly[line][ym] += w

    if not monthly:
        print("  Lines HM: no data")
        return [], {}

    # Filter out stray old months (keep only from 2025-11 onwards)
    all_months = sorted(
        ym for ym in set(m for d in monthly.values() for m in d)
        if ym >= '2025-11'
    )
    if not all_months:
        return [], {}

    hm_labels = [
        f"{UA_SHORT[ym.split('-')[1]]} {ym.split('-')[0][2:]}"
        for ym in all_months
    ]

    def line_num(ln):
        m = _re.search(r'(\d+)', ln)
        return int(m.group(1)) if m else 999

    hm_data = {}
    for line in sorted(monthly.keys(), key=line_num):
        hm_data[line] = [round(monthly[line].get(ym, 0)) for ym in all_months]

    total_kg = sum(sum(v.values()) for v in monthly.values())
    print(f"  Lines HM: {len(hm_data)} lines x {len(all_months)} months, total={round(total_kg):,} kg")
    return hm_labels, hm_data



def parse_retail(rows):
    """
    Читає лист "2026" з окремого Google Sheet роздрібних продажів.
    Структура: col0=№, col1=ПІБ, col2=Дата, col4=Сума грн, col5=Продукт, col7=Кількість кг.
    Повертає: {'sku_ret_kg': {sku: [14 values]}, 'sku_ret_grn': {sku: [14 values]},
               'ret_kg_by_month': {ym: total_kg}, 'ret_grn_by_month': {ym: total_grn}}
    """
    from collections import defaultdict
    from datetime import datetime as _dt

    SKU_DISPLAY = {
        'PETG 3.0': 'PETG 3кг', 'PETG 2.5': 'PETG 2.5кг',
        'PETG 1.0': 'PETG 1кг', 'PLA 3.0':  'PLA 3кг',
        'PLA 2.5':  'PLA 2.5кг',
    }

    if not rows or len(rows) < 2:
        print("  WARNING: parse_retail — no rows")
        return {}

    sku_kg  = defaultdict(lambda: [0.0] * MONTH_COUNT)
    sku_grn = defaultdict(lambda: [0.0] * MONTH_COUNT)
    ret_kg_by_month  = defaultdict(float)
    ret_grn_by_month = defaultdict(float)
    parsed = 0

    for row in rows[1:]:   # пропускаємо рядок заголовку
        if not row or len(row) < 8: continue
        # col2 = дата
        raw = row[2]
        d = None
        if hasattr(raw, 'strftime'):
            d = raw
        elif isinstance(raw, str):
            s = raw.strip()
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y'):
                try: d = _dt.strptime(s[:10], fmt); break
                except: pass
        if d is None: continue
        if d.year < 2020: continue
        ym = d.strftime('%Y-%m')
        try:
            mi = MONTH_ORDER.index(ym)
        except ValueError:
            continue

        # col5 = Продукт
        prod = str(row[5]).strip() if len(row) > 5 else ''
        sku  = SKU_DISPLAY.get(prod, prod if prod else None)
        if not sku or sku in ('nan', 'None', ''): continue

        def _n(v):
            try: return float(str(v).replace(',','.').replace(' ','').replace('\xa0','').strip()) if v else 0.0
            except: return 0.0

        kg  = _n(row[7]) if len(row) > 7 else 0.0
        grn = _n(row[4]) if len(row) > 4 else 0.0
        if kg <= 0 and grn <= 0: continue

        sku_kg[sku][mi]  += kg
        sku_grn[sku][mi] += grn
        ret_kg_by_month[ym]  += kg
        ret_grn_by_month[ym] += grn
        parsed += 1

    print(f"  parse_retail: {parsed} rows, {len(sku_kg)} SKUs")
    for ym in sorted(ret_kg_by_month):
        print(f"    {ym}: {round(ret_kg_by_month[ym],1)} кг, {round(ret_grn_by_month[ym]):,} грн")

    return {
        'sku_ret_kg':       {sk: [round(v,1) if v>0 else None for v in sku_kg[sk]]  for sk in sku_kg},
        'sku_ret_grn':      {sk: [round(v,1) if v>0 else None for v in sku_grn[sk]] for sk in sku_grn},
        'ret_kg_by_month':  dict(ret_kg_by_month),
        'ret_grn_by_month': dict(ret_grn_by_month),
    }


def parse_sales_from_journal(rows):
    """
    Парсить продажі з Журналу Відвантажень (SHIP_SHEET_ID, лист "Відвантаження") —
    об'єднаний реєстр ТОВ(Стрім)+ФОП(Роздріб)+Easy(Изи). Повертає ТОЧНО ТУ Ж
    структуру, що й parse_sales() (з _AllData_$) — щоб не чіпати нічого нижче
    за течією (template.html, main()).

    Чому перехід з _AllData_$ на Журнал: _AllData_$ відстає від бухгалтера і
    давав занижений/нульовий дохід за щойно завершені місяці (той самий баг,
    що ламав П&Л — Червень показував 0 замість факту). Журнал Відвантажень
    веде облік по факту відвантаження (дата+сума), без затримки бухобліку.

    Мапінг колонок реального листа (перевірено на живому файлі):
      'Дата відвантаження', 'Назва товару', 'Маса, кг', 'Сума, грн', 'Джерело'.
    Колонка "Джерело" містить коди:
      SRC1 = Роздріб (ФОП)      → канал "Розница"
      SRC2 = Опт (Стрім, ТОВ)   → канал "Опт", підканал opt1
      SRC3 = Опт2 (Изи, Easy)   → канал "Опт", підканал opt2
    (Це узгоджено з тим самим мапінгом, що вже використано в PnL_Generator.gs
    та Balance_Generator.gs для доходу ТОВ/ФОП/Easy.)
    """
    from collections import defaultdict
    import re

    if not rows or len(rows) < 2:
        print("  WARNING: журнал відвантажень порожній")
        return {}

    header = [str(c or '').strip().lower() for c in rows[0]]
    def find_col(*needles):
        for i, h in enumerate(header):
            if all(n in h for n in needles):
                return i
        return None

    col_date = find_col('дата', 'відвантаж')
    col_name = find_col('назва', 'товар')
    col_kg   = find_col('маса')
    col_sum  = find_col('сума')
    col_src  = find_col('джерело')

    if col_date is None or col_sum is None:
        print(f"  WARNING: не знайдено колонки Дата відвантаження/Сума у журналі. Заголовок: {header}")
        return {}

    data_rows = []
    for row in rows[1:]:
        if len(row) <= max(col_date, col_sum): continue
        date_raw = str(row[col_date]).strip()
        if not date_raw: continue
        try:
            from datetime import datetime as dt
            d = None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%m/%d/%Y'):
                try: d = dt.strptime(date_raw[:10], fmt); break
                except: pass
            if d is None:
                parts = date_raw.split('/')
                if len(parts) == 3:
                    p0, p1, p2 = int(parts[0]), int(parts[1]), int(parts[2])
                    if p2 > 1000: d = dt(p2, p0, p1)
                    else: d = dt(p2, p1, p0)
            if d is None: continue
            if d.year < 2020: continue
        except: continue

        try: revenue = float(str(row[col_sum]).replace(',', '.').replace(' ', '').replace('\xa0', '')) if row[col_sum] else 0
        except: revenue = 0
        kg = 0.0
        if col_kg is not None and len(row) > col_kg:
            try: kg = float(str(row[col_kg]).replace(',', '.').replace(' ', '').replace('\xa0', '')) if row[col_kg] else 0
            except: kg = 0
        if revenue <= 0 and kg <= 0: continue

        product = str(row[col_name]).strip() if col_name is not None and len(row) > col_name else ''
        src_code = str(row[col_src]).strip().upper() if col_src is not None and len(row) > col_src else ''

        if src_code == 'SRC1':
            channel, op_type = 'Розница', 'Роздріб'
        elif src_code == 'SRC2':
            channel, op_type = 'Опт', 'СТРИМТЕХНО'
        elif src_code == 'SRC3':
            channel, op_type = 'Опт', 'EASY'
        else:
            continue  # невизначене джерело — пропускаємо (не вигадуємо канал)

        plastic = 'PETG' if 'PETG' in product.upper() else ('PLA' if 'PLA' in product.upper() else '')
        ym = d.strftime('%Y-%m')
        data_rows.append({'ym': ym, 'channel': channel, 'product': product, 'plastic': plastic,
                           'revenue': revenue, 'kg': kg, 'op_type': op_type})

    if not data_rows:
        print("  WARNING: no sales data parsed from journal")
        return {}

    return _sales_rows_to_result(data_rows)


def _sales_rows_to_result(data_rows):
    """Спільна частина parse_sales/parse_sales_from_journal: перетворює вже
    розпарсені рядки {ym, channel, product, plastic, revenue, kg, op_type}
    на фінальний словник result — ІДЕНТИЧНИЙ формат для обох джерел."""
    from collections import defaultdict
    import re as _rsk

    months_sorted = sorted(set(r['ym'] for r in data_rows))

    monthly_opt = defaultdict(float)
    monthly_ret = defaultdict(float)
    monthly_opt1_kg = defaultdict(float)
    monthly_opt2_kg = defaultdict(float)
    monthly_ret_kg  = defaultdict(float)

    def get_kg_channel(op_type):
        t = str(op_type).strip()
        if 'СТРИМТЕХНО' in t or 'Стримтехно' in t: return 'opt1'
        if 'EASY' in t or 'Easy' in t:               return 'opt2'
        if 'Розниц' in t or 'розниц' in t:           return 'ret'
        return None

    for r in data_rows:
        if r['channel'] == 'Опт':     monthly_opt[r['ym']] += r['revenue']
        elif r['channel'] == 'Розница': monthly_ret[r['ym']] += r['revenue']
        ch = get_kg_channel(r.get('op_type', ''))
        if ch == 'opt1': monthly_opt1_kg[r['ym']] += r['kg']
        elif ch == 'opt2': monthly_opt2_kg[r['ym']] += r['kg']
        elif ch == 'ret':  monthly_ret_kg[r['ym']]  += r['kg']

    def mk_labels(months):
        UA_SHORT = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер','07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
        out = []
        for m in months:
            y, mo = m.split('-')
            out.append(f"{UA_SHORT[mo]} {y[2:]}")
        return out

    labels = mk_labels(months_sorted)
    sales_opt = [round(monthly_opt.get(m, 0)) for m in months_sorted]
    sales_ret = [round(monthly_ret.get(m, 0)) for m in months_sorted]

    prod_rev = defaultdict(float)
    for r in data_rows: prod_rev[r['product']] += r['revenue']
    top10 = sorted(prod_rev.items(), key=lambda x: -x[1])[:10]

    petg_rev = defaultdict(float); petg_kg = defaultdict(float)
    pla_rev  = defaultdict(float); pla_kg  = defaultdict(float)
    for r in data_rows:
        if r['plastic'] == 'PETG' and r['kg'] > 0:
            petg_rev[r['ym']] += r['revenue']; petg_kg[r['ym']] += r['kg']
        elif r['plastic'] == 'PLA' and r['kg'] > 0:
            pla_rev[r['ym']] += r['revenue']; pla_kg[r['ym']] += r['kg']

    petg_months = sorted(m for m in months_sorted if petg_kg.get(m, 0) > 0)
    pla_months  = sorted(m for m in months_sorted if pla_kg.get(m, 0) > 0)

    petg_price = [round(petg_rev[m]/petg_kg[m], 2) for m in petg_months]
    pla_price  = [round(pla_rev[m]/pla_kg[m], 2)  for m in pla_months]

    total_opt = sum(monthly_opt.values())
    total_ret = sum(monthly_ret.values())

    from datetime import datetime as _now_dt
    current_ym = _now_dt.utcnow().strftime('%Y-%m')
    closed_months = [m for m in months_sorted if m < current_ym]
    search_months = closed_months if closed_months else months_sorted
    best_ym  = max(search_months, key=lambda m: monthly_opt.get(m,0)+monthly_ret.get(m,0))
    best_tot = monthly_opt.get(best_ym,0) + monthly_ret.get(best_ym,0)
    best_o   = monthly_opt.get(best_ym,0)
    best_r   = monthly_ret.get(best_ym,0)
    UA_FULL  = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
                '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
    by, bm   = best_ym.split('-')
    best_label = f"{UA_FULL[bm]} {by}"

    donut_by_month = {}
    for m in months_sorted:
        donut_by_month[m] = [round(monthly_opt.get(m,0)), round(monthly_ret.get(m,0))]

    sales_opt1_kg = [round(monthly_opt1_kg.get(m, 0) / 1000, 3) for m in months_sorted]
    sales_opt2_kg = [round(monthly_opt2_kg.get(m, 0) / 1000, 3) for m in months_sorted]
    sales_ret_kg  = [round(monthly_ret_kg.get(m, 0)  / 1000, 3) for m in months_sorted]

    def _norm_sku(p):
        m = _rsk.search(r'(PETG|PLA)\s+(\d+[.,]?\d*)', str(p), _rsk.IGNORECASE)
        if not m: return None
        w = str(float(m.group(2).replace(',', '.'))).rstrip('0').rstrip('.')
        return f"{m.group(1).upper()} {w}кг"

    price_sum = {'PETG': 0.0, 'PLA': 0.0}
    price_cnt = {'PETG': 0,   'PLA': 0}
    for r in data_rows:
        if r['kg'] > 0 and r['revenue'] > 0:
            pl = 'PETG' if 'PETG' in r['plastic'].upper() else ('PLA' if 'PLA' in r['plastic'].upper() else None)
            if pl:
                price_sum[pl] += r['revenue'] / r['kg']
                price_cnt[pl] += 1
    avg_price = {pl: (price_sum[pl]/price_cnt[pl] if price_cnt[pl] else 330.0) for pl in price_sum}
    print(f"  Avg price PETG={avg_price['PETG']:.1f}, PLA={avg_price['PLA']:.1f} грн/кг")

    from collections import defaultdict as _dd2
    sku_opt = _dd2(lambda: [0.0] * MONTH_COUNT)
    sku_ret = _dd2(lambda: [0.0] * MONTH_COUNT)
    zero_kg_cnt = 0; zero_kg_rev = 0.0

    for r in data_rows:
        sku = _norm_sku(r['product'])
        if not sku: continue
        try:
            mi = MONTH_ORDER.index(r['ym'])
        except ValueError:
            continue

        kg = r['kg']
        if kg <= 0 and r['revenue'] > 0:
            pl = 'PETG' if sku.startswith('PETG') else 'PLA'
            kg = r['revenue'] / avg_price[pl]
            zero_kg_cnt += 1
            zero_kg_rev += r['revenue']

        if kg <= 0: continue

        if r['channel'] == 'Опт':
            sku_opt[sku][mi] += kg
        elif r['channel'] == 'Розница':
            sku_ret[sku][mi] += kg

    print(f"  Zero-kg rows fixed: {zero_kg_cnt}, revenue covered: {round(zero_kg_rev):,} грн")

    all_skus = sorted(
        set(list(sku_opt.keys()) + list(sku_ret.keys())),
        key=lambda s: (0 if s.startswith('PETG') else 1,
                       float(_rsk.search(r'(\d+\.?\d*)', s).group(1)) if _rsk.search(r'(\d+\.?\d*)', s) else 0)
    )
    print(f"  SKU sales keys: {all_skus}")

    for mk in ['2025-11','2026-04']:
        if mk in MONTH_ORDER:
            mi = MONTH_ORDER.index(mk)
            tot_o = sum(sku_opt[sk][mi] for sk in all_skus)
            tot_r = sum(sku_ret[sk][mi] for sk in all_skus)
            print(f"  {mk}: Опт={round(tot_o):,} кг, Роздр={round(tot_r):,} кг")

    sku_sales_opt = {sk: [round(v, 1) if v > 0 else None for v in sku_opt[sk]] for sk in all_skus}
    sku_sales_ret = {sk: [round(v, 1) if v > 0 else None for v in sku_ret[sk]] for sk in all_skus}

    result = {
        'sales_labels':      labels,
        'sales_months':      months_sorted,
        'sales_opt':         sales_opt,
        'sales_ret':         sales_ret,
        'sales_opt1_kg':     sales_opt1_kg,
        'sales_opt2_kg':     sales_opt2_kg,
        'sales_ret_kg':      sales_ret_kg,
        'top_labels':        [x[0] for x in top10],
        'top_data':          [round(x[1]) for x in top10],
        'petg_price_labels': mk_labels(petg_months),
        'petg_avg_price':    petg_price,
        'pla_price_labels':  mk_labels(pla_months),
        'pla_avg_price':     pla_price,
        'total_opt':         round(total_opt),
        'total_ret':         round(total_ret),
        'best_month_label':  best_label,
        'best_month_total':  round(best_tot/1e6, 1),
        'best_month_opt':    round(best_o/1e6, 1),
        'best_month_ret':    round(best_r/1e6, 1),
        'donut_by_month':    donut_by_month,
        'sku_sales_opt':     sku_sales_opt,
        'sku_sales_ret':     sku_sales_ret,
        'sku_list':          all_skus,
    }
    print(f"  Sales: {len(months_sorted)} months, opt={round(total_opt/1e6,1)}M, ret={round(total_ret/1e6,1)}M")
    return result


def parse_sales(rows):
    """
    Парсит продажи из листа _AllData_$ — выторг по каналам по месяцам,
    топ продуктов и средние цены PETG/PLA.

    ПРИМІТКА (07.2026): вкладка "Продажі" на дашборді тепер живиться з
    parse_sales_from_journal() (Журнал Відвантажень), а не звідси — _AllData_$
    відставав від бухгалтера і давав занижений дохід за останні місяці.
    Функція лишена як є (для довідки/можливого відкату), у main() більше
    не викликається для вкладки "Продажі".
    """
    from collections import defaultdict
    import re

    # Пропускаем строки-заголовки (первые 2)
    data_rows = []
    for row in rows[1:]:
        if len(row) < 10: continue
        date_raw = str(row[0]).strip()
        if not date_raw or date_raw in ('Дата', 'NaN', 'nan', ''): continue
        # Дата — підтримуємо всі формати включно з M/D/YYYY без нулів
        try:
            from datetime import datetime as dt
            d = None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%m/%d/%Y'):
                try: d = dt.strptime(date_raw[:10], fmt); break
                except: pass
            if d is None:
                parts = date_raw.split('/')
                if len(parts) == 3:
                    p0, p1, p2 = int(parts[0]), int(parts[1]), int(parts[2])
                    if p2 > 1000: d = dt(p2, p0, p1)
                    else: d = dt(p2, p1, p0)
            if d is None: continue
            if d.year < 2020: continue  # відкидаємо 1899 та інші фіктивні дати
        except: continue
        channel = str(row[10]).strip() if len(row) > 10 else ''
        if channel not in ('Опт', 'Розница'): continue
        product = str(row[1]).strip()
        plastic  = str(row[9]).strip()
        try: revenue = float(str(row[5]).replace(',','.').replace(' ','').replace('\xa0','')) if row[5] else 0
        except: revenue = 0
        try: kg = float(str(row[4]).replace(',','.').replace(' ','').replace('\xa0','')) if row[4] else 0
        except: kg = 0
        if revenue <= 0 and kg <= 0: continue
        ym = d.strftime('%Y-%m')
        op_type = str(row[6]).strip() if len(row) > 6 else ''
        data_rows.append({'ym':ym, 'channel':channel, 'product':product, 'plastic':plastic, 'revenue':revenue, 'kg':kg, 'op_type':op_type})

    if not data_rows:
        print("  WARNING: no sales data parsed")
        return {}

    # Sort months
    months_sorted = sorted(set(r['ym'] for r in data_rows))

    # Monthly by channel — revenue and kg
    monthly_opt = defaultdict(float)
    monthly_ret = defaultdict(float)
    monthly_opt1_kg = defaultdict(float)  # Опт_СТРИМТЕХНО
    monthly_opt2_kg = defaultdict(float)  # Опт_EASY
    monthly_ret_kg  = defaultdict(float)  # Розниця

    # Маппінг типу операції → канал kg
    def get_kg_channel(op_type):
        t = str(op_type).strip()
        if 'СТРИМТЕХНО' in t or 'Стримтехно' in t: return 'opt1'
        if 'EASY' in t or 'Easy' in t:               return 'opt2'
        if 'Розниц' in t or 'розниц' in t:           return 'ret'
        return None

    for r in data_rows:
        if r['channel'] == 'Опт':     monthly_opt[r['ym']] += r['revenue']
        elif r['channel'] == 'Розница': monthly_ret[r['ym']] += r['revenue']
        ch = get_kg_channel(r.get('op_type', ''))
        if ch == 'opt1': monthly_opt1_kg[r['ym']] += r['kg']
        elif ch == 'opt2': monthly_opt2_kg[r['ym']] += r['kg']
        elif ch == 'ret':  monthly_ret_kg[r['ym']]  += r['kg']

    def mk_labels(months):
        UA_SHORT = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер','07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
        out = []
        for m in months:
            y, mo = m.split('-')
            out.append(f"{UA_SHORT[mo]} {y[2:]}")
        return out

    labels = mk_labels(months_sorted)
    sales_opt = [round(monthly_opt.get(m, 0)) for m in months_sorted]
    sales_ret = [round(monthly_ret.get(m, 0)) for m in months_sorted]

    # Top-10 products
    prod_rev = defaultdict(float)
    for r in data_rows: prod_rev[r['product']] += r['revenue']
    top10 = sorted(prod_rev.items(), key=lambda x: -x[1])[:10]

    # Avg price per kg per plastic per month
    petg_rev = defaultdict(float); petg_kg = defaultdict(float)
    pla_rev  = defaultdict(float); pla_kg  = defaultdict(float)
    for r in data_rows:
        if r['plastic'] == 'PETG' and r['kg'] > 0:
            petg_rev[r['ym']] += r['revenue']; petg_kg[r['ym']] += r['kg']
        elif r['plastic'] == 'PLA' and r['kg'] > 0:
            pla_rev[r['ym']] += r['revenue']; pla_kg[r['ym']] += r['kg']

    petg_months = sorted(m for m in months_sorted if petg_kg.get(m, 0) > 0)
    pla_months  = sorted(m for m in months_sorted if pla_kg.get(m, 0) > 0)

    petg_price = [round(petg_rev[m]/petg_kg[m], 2) for m in petg_months]
    pla_price  = [round(pla_rev[m]/pla_kg[m], 2)  for m in pla_months]

    total_opt = sum(monthly_opt.values())
    total_ret = sum(monthly_ret.values())

    # Best month — динамічно шукаємо максимум по сумі opt+ret
    # Виключаємо поточний місяць (неповний), якщо він останній
    from datetime import datetime as _now_dt
    current_ym = _now_dt.utcnow().strftime('%Y-%m')
    closed_months = [m for m in months_sorted if m < current_ym]
    search_months = closed_months if closed_months else months_sorted
    best_ym  = max(search_months, key=lambda m: monthly_opt.get(m,0)+monthly_ret.get(m,0))
    best_tot = monthly_opt.get(best_ym,0) + monthly_ret.get(best_ym,0)
    best_o   = monthly_opt.get(best_ym,0)
    best_r   = monthly_ret.get(best_ym,0)
    UA_FULL  = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
                '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
    by, bm   = best_ym.split('-')
    best_label = f"{UA_FULL[bm]} {by}"

    # Donut по місяцях: {ym: [opt, ret]} для JS перемикача
    donut_by_month = {}
    for m in months_sorted:
        donut_by_month[m] = [round(monthly_opt.get(m,0)), round(monthly_ret.get(m,0))]

    sales_opt1_kg = [round(monthly_opt1_kg.get(m, 0) / 1000, 3) for m in months_sorted]
    sales_opt2_kg = [round(monthly_opt2_kg.get(m, 0) / 1000, 3) for m in months_sorted]
    sales_ret_kg  = [round(monthly_ret_kg.get(m, 0)  / 1000, 3) for m in months_sorted]

    # ── SKU-продажі для вкладки Товар ──
    # Нормалізуємо назву: "PETG 2.5 кг (чорний)" → "PETG 2.5кг"
    import re as _rsk
    def _norm_sku(p):
        m = _rsk.search(r'(PETG|PLA)\s+(\d+[.,]?\d*)', str(p), _rsk.IGNORECASE)
        if not m: return None
        w = str(float(m.group(2).replace(',', '.'))).rstrip('0').rstrip('.')
        return f"{m.group(1).upper()} {w}кг"

    # Рахуємо середню ціну по пластику (для fallback де kg=0)
    price_sum = {'PETG': 0.0, 'PLA': 0.0}
    price_cnt = {'PETG': 0,   'PLA': 0}
    for r in data_rows:
        if r['kg'] > 0 and r['revenue'] > 0:
            pl = 'PETG' if 'PETG' in r['plastic'].upper() else ('PLA' if 'PLA' in r['plastic'].upper() else None)
            if pl:
                price_sum[pl] += r['revenue'] / r['kg']
                price_cnt[pl] += 1
    avg_price = {pl: (price_sum[pl]/price_cnt[pl] if price_cnt[pl] else 330.0) for pl in price_sum}
    print(f"  Avg price PETG={avg_price['PETG']:.1f}, PLA={avg_price['PLA']:.1f} грн/кг")

    from collections import defaultdict as _dd2
    sku_opt = _dd2(lambda: [0.0] * MONTH_COUNT)
    sku_ret = _dd2(lambda: [0.0] * MONTH_COUNT)
    # Діагностика
    zero_kg_cnt = 0; zero_kg_rev = 0.0

    for r in data_rows:
        sku = _norm_sku(r['product'])
        if not sku: continue
        try:
            mi = MONTH_ORDER.index(r['ym'])
        except ValueError:
            continue

        kg = r['kg']
        # Fallback: якщо kg=0 але є виручка — обчислюємо з ціни
        if kg <= 0 and r['revenue'] > 0:
            pl = 'PETG' if sku.startswith('PETG') else 'PLA'
            kg = r['revenue'] / avg_price[pl]
            zero_kg_cnt += 1
            zero_kg_rev += r['revenue']

        if kg <= 0: continue

        if r['channel'] == 'Опт':
            sku_opt[sku][mi] += kg
        elif r['channel'] == 'Розница':
            sku_ret[sku][mi] += kg

    print(f"  Zero-kg rows fixed: {zero_kg_cnt}, revenue covered: {round(zero_kg_rev):,} грн")

    all_skus = sorted(
        set(list(sku_opt.keys()) + list(sku_ret.keys())),
        key=lambda s: (0 if s.startswith('PETG') else 1,
                       float(_rsk.search(r'(\d+\.?\d*)', s).group(1)) if _rsk.search(r'(\d+\.?\d*)', s) else 0)
    )
    print(f"  SKU sales keys: {all_skus}")

    # Діагностика по місяцях
    for mk in ['2025-11','2026-04']:
        if mk in MONTH_ORDER:
            mi = MONTH_ORDER.index(mk)
            tot_o = sum(sku_opt[sk][mi] for sk in all_skus)
            tot_r = sum(sku_ret[sk][mi] for sk in all_skus)
            print(f"  {mk}: Опт={round(tot_o):,} кг, Роздр={round(tot_r):,} кг")

    sku_sales_opt = {sk: [round(v, 1) if v > 0 else None for v in sku_opt[sk]] for sk in all_skus}
    sku_sales_ret = {sk: [round(v, 1) if v > 0 else None for v in sku_ret[sk]] for sk in all_skus}
    # retail_data може бути переданий ззовні (з окремого Google Sheet)
    # merging відбувається в main() після виклику parse_sales + parse_retail

    result = {
        'sales_labels':      labels,
        'sales_months':      months_sorted,
        'sales_opt':         sales_opt,
        'sales_ret':         sales_ret,
        'sales_opt1_kg':     sales_opt1_kg,
        'sales_opt2_kg':     sales_opt2_kg,
        'sales_ret_kg':      sales_ret_kg,
        'top_labels':        [x[0] for x in top10],
        'top_data':          [round(x[1]) for x in top10],
        'petg_price_labels': mk_labels(petg_months),
        'petg_avg_price':    petg_price,
        'pla_price_labels':  mk_labels(pla_months),
        'pla_avg_price':     pla_price,
        'total_opt':         round(total_opt),
        'total_ret':         round(total_ret),
        'best_month_label':  best_label,
        'best_month_total':  round(best_tot/1e6, 1),
        'best_month_opt':    round(best_o/1e6, 1),
        'best_month_ret':    round(best_r/1e6, 1),
        'donut_by_month':    donut_by_month,
        'sku_sales_opt':     sku_sales_opt,
        'sku_sales_ret':     sku_sales_ret,
        'sku_list':          all_skus,
    }
    print(f"  Sales: {len(months_sorted)} months, opt={round(total_opt/1e6,1)}M, ret={round(total_ret/1e6,1)}M")
    return result


def jv(v):
    if v is None: return 'null'
    if isinstance(v, bool): return 'true' if v else 'false'
    if isinstance(v, list): return '['+','.join(jv(x) for x in v)+']'
    if isinstance(v, dict):
        inner = ','.join('"'+str(k)+'"'+':'+jv(val) for k, val in v.items())
        return '{' + inner + '}'
    if isinstance(v, str): return '"'+v.replace('\\', '\\\\').replace('"', '\\"')+'"'
    return str(v)






def generate_quality_table_rows(data):
    """
    Генерує рядки зведеної таблиці якості для всіх місяців з даними.
    Повертає HTML рядки для <tbody>.
    """
    from datetime import datetime as _dt
    UA_FULL = {
        '2025-11':'Листопад 2025','2025-12':'Грудень 2025',
        '2026-01':'Січень 2026','2026-02':'Лютий 2026','2026-03':'Березень 2026',
        '2026-04':'Квітень 2026','2026-05':'Травень 2026','2026-06':'Червень 2026',
        '2026-07':'Липень 2026','2026-08':'Серпень 2026','2026-09':'Вересень 2026',
        '2026-10':'Жовтень 2026','2026-11':'Листопад 2026','2026-12':'Грудень 2026',
    }
    cur_ym = _dt.utcnow().strftime('%Y-%m')
    rows_html = ''

    for i, ym in enumerate(MONTH_ORDER):
        petg_nf   = data['petg_nf'][i]
        petg_w    = data['petg_waste'][i]
        pla_nf    = data['pla_nf'][i]
        pla_w     = data['pla_waste'][i]
        total_nf  = data['nf_pct'][i]
        total_w   = data['waste_pct'][i]

        # Пропускаємо місяці без жодних даних
        if all(v is None for v in [petg_nf, petg_w, pla_nf, pla_w, total_nf, total_w]):
            continue

        label = UA_FULL.get(ym, ym)
        is_current = ym == cur_ym
        is_closed  = ym < cur_ym

        # Зірочка для поточного місяця
        if is_current:
            label += '*'

        def cell(v, warn_thr=10.0, good_thr=5.0):
            if v is None: return '<td>—</td>'
            color = 'var(--bad)' if v > warn_thr else ('var(--warn)' if v > good_thr else 'var(--good)')
            return f'<td style="color:{color}">{v:.2f}%</td>'

        # Статус
        if not is_closed:
            badge = '<span class="badge badge-yellow">МІСЯЦЬ НЕ ЗАКРИТ</span>'
        elif total_nf is not None and total_nf > 10:
            badge = '<span class="badge badge-red">КРИТИЧНО!</span>'
        elif any(v is None for v in [petg_nf, petg_w, pla_nf, pla_w]):
            badge = '<span class="badge badge-yellow">ДАНІ НЕПОВНІ</span>'
        elif total_nf is not None and total_nf < 4 and (total_w or 0) < 3:
            badge = '<span class="badge badge-green">ДОБРЕ</span>'
        else:
            badge = '<span class="badge badge-green">НОРМА</span>'

        petg_nf_cell  = cell(petg_nf,  warn_thr=10.0, good_thr=5.0)
        petg_w_cell   = cell(petg_w,   warn_thr=5.0,  good_thr=2.0)
        pla_nf_cell   = cell(pla_nf,   warn_thr=10.0, good_thr=5.0)
        pla_w_cell    = cell(pla_w,    warn_thr=5.0,  good_thr=2.0)
        tot_nf_cell   = cell(total_nf, warn_thr=10.0, good_thr=5.0)
        tot_w_cell    = cell(total_w,  warn_thr=5.0,  good_thr=2.0)

        rows_html += f'<tr><td>{label}</td>{petg_nf_cell}{petg_w_cell}{pla_nf_cell}{pla_w_cell}{tot_nf_cell}{tot_w_cell}<td>{badge}</td></tr>\n'

    return rows_html or '<tr><td colspan="8" style="text-align:center;color:var(--muted)">Немає даних</td></tr>'

def generate(data, calc, calc_ext, sales=None, okr=None, hm_labels=None, hm_data=None, line_norms=None, mode="top", config=None, data_errors=None):
    if line_norms is None: line_norms = {}
    if config is None: config = DEFAULT_CONFIG
    tabs_cfg = config.get('tabs', DEFAULT_CONFIG['tabs'])
    active_tabs = [k for k, v in tabs_cfg.items() if v.get(mode, False)]
    _mode_str          = json.dumps(mode)
    _middle_config_str = json.dumps(tabs_cfg, ensure_ascii=False)
    _active_tabs_str   = json.dumps(active_tabs, ensure_ascii=False)
    _data_errors_str   = json.dumps(data_errors or {}, ensure_ascii=False)

    with open('template.html', 'r', encoding='utf-8') as f:
        html = f.read()
    # KPI — останній закритий місяць
    from datetime import datetime as _dt_kpi
    _cur_ym = _dt_kpi.utcnow().strftime('%Y-%m')
    _closed = [(i,m) for i,m in enumerate(MONTH_ORDER) if m < _cur_ym and data['total_prod'][i] is not None]
    if _closed:
        _ki, _km = _closed[-1]
        _prev = _closed[-2] if len(_closed)>1 else None
        _UA = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
               '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
        _by, _bm = _km.split('-')
        _kpi_label = f"{_UA[_bm]} '{_by[2:]}"
        _tot = data['total_prod'][_ki] or 0
        _petg = data['petg_prod'][_ki] or 0
        _pla = data['pla_prod'][_ki] or 0
        def _fmt(v): return f"{round(v):,}".replace(',', ' ')
        if _prev:
            _pi, _pm = _prev
            _prev_tot = data['total_prod'][_pi] or 0
            _prev_petg = data['petg_prod'][_pi] or 0
            _UA2 = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
                    '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
            _py, _pm2 = _pm.split('-')
            _prev_label = f"{_UA2[_pm2]} '{_py[2:]}"
            if _prev_tot > 0:
                _pct = round((_tot - _prev_tot) / _prev_tot * 100, 1)
                _arrow = '▲' if _pct >= 0 else '▼'
                _cls = 'delta-up' if _pct >= 0 else 'delta-down'
                _delta_tot = f"{_arrow} {'+' if _pct>=0 else ''}{_pct}% vs {_prev_label} ({_fmt(_prev_tot)} кг)"
                _pct_p = round((_petg - _prev_petg) / _prev_petg * 100, 1) if _prev_petg > 0 else 0
                _arr_p = '▲' if _pct_p >= 0 else '▼'
                _cls_p = 'delta-up' if _pct_p >= 0 else 'delta-down'
                _delta_petg = f"{_arr_p} {'+' if _pct_p>=0 else ''}{_pct_p}% vs {_prev_label}"
            else:
                _delta_tot = '—'; _cls = 'delta-up'; _delta_petg = '—'; _cls_p = 'delta-up'
        else:
            _delta_tot = '—'; _cls = 'delta-up'; _delta_petg = '—'; _cls_p = 'delta-up'
    else:
        _kpi_label = '—'; _fmt = lambda v: '—'
        _tot=_petg=_pla=0; _delta_tot='—'; _cls='delta-up'; _delta_petg='—'; _cls_p='delta-up'
        _fmt = lambda v: str(round(v))

    subs = {
        '{{UPDATED}}':         data['updated'],
        '{{QUALITY_TABLE_ROWS}}': generate_quality_table_rows(data),
        '{{KPI_MONTH_LABEL}}':  _kpi_label,
        '{{KPI_TOTAL_KG}}':      _fmt(_tot),
        '{{KPI_PETG_KG}}':       _fmt(_petg),
        '{{KPI_PLA_KG}}':        _fmt(_pla),
        '{{KPI_TOTAL_DELTA}}':   _delta_tot,
        '{{KPI_TOTAL_DELTA_CLASS}}': _cls,
        '{{KPI_PETG_DELTA}}':    _delta_petg,
        '{{KPI_PETG_DELTA_CLASS}}':  _cls_p,
        '{{PETG_PROD}}':       jv(data['petg_prod']),
        '{{PLA_PROD}}':        jv(data['pla_prod']),
        '{{TOTAL_PROD}}':      jv(data['total_prod']),
        '{{NF_PCT}}':          jv(data['nf_pct']),
        '{{PETG_NF}}':         jv(data['petg_nf']),
        '{{PLA_NF}}':          jv(data['pla_nf']),
        '{{PETG_NF_KG}}':      jv(data.get('petg_nf_kg', [None]*MONTH_COUNT)),
        '{{PLA_NF_KG}}':       jv(data.get('pla_nf_kg',  [None]*MONTH_COUNT)),
        '{{PETG_PACKED}}':     jv(data.get('petg_packed', [None]*MONTH_COUNT)),
        '{{PLA_PACKED}}':      jv(data.get('pla_packed',  [None]*MONTH_COUNT)),
        '{{PETG_WASTE}}':      jv(data['petg_waste']),
        '{{PLA_WASTE}}':       jv(data['pla_waste']),
        '{{PETG_WASTE_KG}}':   jv(data.get('petg_waste_kg', [None]*MONTH_COUNT)),
        '{{PLA_WASTE_KG}}':    jv(data.get('pla_waste_kg',  [None]*MONTH_COUNT)),
        '{{PETG_PCS}}':        jv(data.get('petg_pcs',      [None]*MONTH_COUNT)),
        '{{PLA_PCS}}':         jv(data.get('pla_pcs',       [None]*MONTH_COUNT)),
        '/*INCOME*/[null,null,null,null,null,null,null,null,null,null,null,null,null,null]': jv(data['income']),
        '/*EXPENSES*/[null,null,null,null,null,null,null,null,null,null,null,null,null,null]': jv(data['expenses']),
        '{{PROFIT}}':          jv(data['profit']),
        '{{COST_PETG_KG}}':    jv(data['cost_petg_kg']),
        '{{COST_PLA_KG}}':     jv(data['cost_pla_kg']),
        '{{CAL_LABELS}}':      jv(data.get('cal_labels', [])),
        '{{CAL_BAL_START}}':   jv(data.get('cal_bal_start', [])),
        '{{CAL_BAL_END}}':     jv(data.get('cal_bal_end', [])),
        '{{CAL_INCOME}}':      jv(data.get('cal_income', [])),
        '{{CAL_OUT}}':         jv(data.get('cal_out', [])),
        '{{CALC_PETG_PRICE}}': str(calc['petg_price']),
        '{{CALC_PLA_PRICE}}':  str(calc['pla_price']),
        '{{CALC_WASTE_PCT}}':  str(calc['waste_pct']),
        '{{CALC_EX_GRANULE}}': str(calc_ext['granule']),
        '{{HM_LABELS}}':       jv(hm_labels or []),
        '{{HM_NORMS}}':        jv(line_norms or {}),
        '{{HM_DATA}}':         jv(hm_data or {}),
    }
    if sales:
        # FC_FACT: {місяць_номер: тонни} для прогнозу — ВИРОБНИЦТВО з журналів
        # Пріоритет: data['total_prod'] (кг з журналів) → більш точний показник ніж продажі
        # Fallback: сума продажів (opt1+opt2+ret) якщо журнальних даних немає
        fc_fact = {}
        fc_last_m = 0
        total_prod = data.get('total_prod', [])  # кг, індекси = MONTH_ORDER
        opt1 = sales.get('sales_opt1_kg', [])
        opt2 = sales.get('sales_opt2_kg', [])
        ret  = sales.get('sales_ret_kg',  [])
        from datetime import datetime as _now
        # Беремо тільки ЗАКІНЧЕНІ місяці — поточний місяць виключаємо
        _current_ym = _now.utcnow().strftime('%Y-%m')
        for i, ym in enumerate(MONTH_ORDER):
            if not ym.startswith('2026'): continue
            if ym >= _current_ym: continue  # поточний і майбутні — пропускаємо
            month_num = int(ym.split('-')[1])  # 01→1, 05→5 тощо
            # Спочатку беремо виробництво (кг → тонни)
            prod_kg = total_prod[i] if i < len(total_prod) and total_prod[i] else None
            if prod_kg and prod_kg > 0:
                total_t = round(prod_kg / 1000, 1)
            else:
                # Fallback до продажів
                sales_kg = 0
                if i < len(opt1) and opt1[i]: sales_kg += opt1[i]
                if i < len(opt2) and opt2[i]: sales_kg += opt2[i]
                if i < len(ret)  and ret[i]:  sales_kg += ret[i]
                total_t = round(sales_kg / 1000, 1) if sales_kg > 0 else 0
            if total_t > 0:
                fc_fact[month_num] = total_t
                fc_last_m = month_num
        print(f"  FC_FACT (closed months only, production-based): {fc_fact}, LAST_M: {fc_last_m}")
        subs.update({
            '{{FC_FACT}}':  jv(fc_fact) if fc_fact else '{1:25,2:31,3:45,4:50}',
            '{{FC_LAST_M}}': str(fc_last_m) if fc_last_m else '4',
        })
        subs.update({
            '{{SALES_LABELS}}':       jv(sales['sales_labels']),
            '{{SALES_OPT}}':          jv(sales['sales_opt']),
            '{{SALES_RET}}':          jv(sales['sales_ret']),
            '{{SALES_OPT1_KG}}':      jv(sales.get('sales_opt1_kg', [])),
            '{{SALES_OPT2_KG}}':      jv(sales.get('sales_opt2_kg', [])),
            '{{SALES_RET_KG}}':       jv(sales.get('sales_ret_kg', [])),
            '{{TOP_PRODUCTS_LABELS}}':jv(sales['top_labels']),
            '{{TOP_PRODUCTS_DATA}}':  jv(sales['top_data']),
            '{{PETG_PRICE_LABELS}}':  jv(sales['petg_price_labels']),
            '{{PETG_AVG_PRICE}}':     jv(sales['petg_avg_price']),
            '{{PLA_PRICE_LABELS}}':   jv(sales['pla_price_labels']),
            '{{PLA_AVG_PRICE}}':      jv(sales['pla_avg_price']),
            '{{SALES_MONTHS}}':       jv(sales.get('sales_months', [])),
            '{{BEST_MONTH_LABEL}}':   sales.get('best_month_label', '—'),
            '{{BEST_MONTH_TOTAL}}':   str(sales.get('best_month_total', 0)),
            '{{BEST_MONTH_OPT}}':     str(sales.get('best_month_opt', 0)),
            '{{BEST_MONTH_RET}}':     str(sales.get('best_month_ret', 0)),
            '{{DONUT_BY_MONTH}}':     jv(sales.get('donut_by_month', {})),
            '{{SKU_SALES_OPT}}':      jv(sales.get('sku_sales_opt', {})),
            '{{SKU_SALES_RET}}':      jv(sales.get('sku_sales_ret', {})),
            '{{SKU_LIST}}':           jv(sales.get('sku_list', [])),
            '{{PROD_BY_SKU}}':        jv(data.get('prod_by_sku', {})),
            '{{PROD_SKU_LIST}}':      jv(data.get('prod_sku_list', [])),
        })
    else:
        subs.update({
            '{{SALES_LABELS}}':       '[]',
            '{{SALES_OPT}}':          '[]',
            '{{SALES_RET}}':          '[]',
            '{{SALES_OPT1_KG}}':      '[]',
            '{{SALES_OPT2_KG}}':      '[]',
            '{{SALES_RET_KG}}':       '[]',
            '{{TOP_PRODUCTS_LABELS}}':'[]',
            '{{TOP_PRODUCTS_DATA}}':  '[]',
            '{{PETG_PRICE_LABELS}}':  '[]',
            '{{PETG_AVG_PRICE}}':     '[]',
            '{{PLA_PRICE_LABELS}}':   '[]',
            '{{PLA_AVG_PRICE}}':      '[]',
            '{{SALES_MONTHS}}':       '[]',
            '{{BEST_MONTH_LABEL}}':   '—',
            '{{BEST_MONTH_TOTAL}}':   '0',
            '{{BEST_MONTH_OPT}}':     '0',
            '{{BEST_MONTH_RET}}':     '0',
            '{{DONUT_BY_MONTH}}':     '{}',
            '{{SKU_SALES_OPT}}':      '{}',
            '{{SKU_SALES_RET}}':      '{}',
            '{{SKU_LIST}}':           '[]',
            '{{PROD_BY_SKU}}':        jv(data.get('prod_by_sku', {})),
            '{{PROD_SKU_LIST}}':      jv(data.get('prod_sku_list', [])),
        })
    # OKR placeholders — завжди замінюємо, навіть якщо okr=None (щоб не було JS syntax error)
    subs.update({
        '{{OKR_COMPANY_PCT}}':   str(round(okr['company_pct'] * 100, 1)) if okr else '0',
        '{{OKR_DATA}}':          okr['okr_data_json']  if okr else '[]',
        '{{OKR_PEOPLE}}':        okr['people_json']    if okr else '[]',
        '{{OKR_KR_DATA}}':       okr['kr_data_json']   if okr else '[]',
        '{{OKR_HISTORY}}':       okr['okr_history_json'] if okr and okr.get('okr_history_json') else 'null',
    })
    # ── Mode/config placeholders (for tab visibility + admin panel) ──
    subs.update({
        '{{DASHBOARD_MODE}}': _mode_str,
        '{{MIDDLE_CONFIG}}':  _middle_config_str,
        '{{ACTIVE_TABS}}':    _active_tabs_str,
        '{{DATA_ERRORS}}':    _data_errors_str,
    })

    for k, v in subs.items():
        html = html.replace(k, v)
    missing = [k for k in subs if k in html]
    if missing: print(f"WARNING unreplaced: {missing}")
    return html

if __name__ == '__main__':
    line_norms = {}

    # ── 1a. Виробництво (_AllData_Product) — для вкладів/операторів ──
    # Залишаємо як є — використовується для heatmap, операторів, мотивації.
    prod_rows = []
    data = None
    try:
        prod_rows = fetch_csv(SHEET_ID, "_AllData_Product")
        data = parse_production_from_alldata(prod_rows)
    except Exception as e:
        print(f"WARNING _AllData_Product: {e}")
        import traceback; traceback.print_exc()
    if data is None:
        data = _empty_production()

    # ── 1b. Виробництво з Журналів локацій — для кг/НФ/Брак/ПнЛ ─────
    # LINES_SHEET_ID  = Журнал Локація 1 (1SewXdbiFVIUPCESo5XDrRzvvG5rut4vuQTDyBDg3qp4)
    # LINES_SHEET_ID2 = Журнал Локація 2 (1NJkxtyha_oSpeaB7Jzmf440-kOF2gHBB0xsaMfKPRsI)
    # Назви листів журналів. Щоб додати нову локацію — додайте (SHEET_ID, "Журнал.ЛокаціяN").
    JOURNAL_SOURCES = [
        (LINES_SHEET_ID,  "Журнал.Локация1"),
        (LINES_SHEET_ID2, "Журнал.Локация2"),
    ]
    journal_rows_list = []
    for _jid, _jsheet in JOURNAL_SOURCES:
        try:
            _rows = fetch_csv(_jid, _jsheet)
            print(f"  Journal '{_jsheet}': {len(_rows)} rows")
            journal_rows_list.append(_rows)
        except Exception as e:
            print(f"  WARNING journal '{_jsheet}': {e}")

    if journal_rows_list:
        try:
            jdata = parse_production_from_journals(journal_rows_list)
            # Перезаписуємо поля виробництва/НФ/Брак з точних даних журналів.
            # Поля вкладів операторів (_AllData_Product) залишаються незмінними.
            for _key in ('petg_prod', 'pla_prod', 'total_prod',
                         'petg_nf', 'pla_nf', 'petg_nf_kg', 'pla_nf_kg', 'nf_pct',
                         'petg_waste', 'pla_waste', 'waste_pct'):
                data[_key] = jdata[_key]
            # Нові поля для ПнЛ (kg абсолютні значення НФ/Брак)
            data['petg_waste_kg'] = jdata['petg_waste_kg']
            data['pla_waste_kg']  = jdata['pla_waste_kg']
            data['petg_pcs']      = jdata['petg_pcs']
            data['pla_pcs']       = jdata['pla_pcs']
            data['prod_by_sku']   = jdata['prod_by_sku']
            data['prod_sku_list'] = jdata['prod_sku_list']
            print("  Journal data merged into data[] OK")
            print(f"  prod_sku_list: {jdata['prod_sku_list']}")
        except Exception as e:
            print(f"  WARNING parse_production_from_journals: {e}")
            import traceback; traceback.print_exc()
    else:
        print("  WARNING: no journal rows fetched — using _AllData_Product for production")

    # ── 2. Фінансові дані (_AllData_Sebest) ────────────────────
    try:
        fin_rows = fetch_csv(SHEET_ID, "_AllData_Sebest")
        print(f"  _AllData_Sebest: {len(fin_rows)} rows")
        col_map_kg, _ = detect_month_columns(fin_rows)
        col_map_grn = {mk: ci + 1 for mk, ci in col_map_kg.items()}
        print(f"  col_map_kg:  {col_map_kg}")
        print(f"  col_map_grn: {col_map_grn}")
        print("  _AllData_Sebest row labels (col A):")
        for i, row in enumerate(fin_rows):
            v = str(row[0]).strip() if row else ''
            if v: print(f"    [{i}] '{v}'")

        def sebest_vals(keywords, cmap=col_map_grn):
            for kw in keywords:
                row = get_row(fin_rows, kw)
                if row is not None:
                    print(f"  sebest_vals matched '{kw}': {[row[cmap[mk]] if cmap.get(mk) and cmap[mk]<len(row) else None for mk in MONTH_ORDER]}")
                    return extract_row_by_month(row, cmap)
            print(f"  WARNING: none of {keywords} found")
            return [None] * MONTH_COUNT

        income   = sebest_vals(['ДОХОД'])
        expenses = sebest_vals(['Разом (всі витрати)', 'Разом (всі', 'Разом'])
        profit   = sebest_vals(['Операційний прибуток', 'Операційний'])
        data["income"]   = [round(v) if v else None for v in income]
        data["expenses"] = [round(v) if v else None for v in expenses]
        data["profit"]   = [round(v) if v else None for v in profit]
        print(f"  Income:   {data['income']}")
        print(f"  Expenses: {data['expenses']}")

        # ── Собівартість з P&L (аркуш 'P&L 2026', рядки з '═ ЦІНА PETG/PLA ВИРОБЛЕНО') ──
        cpkg_petg = [None] * MONTH_COUNT
        cpkg_pla  = [None] * MONTH_COUNT
        PNL_SHEET_ID = os.environ.get('PNL_SHEET_ID', '1kIwx30hqxuT7HDq0fq7shxyxvwv2zO3V2aP_ey0NtFw')
        try:
            pnl_rows = fetch_csv(PNL_SHEET_ID, 'P&L 2026')
            print(f"  P&L 2026: {len(pnl_rows)} rows")
            pnl_col_map, _ = detect_month_columns(pnl_rows)
            print(f"  P&L col_map: {pnl_col_map}")
            found_petg = found_pla = False
            # З ПДВ секція починається після рядка '📘 З ПДВ' (~R126)
            # Беремо ДРУГУ появу 'ціна petg вироблено' (перша — без ПДВ, друга — з ПДВ)
            in_vat_section = False
            for i, row in enumerate(pnl_rows):
                if not row: continue
                label = str(row[1] if len(row) > 1 else row[0]).strip()
                label_low = label.lower()
                # Детектуємо початок секції З ПДВ
                if 'з пдв' in label_low or 'з пдв' in ' '.join(str(c) for c in row).lower():
                    in_vat_section = True
                    print(f"  VAT section starts at row [{i}]")
                    continue
                if not in_vat_section:
                    continue
                # В секції З ПДВ шукаємо рядки собівартості
                if not found_petg and 'ціна petg вироблено' in label_low:
                    cpkg_petg = extract_row_by_month(row, pnl_col_map)
                    print(f"  PETG cost (з ПДВ) row [{i}]: {label} → {cpkg_petg}")
                    found_petg = True
                elif not found_pla and 'ціна pla вироблено' in label_low:
                    cpkg_pla = extract_row_by_month(row, pnl_col_map)
                    print(f"  PLA cost (з ПДВ) row [{i}]: {label} → {cpkg_pla}")
                    found_pla = True
                if found_petg and found_pla:
                    break
            if not found_petg:
                print(f"  WARNING: ЦІНА PETG ВИРОБЛЕНО not found in P&L — trying _AllData_Sebest fallback")
                for i, row in enumerate(fin_rows):
                    joined = ' '.join(str(c) for c in row).lower()
                    if 'себест' in joined and '1 кг' in joined:
                        if i + 1 < len(fin_rows):
                            cpkg_petg = extract_row_by_month(fin_rows[i+1], col_map_grn)
                        if i + 2 < len(fin_rows):
                            cpkg_pla = extract_row_by_month(fin_rows[i+2], col_map_grn)
                        break
        except Exception as epnl:
            print(f"  WARNING P&L cost fetch: {epnl}")
        print(f"  Cost PETG: {cpkg_petg}")
        print(f"  Cost PLA:  {cpkg_pla}")
        data["cost_petg_kg"] = [round(v, 2) if v else None for v in cpkg_petg]
        data["cost_pla_kg"]  = [round(v, 2) if v else None for v in cpkg_pla]

        # ── Календар управлінця (Огляд, низ) — тепер напряму з P&L (без ПДВ-переносу,
        #    без хардкоду). Раніше цей блок був статичним плейсхолдером, вручну
        #    вписаним у template.html ("НЕЗАБАРОМ підключається CF") — тепер бере
        #    Дохід/Залишки прямо з рядків P&L, Витрати = Дохід − EBITDA (тобто
        #    Собівартість + Операційні витрати разом, до податків). ──
        try:
            row_income = get_row(pnl_rows, 'доходи')
            row_ebitda = get_row(pnl_rows, 'ebitda (операційний прибуток)')
            row_bal_s  = get_row(pnl_rows, 'залишок на початок місяця')
            row_bal_e  = get_row(pnl_rows, 'залишок на кінець місяця')
            arr_income = extract_row_by_month(row_income, pnl_col_map) if row_income else [None] * MONTH_COUNT
            arr_ebitda = extract_row_by_month(row_ebitda, pnl_col_map) if row_ebitda else [None] * MONTH_COUNT
            arr_bal_s  = extract_row_by_month(row_bal_s,  pnl_col_map) if row_bal_s  else [None] * MONTH_COUNT
            arr_bal_e  = extract_row_by_month(row_bal_e,  pnl_col_map) if row_bal_e  else [None] * MONTH_COUNT

            UA_CAL = {'01':'Січ','02':'Лют','03':'Бер','04':'Кві','05':'Тра','06':'Чер',
                      '07':'Лип','08':'Сер','09':'Вер','10':'Жов','11':'Лис','12':'Гру'}
            cal_idx = [i for i, ym in enumerate(MONTH_ORDER) if arr_income[i] is not None]
            data['cal_labels']    = [f"{UA_CAL[MONTH_ORDER[i][5:7]]} {MONTH_ORDER[i][2:4]}" for i in cal_idx]
            data['cal_bal_start'] = [round(arr_bal_s[i]) if arr_bal_s[i] is not None else None for i in cal_idx]
            data['cal_bal_end']   = [round(arr_bal_e[i]) if arr_bal_e[i] is not None else None for i in cal_idx]
            data['cal_income']    = [round(arr_income[i]) if arr_income[i] is not None else None for i in cal_idx]
            data['cal_out']       = [round(arr_income[i] - arr_ebitda[i]) if arr_ebitda[i] is not None else None for i in cal_idx]
            print(f"  Календар управлінця (з P&L): {data['cal_labels']}")
            print(f"    income={data['cal_income']}")
            print(f"    out={data['cal_out']}")
            print(f"    bal_start={data['cal_bal_start']}")
            print(f"    bal_end={data['cal_bal_end']}")
        except Exception as ecal:
            print(f"  WARNING Календар управлінця: {ecal}")
            data['cal_labels'] = data['cal_bal_start'] = data['cal_bal_end'] = data['cal_income'] = data['cal_out'] = []
    except Exception as e:
        print(f"WARNING _AllData_Sebest: {e}")
        import traceback; traceback.print_exc()

    # ── 3. Калькулятор ─────────────────────────────────────────
    calc     = {"petg_price": 146.4, "pla_price": 175.5, "waste_pct": 5.0}
    calc_ext = {"granule": 112.2}
    try:
        calc = parse_calculator(fetch_csv(CALC_SHEET_ID, "Калькулятор"))
    except Exception as e:
        print(f"WARNING calc: {e}")
    try:
        calc_ext = parse_calc_extended(fetch_csv(CALC_SHEET_ID, "Расширенный"))
    except Exception as e:
        print(f"WARNING calc ext: {e}")

    # ── 4. Продажі (Журнал Відвантажень — з 07.2026, замість _AllData_$) ──────
    # Журнал вже об'єднує ТОВ(Стрім)+ФОП(Роздріб)+Easy(Изи) з датою відвантаження,
    # без затримки бухобліку (_AllData_$ давав занижений/0 дохід за останні місяці —
    # той самий баг, що ламав П&Л). Формат результату ідентичний parse_sales().
    sales = None
    try:
        sales = parse_sales_from_journal(fetch_csv(SHIP_SHEET_ID, "Відвантаження"))
        if sales and 'donut_by_month' in sales:
            for i, ym in enumerate(MONTH_ORDER):
                if data['income'][i] is None and ym in sales['donut_by_month']:
                    opt, ret = sales['donut_by_month'][ym]
                    data['income'][i] = round(opt + ret)
            print(f"  Income after sales backfill: {data['income']}")
    except Exception as e:
        print(f"WARNING sales: {e}")

    # ── 4b. Роздрібні продажі (окремий Google Sheet) ──────────
    # ВИМКНЕНО з 07.2026: Журнал Відвантажень (крок 4 вище) вже містить роздріб
    # як джерело SRC1 — повторний мерж звідси задвоював би роздрібні кг/виручку.
    # Лишаю код на випадок відкату до _AllData_$ (parse_sales); просто не заходимо
    # в блок, якщо sales вже прийшли з журналу.
    _RETAIL_MERGE_ENABLED = False
    try:
        retail_rows = fetch_csv(RETAIL_SHEET_ID, "2026") if _RETAIL_MERGE_ENABLED else None
        retail = parse_retail(retail_rows) if retail_rows else None
        if retail and sales:
            from collections import defaultdict as _ddr
            # Мержимо роздрібні кг у sku_sales_ret (додаємо до існуючих wholesale retail)
            for sku, arr in retail['sku_ret_kg'].items():
                if sku not in sales['sku_sales_ret']:
                    sales['sku_sales_ret'][sku] = [None] * MONTH_COUNT
                    if sku not in sales['sku_list']:
                        sales['sku_list'].append(sku)
                for i, v in enumerate(arr):
                    if v:
                        old_v = sales['sku_sales_ret'][sku][i] or 0
                        sales['sku_sales_ret'][sku][i] = round(old_v + v, 1)
            # Додаємо роздрібну виручку до income якщо ще не враховано
            for ym, grn in retail['ret_grn_by_month'].items():
                try:
                    mi = MONTH_ORDER.index(ym)
                    # income вже включає опт — додаємо розд тільки якщо _AllData_$ не мав Розница
                    # (перевіряємо чи monthly_ret було 0 — але немає прямого доступу тут)
                    # Безпечніше: не дублюємо, просто логуємо
                except ValueError:
                    pass
            # Також оновлюємо sales_ret_kg (тонни по MONTH_ORDER) для FC_FACT
            cur_ret_kg = list(sales.get('sales_ret_kg', [None]*MONTH_COUNT))
            # Вирівнюємо до MONTH_COUNT
            while len(cur_ret_kg) < MONTH_COUNT:
                cur_ret_kg.append(None)
            for sku, arr in retail['sku_ret_kg'].items():
                for i, v in enumerate(arr):
                    if v and i < MONTH_COUNT:
                        cur_ret_kg[i] = round((cur_ret_kg[i] or 0) + v / 1000, 3)
            sales['sales_ret_kg'] = cur_ret_kg
            print(f"  Retail merged: {sorted(retail['sku_ret_kg'].keys())}")
            print(f"  sales_ret_kg updated: {[round(v,1) if v else None for v in cur_ret_kg]}")
        elif retail and not sales:
            # Якщо продажів з _AllData_$ взагалі немає — створюємо sales зі структури retail
            sales = {
                'sku_sales_opt': {},
                'sku_sales_ret': {sk: arr for sk, arr in retail['sku_ret_kg'].items()},
                'sku_list': list(retail['sku_ret_kg'].keys()),
                'sales_opt': [None]*MONTH_COUNT, 'sales_ret': [None]*MONTH_COUNT,
                'donut_by_month': {},
            }
            print(f"  Retail-only sales created")
    except Exception as e:
        print(f"  WARNING retail: {e}")
        import traceback; traceback.print_exc()

    # ── 5. OKR (стратегія xlsx) ────────────────────────────────
    okr = None
    try:
        fetch_xlsx(STRATEGY_SHEET_ID, STRATEGY_FILE)
        import importlib, os as _os
        _fsize = _os.path.getsize(STRATEGY_FILE)
        print(f"  Strategy file: {_fsize:,} bytes")
        if _fsize < 10000:
            raise ValueError(f"Strategy file too small ({_fsize} bytes)")
        import okr_tracker
        importlib.reload(okr_tracker)
        okr_result = okr_tracker.run(STRATEGY_FILE)
        okr = okr_tracker.to_dashboard_json(okr_result)
        print(f"  OKR: company={okr_result['company_pct']*100:.1f}%, okrs={len(okr_result['okr_results'])}, people={len(okr_result['person_contribs'])}")
    except Exception as e:
        print(f"WARNING okr: {e}")
        import traceback; traceback.print_exc()

    # ── 6. Lines heatmap + норми ───────────────────────────────
    hm_labels, hm_data = [], {}
    try:
        hm_labels, hm_data = parse_lines_heatmap_from_alldata(prod_rows)
        print(f"  Lines HM result: {len(hm_labels)} months, {len(hm_data)} lines")
    except Exception as e:
        print(f"WARNING lines heatmap: {e}")
    try:
        norms_rows = fetch_csv(LINES_SHEET_ID, 'НОРМЫ')
        line_norms = parse_norms(norms_rows)
    except Exception as e:
        print(f"WARNING norms: {e}")
        line_norms = {}

    config = load_config()
    data_errors = {}

    # index.html — TOP (всі табки)
    html_top = generate(data, calc, calc_ext, sales, okr, hm_labels, hm_data, line_norms,
                        mode="top", config=config, data_errors=data_errors)
    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html_top)
    print(f"OK index.html ({len(html_top):,} bytes)")

    # middle.html — MID (тільки табки з mid:true)
    html_mid = generate(data, calc, calc_ext, sales, okr, hm_labels, hm_data, line_norms,
                        mode="mid", config=config, data_errors=data_errors)
    with open('middle.html', 'w', encoding='utf-8') as f:
        f.write(html_mid)
    print(f"OK middle.html ({len(html_mid):,} bytes)")
    print(f"\n✅ Done — index: {len(html_top):,} / middle: {len(html_mid):,} chars, updated {data['updated']}")