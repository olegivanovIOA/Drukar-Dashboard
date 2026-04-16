"""
generate.py — читает Google Sheets → генерирует index.html
Поддерживает данные за любой месяц с Ноябрь 2025 по Декабрь 2026.
Таблицы должны быть публичными (Поділитися → Всі з посиланням → Переглядач).
"""
import os, requests, io, csv
from datetime import datetime

SHEET_ID      = os.environ.get("SHEET_ID",      "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")
SHEET_ID2     = os.environ.get("SHEET_ID2",     "1NJkxtyha_oSpeaB7Jzmf440-kOF2gHBB0xsaMfKPRsI")
LINES_SHEET_ID  = os.environ.get("LINES_SHEET_ID",  "1SewXdbiFVIUPCESo5XDrRzvvG5rut4vuQTDyBDg3qp4")
LINES_SHEET_ID2 = os.environ.get("LINES_SHEET_ID2", "1NJkxtyha_oSpeaB7Jzmf440-kOF2gHBB0xsaMfKPRsI")
CALC_SHEET_ID = os.environ.get("CALC_SHEET_ID", "1U8dZJ_2niv5eYp0VGHvUHThQP6Ts4WaxeR10SEKIBvM")
STRATEGY_SHEET_ID = os.environ.get("STRATEGY_SHEET_ID", "1ASrf0kKP_0uIBdLCB__hoYp6GPjW5bNyzauMIDcbSWk")
STRATEGY_FILE = "Друкар_стратегия_2026.xlsx"

# 14 месяцев: Ноябрь 2025 — Декабрь 2026
# Позиция в массиве (0-based): Nov25=0, Dec25=1, Jan26=2 ... Dec26=13
MONTH_COUNT = 14
MONTH_ORDER = [
    '2025-11','2025-12',
    '2026-01','2026-02','2026-03','2026-04','2026-05','2026-06',
    '2026-07','2026-08','2026-09','2026-10','2026-11','2026-12'
]

def fetch_csv(sheet_id, sheet_name):
    url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
           f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}")
    print(f"Fetching: {sheet_name}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return list(csv.reader(io.StringIO(r.text)))

def fetch_xlsx(sheet_id, dest_path):
    """Скачивает Google Sheets как .xlsx файл."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    print(f"Fetching xlsx: {sheet_id}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(dest_path, 'wb') as f:
        f.write(r.content)
    print(f"  Saved {len(r.content):,} bytes → {dest_path}")

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
    month_keywords = {
        'ноябрь': '2025-11', 'листопад': '2025-11',
        'декабрь': '2025-12', 'грудень': '2025-12',
        'январь': '2026-01', 'січень': '2026-01',
        'февраль': '2026-02', 'лютий': '2026-02',
        'март':   '2026-03', 'березень': '2026-03',
        'апрель': '2026-04', 'квітень': '2026-04',
        'май':    '2026-05', 'травень': '2026-05',
        'июнь':   '2026-06', 'червень': '2026-06',
        'июль':   '2026-07', 'липень': '2026-07',
        'август': '2026-08', 'серпень': '2026-08',
        'сентябрь':'2026-09','вересень':'2026-09',
        'октябрь':'2026-10', 'жовтень': '2026-10',
        'ноябрь 2026':'2026-11','листопад 2026':'2026-11',
        'декабрь 2026':'2026-12','грудень 2026':'2026-12',
    }
    mapping = {}
    for row in rows[:5]:  # headers in first 5 rows
        for ci, cell in enumerate(row):
            cell_l = str(cell).lower().strip()
            for kw, month_key in month_keywords.items():
                if kw in cell_l:
                    if month_key not in mapping:
                        mapping[month_key] = ci
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
        'petg_nf': 0.0, 'pla_nf': 0.0,
        'petg_waste': 0.0, 'pla_waste': 0.0,
    })

    # Щоб не дублювати: зберігаємо унікальні (дата, зміна, лінія) вже оброблені
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

        is_petg = 'PETG' in vid
        is_pla  = 'PLA' in vid and 'PETG' not in vid

        if is_petg:
            monthly[ym]['petg']       += weight
            monthly[ym]['petg_nf']    += nf
            monthly[ym]['petg_waste'] += waste
        elif is_pla:
            monthly[ym]['pla']        += weight
            monthly[ym]['pla_nf']     += nf
            monthly[ym]['pla_waste']  += waste

    # Формуємо масиви по MONTH_ORDER
    petg_prod  = [round(monthly[m]['petg'],1)       if m in monthly else None for m in MONTH_ORDER]
    pla_prod   = [round(monthly[m]['pla'],1)        if m in monthly else None for m in MONTH_ORDER]
    petg_nf_kg = [round(monthly[m]['petg_nf'],1)    if m in monthly else None for m in MONTH_ORDER]
    pla_nf_kg  = [round(monthly[m]['pla_nf'],1)     if m in monthly else None for m in MONTH_ORDER]
    petg_w_kg  = [round(monthly[m]['petg_waste'],1) if m in monthly else None for m in MONTH_ORDER]
    pla_w_kg   = [round(monthly[m]['pla_waste'],1)  if m in monthly else None for m in MONTH_ORDER]

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
        "total_prod":   total_prod,
        "nf_pct":       nf_pct, "waste_pct": waste_pct,
        "petg_nf":      petg_nf_r,
        "pla_nf":       pla_nf_r,
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


def _empty_production():
    return {
        "updated": datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC'),
        "petg_prod": [None]*MONTH_COUNT, "pla_prod": [None]*MONTH_COUNT,
        "total_prod": [None]*MONTH_COUNT, "nf_pct": [None]*MONTH_COUNT,
        "waste_pct": [None]*MONTH_COUNT, "petg_nf": [None]*MONTH_COUNT,
        "pla_nf": [None]*MONTH_COUNT, "petg_waste": [None]*MONTH_COUNT,
        "pla_waste": [None]*MONTH_COUNT, "income": [None]*MONTH_COUNT,
        "expenses": [None]*MONTH_COUNT, "profit": [None]*MONTH_COUNT,
        "cost_petg_kg": [None]*MONTH_COUNT, "cost_pla_kg": [None]*MONTH_COUNT,
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
    for i, row in enumerate(rows):
        if 'себестоимость 1 кг' in ' '.join(str(c) for c in row).lower():
            if i+1 < len(rows):
                cpkg_petg = extract_row_by_month(rows[i+1], col_map)
            if i+2 < len(rows):
                cpkg_pla  = extract_row_by_month(rows[i+2], col_map)
            break

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
        for row in rows[data_start:]:
            if not row: continue
            # Forward-fill date (needed for Журнал where date only on first row of shift block)
            d = row[ci_date] if ci_date < len(row) else None
            if d is not None and hasattr(d, 'strftime'):
                current_date = d
            elif d is not None and isinstance(d, (int, float)) and 40000 < d < 60000:
                from datetime import date as _date, timedelta
                current_date = _date(1899, 12, 30) + timedelta(days=int(d))
            elif d is not None and isinstance(d, str) and len(d) >= 7:
                try:
                    from datetime import datetime as _dt
                    current_date = _dt.strptime(d[:10], '%Y-%m-%d')
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

            # For "Аналіз вкладів": w = вага×вклад%, need w/contrib to get full line weight
            # For "Журнал": w = full line weight already, no division needed
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



def parse_sales(rows):
    """
    Парсит продажи из листа _AllData_$ — выторг по каналам по месяцам,
    топ продуктов и средние цены PETG/PLA.
    """
    from collections import defaultdict
    import re

    # Пропускаем строки-заголовки (первые 2)
    data_rows = []
    for row in rows[1:]:
        if len(row) < 10: continue
        date_raw = str(row[0]).strip()
        if not date_raw or date_raw in ('Дата', 'NaN', 'nan', ''): continue
        # Дата
        try:
            from datetime import datetime as dt
            # разные форматы
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y'):
                try: d = dt.strptime(date_raw[:10], fmt[:len(fmt)]); break
                except: pass
            else: continue
        except: continue
        channel = str(row[10]).strip() if len(row) > 10 else ''
        if channel not in ('Опт', 'Розница'): continue
        product = str(row[1]).strip()
        plastic  = str(row[9]).strip()
        try: revenue = float(str(row[5]).replace(',','.').replace(' ','').replace('\xa0','')) if row[5] else 0
        except: revenue = 0
        try: kg = float(str(row[4]).replace(',','.').replace(' ','').replace('\xa0','')) if row[4] else 0
        except: kg = 0
        if revenue <= 0: continue
        ym = d.strftime('%Y-%m')
        data_rows.append({'ym':ym, 'channel':channel, 'product':product, 'plastic':plastic, 'revenue':revenue, 'kg':kg})

    if not data_rows:
        print("  WARNING: no sales data parsed")
        return {}

    # Sort months
    months_sorted = sorted(set(r['ym'] for r in data_rows))

    # Monthly by channel
    monthly_opt = defaultdict(float)
    monthly_ret = defaultdict(float)
    for r in data_rows:
        if r['channel'] == 'Опт':     monthly_opt[r['ym']] += r['revenue']
        elif r['channel'] == 'Розница': monthly_ret[r['ym']] += r['revenue']

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

    result = {
        'sales_labels':      labels,
        'sales_months':      months_sorted,
        'sales_opt':         sales_opt,
        'sales_ret':         sales_ret,
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





def generate(data, calc, calc_ext, sales=None, okr=None, hm_labels=None, hm_data=None, line_norms=None):
    if line_norms is None: line_norms = {}
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
        '{{PETG_WASTE}}':      jv(data['petg_waste']),
        '{{PLA_WASTE}}':       jv(data['pla_waste']),
        '{{INCOME}}':          jv(data['income']),
        '{{EXPENSES}}':        jv(data['expenses']),
        '{{PROFIT}}':          jv(data['profit']),
        '{{COST_PETG_KG}}':    jv(data['cost_petg_kg']),
        '{{COST_PLA_KG}}':     jv(data['cost_pla_kg']),
        '{{CALC_PETG_PRICE}}': str(calc['petg_price']),
        '{{CALC_PLA_PRICE}}':  str(calc['pla_price']),
        '{{CALC_WASTE_PCT}}':  str(calc['waste_pct']),
        '{{CALC_EX_GRANULE}}': str(calc_ext['granule']),
        '{{HM_LABELS}}':       jv(hm_labels or []),
        '{{HM_NORMS}}':        jv(line_norms or {}),
        '{{HM_DATA}}':         jv(hm_data or {}),
    }
    if sales:
        subs.update({
            '{{SALES_LABELS}}':       jv(sales['sales_labels']),
            '{{SALES_OPT}}':          jv(sales['sales_opt']),
            '{{SALES_RET}}':          jv(sales['sales_ret']),
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
        })
    else:
        subs.update({
            '{{SALES_LABELS}}':       '[]',
            '{{SALES_OPT}}':          '[]',
            '{{SALES_RET}}':          '[]',
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
        })
    # OKR placeholders — завжди замінюємо, навіть якщо okr=None (щоб не було JS syntax error)
    subs.update({
        '{{OKR_COMPANY_PCT}}':   str(round(okr['company_pct'] * 100, 1)) if okr else '0',
        '{{OKR_DATA}}':          okr['okr_data_json']  if okr else '[]',
        '{{OKR_PEOPLE}}':        okr['people_json']    if okr else '[]',
        '{{OKR_KR_DATA}}':       okr['kr_data_json']   if okr else '[]',
    })



    for k, v in subs.items():
        html = html.replace(k, v)
    missing = [k for k in subs if k in html]
    if missing: print(f"WARNING unreplaced: {missing}")
    return html

if __name__ == '__main__':
    try:
        prod_rows = fetch_csv(SHEET_ID, "_AllData_Product")
        data = parse_production_from_alldata(prod_rows)
        # Фінансові дані — окремо з _Drukar_Product
        try:
            fin_rows = fetch_csv(SHEET_ID, "_Drukar_Product")
            fin = parse_production(fin_rows)
            data["income"]       = fin["income"]
            data["expenses"]     = fin["expenses"]
            data["profit"]       = fin["profit"]
            data["cost_petg_kg"] = fin["cost_petg_kg"]
            data["cost_pla_kg"]  = fin["cost_pla_kg"]
        except Exception as e:
            print(f"  WARNING fin data: {e}")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback; traceback.print_exc()
        exit(1)

    calc = {"petg_price": 146.4, "pla_price": 175.5, "waste_pct": 5.0}
    calc_ext = {"granule": 112.2}
    sales = None
    okr   = None
    try:
        calc = parse_calculator(fetch_csv(CALC_SHEET_ID, "Калькулятор"))
    except Exception as e:
        print(f"WARNING calc: {e}")
    try:
        calc_ext = parse_calc_extended(fetch_csv(CALC_SHEET_ID, "Расширенный"))
    except Exception as e:
        print(f"WARNING calc ext: {e}")
    try:
        sales = parse_sales(fetch_csv(SHEET_ID, "_AllData_$"))
    except Exception as e:
        print(f"WARNING sales: {e}")
    try:
        fetch_xlsx(STRATEGY_SHEET_ID, STRATEGY_FILE)
        import importlib, os as _os
        # Перевіряємо що файл скачався нормально
        _fsize = _os.path.getsize(STRATEGY_FILE)
        print(f"  Strategy file: {_fsize:,} bytes")
        if _fsize < 10000:
            raise ValueError(f"Strategy file too small ({_fsize} bytes) — download failed?")
        import okr_tracker
        importlib.reload(okr_tracker)
        okr_result = okr_tracker.run(STRATEGY_FILE)
        okr = okr_tracker.to_dashboard_json(okr_result)
        print(f"  OKR: company={okr_result['company_pct']*100:.1f}%, okrs={len(okr_result['okr_results'])}, people={len(okr_result['person_contribs'])}")
    except Exception as e:
        print(f"WARNING okr: {e}")
        import traceback; traceback.print_exc()

    # ── Lines heatmap ──
    hm_labels, hm_data = [], {}
    try:
        def fetch_lines_sheet(sheet_id, candidates):
            """Try multiple sheet names, return first that has date+line data."""
            for name in candidates:
                try:
                    rows = fetch_csv(sheet_id, name)
                    if not rows or len(rows) < 3:
                        print(f"  Lines HM: '{name}' — too few rows ({len(rows)}), skipping")
                        continue
                    # Quick check: does any of first 3 rows look like a journal header?
                    preview = ' '.join(str(c) for row in rows[:3] for c in row if c).lower()
                    if 'лін' in preview or 'лини' in preview or 'line' in preview:
                        print(f"  Lines HM: '{name}' — OK ({len(rows)} rows)")
                        return rows
                    else:
                        print(f"  Lines HM: '{name}' — no line column found, skipping")
                except Exception as e:
                    print(f"  Lines HM: '{name}' failed: {e}")
            return []

        SHEET1_CANDIDATES = ["Журнал.Локация1", "Аналіз вкладів", "_AllData"]
        SHEET2_CANDIDATES = ["Журнал.Локация2", "Аналіз вкладів", "_AllData"]

        rows1 = fetch_lines_sheet(LINES_SHEET_ID,  SHEET1_CANDIDATES)
        rows2 = fetch_lines_sheet(LINES_SHEET_ID2, SHEET2_CANDIDATES)
        hm_labels, hm_data = parse_lines_heatmap([rows1, rows2])
        print(f"  Lines HM result: {len(hm_labels)} months, {len(hm_data)} lines")
        # Читаємо норми з листа НОРМЫ
        try:
            norms_rows = fetch_csv(LINES_SHEET_ID, 'НОРМЫ')
            line_norms = parse_norms(norms_rows)
        except Exception as e_n:
            print(f"  WARNING norms: {e_n}")
            line_norms = {}
    except Exception as e:
        import traceback
        print(f"WARNING lines heatmap: {e}")
        traceback.print_exc()

    html = generate(data, calc, calc_ext, sales, okr, hm_labels, hm_data, line_norms)
    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n✅ Done — {len(html):,} chars, updated {data['updated']}")