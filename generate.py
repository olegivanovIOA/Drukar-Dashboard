"""
generate.py — читает Google Sheets → генерирует index.html
Поддерживает данные за любой месяц с Ноябрь 2025 по Декабрь 2026.
Таблицы должны быть публичными (Поділитися → Всі з посиланням → Переглядач).
"""
import os, requests, io, csv
from datetime import datetime

SHEET_ID      = os.environ.get("SHEET_ID",      "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")
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

    result = {
        'sales_labels':      labels,
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
    }
    print(f"  Sales: {len(months_sorted)} months, opt={round(total_opt/1e6,1)}M, ret={round(total_ret/1e6,1)}M")
    return result


def jv(v):
    if v is None: return 'null'
    if isinstance(v, list): return '['+','.join(jv(x) for x in v)+']'
    if isinstance(v, str): return '"'+v.replace('"', '\\"')+'"'
    return str(v)


def parse_lines_operators(rows):
    """
    Парсит _AllData_Product (журнал всіх ліній) →
    повертає дані для:
      - HM_WEEKS / HM_DATA (теплова карта)
      - lns (бар-чарт завантаженості ліній)
      - OP_ALL / OP_BY_MONTH (оператори)
    """
    from collections import defaultdict
    from datetime import datetime, timedelta

    def exval(cell):
        import re
        s = str(cell)
        if 'COMPUTED_VALUE' in s or 'DUMMYFUNCTION' in s:
            m = re.search(r'\),(.+)\)$', s)
            if m:
                return m.group(1).strip().strip('"')
        return cell

    line_week_kg  = defaultdict(lambda: defaultdict(float))
    line_total_kg = defaultdict(float)
    op_month_kg   = defaultdict(lambda: defaultdict(float))
    op_total_kg   = defaultdict(float)

    header_row = 0
    for i, row in enumerate(rows[:5]):
        joined = ' '.join(str(c).lower() for c in row)
        if 'лін' in joined or 'лин' in joined or 'дата' in joined:
            header_row = i
            break

    for row in rows[header_row + 1:]:
        vals = [exval(c) for c in row]
        if not vals[0]:
            continue
        try:
            date_serial = float(str(vals[0]))
            d = datetime.fromordinal(datetime(1899, 12, 30).toordinal() + int(date_serial))
        except:
            try:
                # Try dd.mm.yyyy format
                d = datetime.strptime(str(vals[0])[:10], '%d.%m.%Y')
            except:
                continue

        line = str(vals[4]).strip() if len(vals) > 4 and vals[4] else ''
        if not line.upper().startswith('ЛІН') and not line.upper().startswith('ЛИН'):
            continue

        line = line.upper().replace('ЛИНИЯ', 'ЛІНІЯ')
        operator = str(vals[2]).strip() if len(vals) > 2 and vals[2] else ''

        try:
            weight_raw = float(str(vals[7])) if len(vals) > 7 and vals[7] else 0
            contrib = float(str(vals[3])) if len(vals) > 3 and vals[3] else 0.5
            if contrib > 1.5: contrib = contrib / 100
            if contrib <= 0: contrib = 0.5
            weight_real = round(weight_raw / contrib, 2)
        except:
            weight_real = 0

        if weight_real <= 0:
            continue

        day_of_week = d.weekday()
        week_start = d - timedelta(days=day_of_week)
        week_key = week_start.strftime('%d.%m')
        ym = d.strftime('%Y-%m')

        line_week_kg[line][week_key] += weight_real
        line_total_kg[line]          += weight_real
        op_month_kg[ym][operator]    += weight_real
        op_total_kg[operator]        += weight_real

    # Sort weeks chronologically
    def week_sort_key(w):
        dd, mm = w.split('.')
        yy = '2025' if int(mm) >= 10 else '2026'
        return datetime.strptime(f'{dd}.{mm}.{yy}', '%d.%m.%Y')

    all_weeks = sorted(
        set(wk for lw in line_week_kg.values() for wk in lw.keys()),
        key=week_sort_key
    )

    lines_order = ['ЛІНІЯ 1','ЛІНІЯ 2','ЛІНІЯ 3','ЛІНІЯ 4',
                   'ЛІНІЯ 5','ЛІНІЯ 6','ЛІНІЯ 7','ЛІНІЯ 8']
    hm_data = {}
    for line in lines_order:
        if line in line_week_kg:
            hm_data[line] = [round(line_week_kg[line].get(wk, 0)) for wk in all_weeks]

    lns = sorted(
        [{'n': line, 'kg': round(line_total_kg[line], 1)} for line in line_total_kg],
        key=lambda x: -x['kg']
    )

    op_all = sorted(
        [[op, round(kg, 1)] for op, kg in op_total_kg.items()],
        key=lambda x: -x[1]
    )
    op_by_month = {
        ym: sorted([[op, round(kg, 1)] for op, kg in ops.items()], key=lambda x: -x[1])
        for ym, ops in sorted(op_month_kg.items())
    }

    print(f"  Lines: {sorted(line_total_kg.keys())}")
    print(f"  Weeks: {len(all_weeks)}, ops: {len(op_total_kg)}")

    return {
        'hm_weeks': all_weeks,
        'hm_data':  hm_data,
        'lns':      lns,
        'op_all':   op_all,
        'op_by_month': op_by_month,
    }

def generate(data, calc, calc_ext, sales=None, okr=None, lines_ops_data=None):
    with open('template.html', 'r', encoding='utf-8') as f:
        html = f.read()
    subs = {
        '{{UPDATED}}':         data['updated'],
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
            '{{SALES_TOTAL_OPT}}':    str(sales['total_opt']),
            '{{SALES_TOTAL_RET}}':    str(sales['total_ret']),
        })
    if okr:
        subs.update({
            '{{OKR_COMPANY_PCT}}':   str(round(okr['company_pct'] * 100, 1)),
            '{{OKR_DATA}}':          okr['okr_data_json'],
            '{{OKR_PEOPLE}}':        okr['people_json'],
            '{{OKR_KR_DATA}}':       okr['kr_data_json'],
        })

    # ── Lines + Operators (from _AllData_Product)
    lines_ops = lines_ops_data or {}
    if lines_ops:
        def js_hm(hm):
            inner = ','.join(f"'{k}':{jv(v)}" for k, v in hm.items())
            return '{' + inner + '}'
        subs.update({
            '{{HM_WEEKS}}':    jv(lines_ops['hm_weeks']),
            '{{HM_DATA}}':     js_hm(lines_ops['hm_data']),
            '{{LNS_DATA}}':    jv(lines_ops['lns']),
            '{{OP_ALL}}':      jv(lines_ops['op_all']),
            '{{OP_BY_MONTH}}': js_hm(lines_ops['op_by_month']),
        })

    for k, v in subs.items():
        html = html.replace(k, v)
    missing = [k for k in subs if k in html]
    if missing: print(f"WARNING unreplaced: {missing}")
    return html

if __name__ == '__main__':
    try:
        prod_rows = fetch_csv(SHEET_ID, "_Drukar_Product")
        data = parse_production(prod_rows)
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
        import okr_tracker
        okr_result = okr_tracker.run(STRATEGY_FILE)
        okr = okr_tracker.to_dashboard_json(okr_result)
    except Exception as e:
        print(f"WARNING okr: {e}")
        import traceback; traceback.print_exc()

    lines_ops = None
    try:
        alldata_rows = fetch_csv(SHEET_ID, "_AllData_Product")
        lines_ops = parse_lines_operators(alldata_rows)
    except Exception as e:
        print(f"WARNING _AllData_Product: {e}")

    html = generate(data, calc, calc_ext, sales, okr, lines_ops)
    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n✅ Done — {len(html):,} chars, updated {data['updated']}")