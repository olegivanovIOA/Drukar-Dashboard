"""
generate.py — читает данные из Google Sheets и генерирует index.html
Таблица должна быть публичной (доступ "Переглядач" для всех з посиланням)
"""
import os, json, requests, io, csv
from datetime import datetime

SHEET_ID = os.environ.get("SHEET_ID", "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")

def fetch_csv(sheet_name):
    url = (
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}"
    )
    print(f"Fetching sheet: {sheet_name}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    reader = csv.reader(io.StringIO(r.text))
    return list(reader)

def f(v, default=None):
    try:
        s = str(v).strip().replace(',', '.').replace(' ', '').replace('\xa0', '').replace('%','')
        return float(s) if s else default
    except:
        return default

def row_vals(row, start=2, count=5):
    if row is None:
        return [None] * count
    result = []
    data_cells = [c for c in row[start:] if str(c).strip() not in ('', 'None')]
    for i in range(count):
        result.append(f(data_cells[i]) if i < len(data_cells) else None)
    return result

def get_row(rows, keyword):
    kw = keyword.lower().strip()
    for row in rows:
        for cell in row:
            if kw in str(cell).lower():
                return row
    return None

def pct(v):
    if v is None: return None
    return round(v * 100, 2) if abs(v) <= 1.5 else round(v, 2)

def parse():
    rows = fetch_csv("_Drukar_Product")

    def vals(keyword):
        return row_vals(get_row(rows, keyword))

    petg_prod   = vals('PETG, Продукции, кг')
    pla_prod    = vals('PLA, Продукции, кг')
    petg_nf_raw = vals('PETG, НФ, %')
    pla_nf_raw  = vals('PLA, НФ, %')
    petg_w_raw  = vals('PETG, Брак, %')
    pla_w_raw   = vals('PLA, Брак, %')
    petg_nf_kg  = vals('PETG, НФ, кг')
    pla_nf_kg   = vals('PLA, НФ, кг')
    petg_w_kg   = vals('PETG, Брак, кг')
    pla_w_kg    = vals('PLA, Брак, кг')
    expenses    = vals('Разом (всі витрати)')
    income      = vals('ДОХОД, грн')
    profit      = vals('Операційний')

    cpkg_petg = [None]*5
    cpkg_pla  = [None]*5
    for i, row in enumerate(rows):
        if 'себестоимость 1 кг' in ' '.join(str(c) for c in row).lower():
            if i+1 < len(rows): cpkg_petg = row_vals(rows[i+1])
            if i+2 < len(rows): cpkg_pla  = row_vals(rows[i+2])
            break

    nf_pct = []
    waste_pct = []
    for i in range(5):
        pp = petg_prod[i] or 0; lp = pla_prod[i] or 0
        total = pp + lp
        nf_sum = (petg_nf_kg[i] or 0) + (pla_nf_kg[i] or 0)
        w_sum  = (petg_w_kg[i] or 0)  + (pla_w_kg[i] or 0)
        nf_pct.append(round(nf_sum/total*100, 2) if total else None)
        waste_pct.append(round(w_sum/total*100, 2) if total else None)

    total_prod = [
        round((petg_prod[i] or 0) + (pla_prod[i] or 0), 1)
        if petg_prod[i] is not None or pla_prod[i] is not None else None
        for i in range(5)
    ]

    data = {
        "updated":      datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC'),
        "petg_prod":    [round(v,1) if v else None for v in petg_prod],
        "pla_prod":     [round(v,1) if v else None for v in pla_prod],
        "total_prod":   total_prod,
        "nf_pct":       nf_pct,
        "waste_pct":    waste_pct,
        "petg_nf":      [pct(v) for v in petg_nf_raw],
        "pla_nf":       [pct(v) for v in pla_nf_raw],
        "petg_waste":   [pct(v) for v in petg_w_raw],
        "pla_waste":    [pct(v) for v in pla_w_raw],
        "income":       [round(v) if v else None for v in income],
        "expenses":     [round(v) if v else None for v in expenses],
        "profit":       [round(v) if v else None for v in profit],
        "cost_petg_kg": [round(v,2) if v else None for v in cpkg_petg],
        "cost_pla_kg":  [round(v,2) if v else None for v in cpkg_pla],
    }

    print("\n=== Parsed data ===")
    for k, v in data.items():
        if k != 'updated':
            print(f"  {k}: {v}")
    return data

def jv(v):
    if v is None: return 'null'
    if isinstance(v, list): return '[' + ','.join(jv(x) for x in v) + ']'
    return str(v)

def generate(data):
    with open('template.html', 'r', encoding='utf-8') as f:
        html = f.read()
    subs = {
        '{{UPDATED}}':      data['updated'],
        '{{PETG_PROD}}':    jv(data['petg_prod']),
        '{{PLA_PROD}}':     jv(data['pla_prod']),
        '{{TOTAL_PROD}}':   jv(data['total_prod']),
        '{{NF_PCT}}':       jv(data['nf_pct']),
        '{{PETG_NF}}':      jv(data['petg_nf']),
        '{{PLA_NF}}':       jv(data['pla_nf']),
        '{{PETG_WASTE}}':   jv(data['petg_waste']),
        '{{PLA_WASTE}}':    jv(data['pla_waste']),
        '{{INCOME}}':       jv(data['income']),
        '{{EXPENSES}}':     jv(data['expenses']),
        '{{PROFIT}}':       jv(data['profit']),
        '{{COST_PETG_KG}}': jv(data['cost_petg_kg']),
        '{{COST_PLA_KG}}':  jv(data['cost_pla_kg']),
    }
    for k, v in subs.items():
        html = html.replace(k, v)
    missing = [k for k in subs if k in html]
    if missing:
        print(f"WARNING: unreplaced placeholders: {missing}")
    return html

if __name__ == '__main__':
    try:
        data = parse()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback; traceback.print_exc()
        exit(1)
    html = generate(data)
    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nindex.html generated ({len(html):,} chars), updated: {data['updated']}")
