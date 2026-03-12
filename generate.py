"""
generate.py — читает данные из Google Sheets и генерирует index.html
Требует: SHEET_ID (основная таблица) и CALC_SHEET_ID (калькулятор) в GitHub Secrets
Таблицы должны быть публичными (доступ "Переглядач" для всех з посиланням)
"""
import os, requests, io, csv
from datetime import datetime

SHEET_ID      = os.environ.get("SHEET_ID",      "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")
CALC_SHEET_ID = os.environ.get("CALC_SHEET_ID", "1U8dZJ_2niv5eYp0VGHvUHThQP6Ts4WaxeR10SEKIBvM")

def fetch_csv(sheet_id, sheet_name):
    url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
           f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}")
    print(f"Fetching: {sheet_name} from {sheet_id[:20]}...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return list(csv.reader(io.StringIO(r.text)))

def f(v, default=None):
    try:
        s = str(v).strip().replace(',','.').replace(' ','').replace('\xa0','').replace('%','')
        return float(s) if s else default
    except:
        return default

def row_vals(row, start=2, count=5):
    if row is None: return [None]*count
    cells = [c for c in row[start:] if str(c).strip() not in ('','None')]
    return [f(cells[i]) if i < len(cells) else None for i in range(count)]

def get_row(rows, keyword):
    kw = keyword.lower()
    for row in rows:
        if any(kw in str(c).lower() for c in row):
            return row
    return None

def pct(v):
    if v is None: return None
    return round(v*100, 2) if abs(v) <= 1.5 else round(v, 2)

def parse_production(rows):
    def vals(kw): return row_vals(get_row(rows, kw))
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

    cpkg_petg = [None]*5; cpkg_pla = [None]*5
    for i, row in enumerate(rows):
        if 'себестоимость 1 кг' in ' '.join(str(c) for c in row).lower():
            if i+1 < len(rows): cpkg_petg = row_vals(rows[i+1])
            if i+2 < len(rows): cpkg_pla  = row_vals(rows[i+2])
            break

    nf_pct=[]; waste_pct=[]
    for i in range(5):
        pp=(petg_prod[i] or 0)+(pla_prod[i] or 0)
        nf_pct.append(round(((petg_nf_kg[i] or 0)+(pla_nf_kg[i] or 0))/pp*100,2) if pp else None)
        waste_pct.append(round(((petg_w_kg[i] or 0)+(pla_w_kg[i] or 0))/pp*100,2) if pp else None)

    total_prod = [round((petg_prod[i] or 0)+(pla_prod[i] or 0),1)
                  if petg_prod[i] is not None or pla_prod[i] is not None else None for i in range(5)]
    return {
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

def parse_calculator(rows):
    """Читает лист Калькулятор: строки 1-3 = параметры, cols 1,3,5 = база,вар1,вар2"""
    result = {"petg_price": 146.4, "pla_price": 175.5, "waste_pct": 5.0, "granule": 112.2}
    try:
        # Row 1: PETG price (col 1 = база)
        r1 = get_row(rows, 'цена сырья PETG')
        if r1:
            v = f(r1[1] if len(r1)>1 else None)
            if v: result["petg_price"] = v
        # Row 2: PLA price
        r2 = get_row(rows, 'цена сырья PLA')
        if r2:
            v = f(r2[1] if len(r2)>1 else None)
            if v: result["pla_price"] = v
        # Row 3: waste %
        r3 = get_row(rows, 'Брака и НФ')
        if r3:
            v = f(r3[1] if len(r3)>1 else None)
            if v: result["waste_pct"] = round(v*100 if v<1 else v, 1)
        print(f"  Calculator: PETG={result['petg_price']}, PLA={result['pla_price']}, waste={result['waste_pct']}%")
    except Exception as e:
        print(f"  Calculator parse error: {e}")
    return result

def parse_calc_extended(rows):
    """Читает лист Расширенный: цена гранулы"""
    result = {"granule": 112.2}
    try:
        r = get_row(rows, 'гранул')
        if r:
            v = f(r[1] if len(r)>1 else None)
            if v: result["granule"] = v
            print(f"  Extended calc: granule={result['granule']}")
    except Exception as e:
        print(f"  Extended calc parse error: {e}")
    return result

def jv(v):
    if v is None: return 'null'
    if isinstance(v, list): return '['+','.join(jv(x) for x in v)+']'
    return str(v)

def generate(data, calc, calc_ext):
    with open('template.html','r',encoding='utf-8') as f:
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
    for k,v in subs.items():
        html = html.replace(k, v)
    missing = [k for k in subs if k in html]
    if missing: print(f"WARNING unreplaced: {missing}")
    return html

if __name__ == '__main__':
    # Parse production data
    try:
        prod_rows = fetch_csv(SHEET_ID, "_Drukar_Product")
        prod_data = parse_production(prod_rows)
        prod_data['updated'] = datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC')
        print("\nProduction data OK")
        for k,v in prod_data.items():
            if k != 'updated': print(f"  {k}: {v}")
    except Exception as e:
        print(f"ERROR reading production data: {e}")
        import traceback; traceback.print_exc()
        exit(1)

    # Parse calculator (non-fatal)
    calc = {"petg_price": 146.4, "pla_price": 175.5, "waste_pct": 5.0}
    calc_ext = {"granule": 112.2}
    try:
        calc_rows = fetch_csv(CALC_SHEET_ID, "Калькулятор")
        calc = parse_calculator(calc_rows)
    except Exception as e:
        print(f"WARNING: Could not read calculator sheet: {e}")
    try:
        ext_rows = fetch_csv(CALC_SHEET_ID, "Расширенный")
        calc_ext = parse_calc_extended(ext_rows)
    except Exception as e:
        print(f"WARNING: Could not read extended calculator sheet: {e}")

    html = generate(prod_data, calc, calc_ext)
    with open('index.html','w',encoding='utf-8') as f:
        f.write(html)
    print(f"\n✅ index.html generated ({len(html):,} chars)")
    print(f"   Updated: {prod_data['updated']}")
