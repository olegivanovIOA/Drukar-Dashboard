"""
generate.py — читает данные из Google Sheets и генерирует index.html
Требует: SHEET_ID в переменных окружения (GitHub Secrets)
Таблица должна быть публичной (доступ "Просматривать" для всех с ссылкой)
"""
import os, json, requests, re
from datetime import datetime

SHEET_ID = os.environ.get("SHEET_ID", "1thXW13Min0-5qWpNUvi0Y5ZWNl1LxYsZyLA78zf0khA")

def fetch_sheet(sheet_name):
    """Читает лист через публичный CSV экспорт Google Sheets"""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    import csv, io
    reader = csv.reader(io.StringIO(r.text))
    return list(reader)

def safe_float(v, default=None):
    try:
        return float(str(v).replace(',', '.').replace(' ', '').replace('\xa0', ''))
    except:
        return default

def parse_data():
    """Парсит данные из Google Sheets"""
    print("Fetching data from Google Sheets...")
    
    try:
        rows = fetch_sheet("_Drukar_Product")
    except Exception as e:
        print(f"Error fetching sheet: {e}")
        return None

    # Ищем строки с данными по месяцам
    # Структура: row[0]=категория, row[1]=подкатегория, row[2..]=значения по месяцам
    months = []
    petg_prod = []
    pla_prod = []
    petg_nf = []
    pla_nf = []
    petg_waste = []
    pla_waste = []
    total_costs = []
    income = []
    profit = []
    cost_petg_kg = []
    cost_pla_kg = []

    header_row = None
    for i, row in enumerate(rows):
        # Ищем строку с месяцами (заголовок)
        row_str = ' '.join(str(c) for c in row)
        if 'ноябрь' in row_str.lower() or 'листопад' in row_str.lower() or '2025' in row_str:
            header_row = i
            # Извлекаем названия месяцев из заголовка
            for cell in row:
                cell = str(cell).strip()
                if cell and ('2025' in cell or '2026' in cell or 'ноябрь' in cell.lower() or 'декабрь' in cell.lower()):
                    months.append(cell)
            break

    if not months:
        print("Could not find month headers, using defaults")
        months = ['Листопад 2025', 'Грудень 2025', 'Січень 2026', 'Лютий 2026', 'Березень 2026']

    def get_row_values(rows, keyword):
        """Находит строку по ключевому слову и возвращает числовые значения"""
        for row in rows:
            row_flat = ' '.join(str(c) for c in row).lower()
            if keyword.lower() in row_flat:
                vals = []
                for cell in row[2:]:  # пропускаем первые 2 столбца с метками
                    vals.append(safe_float(cell))
                return vals[:5]  # берём до 5 месяцев
        return [None] * 5

    # Извлекаем данные
    petg_prod = get_row_values(rows, 'PETG, Продукции, кг')
    pla_prod = get_row_values(rows, 'PLA, Продукции, кг')
    petg_nf_pct = get_row_values(rows, 'PETG, НФ, %')
    pla_nf_pct = get_row_values(rows, 'PLA, НФ, %')
    petg_waste_pct = get_row_values(rows, 'PETG, Брак, %')
    pla_waste_pct = get_row_values(rows, 'PLA, Брак, %')

    # Пробуем получить финансовые данные
    try:
        rows_seb = fetch_sheet("_AllData_Sebest")
        total_costs_row = get_row_values(rows_seb, 'Разом (всі витрати)')
        income_row = get_row_values(rows_seb, 'ДОХОД')
        profit_row = get_row_values(rows_seb, 'Операційний Прибуток')
        cost_petg_row = get_row_values(rows_seb, 'PETG')
        cost_pla_row = get_row_values(rows_seb, 'PLA')
    except Exception as e:
        print(f"Could not fetch sebest sheet: {e}")
        total_costs_row = [None]*5
        income_row = [None]*5
        profit_row = [None]*5
        cost_petg_row = [None]*5
        cost_pla_row = [None]*5

    def pct_to_display(v):
        """Конвертирует долю (0.05) в проценты (5.0) если нужно"""
        if v is None: return None
        if abs(v) < 1: return round(v * 100, 2)
        return round(v, 2)

    data = {
        "months_short": months,
        "updated": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "petg_prod": petg_prod,
        "pla_prod": pla_prod,
        "total_prod": [
            (p + l) if p is not None and l is not None else None
            for p, l in zip(petg_prod, pla_prod)
        ],
        "nf_pct": [pct_to_display(v) for v in [
            # Общий НФ как среднее PETG+PLA (примерно)
            (safe_float(petg_nf_pct[i]) if i < len(petg_nf_pct) else None)
            for i in range(5)
        ]],
        "petg_nf": [pct_to_display(v) for v in petg_nf_pct],
        "pla_nf": [pct_to_display(v) for v in pla_nf_pct],
        "petg_waste": [pct_to_display(v) for v in petg_waste_pct],
        "pla_waste": [pct_to_display(v) for v in pla_waste_pct],
        "income": income_row,
        "expenses": total_costs_row,
        "profit": profit_row,
        "cost_petg_kg": cost_petg_row,
        "cost_pla_kg": cost_pla_row,
    }

    print(f"Data parsed: {len(months)} months")
    print(f"PETG prod: {petg_prod}")
    print(f"PLA prod: {pla_prod}")
    return data


def js_val(v):
    """Конвертирует Python значение в JS"""
    if v is None: return 'null'
    if isinstance(v, list):
        return '[' + ','.join(js_val(x) for x in v) + ']'
    if isinstance(v, float): return str(round(v, 2))
    if isinstance(v, int): return str(v)
    return json.dumps(v, ensure_ascii=False)


def generate_html(data):
    """Генерирует полный HTML дашборда с подставленными данными"""
    
    # Читаем шаблон
    with open('template.html', 'r', encoding='utf-8') as f:
        template = f.read()

    # Подставляем данные
    replacements = {
        '{{UPDATED}}': data.get('updated', ''),
        '{{PETG_PROD}}': js_val(data['petg_prod']),
        '{{PLA_PROD}}': js_val(data['pla_prod']),
        '{{TOTAL_PROD}}': js_val(data['total_prod']),
        '{{NF_PCT}}': js_val(data['nf_pct']),
        '{{PETG_NF}}': js_val(data['petg_nf']),
        '{{PLA_NF}}': js_val(data['pla_nf']),
        '{{PETG_WASTE}}': js_val(data['petg_waste']),
        '{{PLA_WASTE}}': js_val(data['pla_waste']),
        '{{INCOME}}': js_val(data['income']),
        '{{EXPENSES}}': js_val(data['expenses']),
        '{{PROFIT}}': js_val(data['profit']),
        '{{COST_PETG_KG}}': js_val(data['cost_petg_kg']),
        '{{COST_PLA_KG}}': js_val(data['cost_pla_kg']),
    }

    html = template
    for k, v in replacements.items():
        html = html.replace(k, v)

    return html


if __name__ == '__main__':
    data = parse_data()
    if data is None:
        print("ERROR: Could not parse data. Keeping existing index.html")
        exit(0)
    
    html = generate_html(data)
    
    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"index.html generated successfully ({len(html)} chars)")
    print(f"Updated: {data['updated']}")
