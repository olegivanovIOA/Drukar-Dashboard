"""
okr_tracker.py — Трекинг прогресса OKR 2026, Друкар
======================================================
Читает: Друкар_стратегия_2026.xlsx
Выводит:
  1. Общий % прогресса компании
  2. Прогресс по каждому OKR (вес + %)
  3. Прогресс по каждому KR (внутри OKR)
  4. Рейтинг вклада каждого человека

Прогресс берётся из колонки 'Progress' листа OKR_2026.
Значение 0–1 (или 0–100%, нормализуется автоматически).
"""

import pandas as pd
import numpy as np
import sys
import os

# ─────────────────────────────────────────────
FILE = os.path.join(os.path.dirname(__file__), 'Друкар_стратегия_2026.xlsx')
if not os.path.exists(FILE):
    # fallback for direct run
    FILE = '/mnt/user-data/uploads/Друкар_стратегия_2026.xlsx'
# ─────────────────────────────────────────────

LEAD_COEFF    = 0.7   # вес лида за задачу
# Вес саппорта берётся из листа Весакоэфф, но если не найден — fallback
DEFAULT_SUPPORT_COEFF = 0.3


def to_progress(v):
    """Парсит значение прогресса → float 0.0–1.0 или None."""
    try:
        f = float(v)
        if f > 1.0:
            f = f / 100.0   # если введено как %
        return round(min(max(f, 0.0), 1.0), 4)
    except:
        return None


DONE_KEYWORDS = {'выполнено', 'done', 'завершено', 'готово', 'completed', '✓', '✅'}

def is_done_status(status_val):
    """Возвращает True если статус означает 'выполнено'."""
    s = str(status_val).strip().lower()
    return any(kw in s for kw in DONE_KEYWORDS)


def parse_okr_weights(raw_df):
    """Читает веса OKR из блока строк 191–198 листа OKR_2026."""
    weights = {}
    for idx in range(191, 199):
        try:
            name = str(raw_df.iloc[idx, 0]).strip()
            val  = raw_df.iloc[idx, 2]
            if name != 'nan' and pd.notna(val):
                weights[name] = int(val)
        except:
            pass
    return weights


    """Читает веса OKR из блока строк 191–198 листа OKR_2026."""
    weights = {}
    for idx in range(191, 199):
        try:
            name = str(raw_df.iloc[idx, 0]).strip()
            val  = raw_df.iloc[idx, 2]
            if name != 'nan' and pd.notna(val):
                weights[name] = int(val)
        except:
            pass
    return weights


def parse_support_coeffs(xl):
    """
    Читает лист Весакоэфф и строит dict:
      (okr_prefix, kr_prefix, task_prefix) → {'lead': 0.7, 'support': 0.3, 'n_support': 1}
    Используется для точных коэффициентов.
    Если строка не найдена — возвращается DEFAULT_SUPPORT_COEFF.
    """
    try:
        df = xl.parse('Весакоэфф', header=0)
        # Колонки: OKR(Цель), РЕЗУЛЬТАТ, ЗАДАЧА, Удельный вес задачи,
        #          Вес Лида, Доля 1 Саппорта, Всего помогаторов
        coeffs = []
        cur_okr = None
        cur_kr  = None
        for _, row in df.iterrows():
            okr  = str(row.iloc[0]).strip()
            kr   = str(row.iloc[1]).strip()
            task = str(row.iloc[2]).strip()
            lead = to_progress(row.iloc[4])
            supp = to_progress(row.iloc[5])
            n    = row.iloc[6]
            try: n = int(n)
            except: n = 1
            if okr.startswith('ОКР') and okr != 'nan':
                cur_okr = okr
            if kr.startswith('КР') and kr != 'nan':
                cur_kr = kr
            if lead is not None and supp is not None:
                coeffs.append({
                    'okr': cur_okr, 'kr': cur_kr,
                    'task': task if task != 'nan' else None,
                    'lead': lead, 'support': supp, 'n_support': n
                })
        return coeffs
    except Exception as e:
        print(f"  ⚠ Весакоэфф: {e}")
        return []


def get_support_coeff(coeffs, okr, kr, task):
    """Ищет коэффициент саппорта для конкретной задачи/КР."""
    for c in reversed(coeffs):
        task_ok = (task and c['task'] and task[:30] in (c['task'] or '')) or \
                  c['task'] is None or c['task'] == 'nan'
        kr_ok   = (kr and c['kr'] and str(kr)[:20] in str(c['kr'] or '')) or \
                  c['kr'] is None
        okr_ok  = (okr and c['okr'] and str(okr)[:20] in str(c['okr'] or ''))
        if okr_ok and kr_ok and task_ok:
            return c['support']
    return DEFAULT_SUPPORT_COEFF


def parse_main_sheet(xl):
    """
    Читает OKR_2026 и строит иерархию:
    [
      {
        'okr': 'ОКР1. ...',
        'kr':  'КР1.1. ...',
        'task': 'описание задачи',
        'type': 'OKR'|'KR'|'TASK',
        'responsible': '...',
        'helpers': ['...', '...'],
        'progress': 0.0–1.0 or None,
      }, ...
    ]
    """
    raw  = xl.parse('OKR_2026', header=None)
    okr_weights = parse_okr_weights(raw)

    df = xl.parse('OKR_2026', header=0).iloc[:185].copy()

    def row_type(row):
        okr  = str(row['ОКР']).strip()
        kr   = str(row['КР']).strip()
        task = str(row['Проект / Задача']).strip()
        if okr.startswith('ОКР') and okr != 'nan':  return 'OKR'
        if kr.startswith('КР')  and kr  != 'nan':
            # Если задача = "Выполнено" — это маркер прогресса самого KR, не отдельная задача
            if is_done_status(task): return 'KR'
            return 'KR'
        if task != 'nan' and task and not is_done_status(task): return 'TASK'
        return None

    df['_type'] = df.apply(row_type, axis=1)

    # Forward-fill current OKR / KR context
    df['_okr'] = df['ОКР'].where(df['ОКР'].notna()).ffill()
    df['_kr']  = df['КР'].where(df['КР'].notna()).ffill()

    rows = []
    for _, r in df[df['_type'].notna()].iterrows():
        helpers_raw = str(r.get('Кто помогает', '')).strip()
        helpers = [h.strip() for h in helpers_raw.split(',') if h.strip() and h.strip() != 'nan']

        task_name = str(r.get('Проект / Задача', '')).strip()
        if task_name == 'nan': task_name = None

        # Прогресс: проверяем "Выполнено" в статусе, задаче или колонке помогающих
        status_raw   = str(r.get('Статус/комментарий', '') or '')
        task_raw     = str(r.get('Проект / Задача',    '') or '')
        helpers_raw2 = str(r.get('Кто помогает',       '') or '').strip()
        if is_done_status(status_raw) or is_done_status(task_raw) or is_done_status(helpers_raw2):
            progress = 1.0
        else:
            progress = to_progress(r.get('Progress'))

        rows.append({
            'type':        r['_type'],
            'okr':         str(r['_okr']).strip(),
            'kr':          str(r['_kr']).strip() if pd.notna(r['_kr']) else None,
            'task':        task_name,
            'responsible': str(r.get('Ответственный', '')).strip() or None,
            'helpers':     helpers,
            'progress':    progress,
            'status':      str(status_raw).strip() if pd.notna(status_raw) else None,
        })
    return rows, okr_weights


def parse_person_sheets(xl):
    """
    Читает OKR_<Имя> листы → dict person → list of {okr, kr, task, role}
    role: 'Ответственный' | 'Помогаю'
    """
    person_map = {}
    for sheet in xl.sheet_names:
        if not sheet.startswith('OKR_') or sheet == 'OKR_2026':
            continue
        person = sheet.replace('OKR_', '')
        df = xl.parse(sheet, header=0)
        entries = []
        cur_okr = None
        cur_kr  = None
        for _, row in df.iterrows():
            okr  = str(row.iloc[0]).strip()
            kr   = str(row.iloc[1]).strip()
            role = str(row.iloc[2]).strip()
            task = str(row.iloc[3]).strip() if len(row) > 3 else ''
            if okr.startswith('ОКР') and okr != 'nan': cur_okr = okr
            if kr.startswith('КР')  and kr  != 'nan':  cur_kr  = kr
            if role in ('Ответственный', 'Помогаю') and cur_okr:
                entries.append({
                    'okr':  cur_okr,
                    'kr':   cur_kr,
                    'task': task if task not in ('nan', '') else None,
                    'role': role,
                })
        if entries:
            person_map[person] = entries
    return person_map


# ══════════════════════════════════════════════
# CALCULATION ENGINE
# ══════════════════════════════════════════════

def safe_float(v, default=0.0):
    """float без NaN/None."""
    try:
        f = float(v)
        return default if f != f else f   # f!=f только для NaN
    except:
        return default


def calc_kr_progress(rows, okr_name, kr_name):
    """
    Прогресс KR = среднее по задачам (незаполненные = 0).
    Нет задач → прогресс самой KR-строки (незаполненная = 0).
    """
    task_rows = [r for r in rows
                 if r['type'] == 'TASK' and r['okr'] == okr_name and r['kr'] == kr_name]
    if task_rows:
        progs = [safe_float(r['progress']) for r in task_rows]
        return round(sum(progs) / len(progs), 4)

    kr_rows = [r for r in rows
               if r['type'] == 'KR' and r['okr'] == okr_name and r['kr'] == kr_name]
    return round(safe_float(kr_rows[0]['progress']) if kr_rows else 0.0, 4)


def calc_okr_progress(rows, okr_name):
    """Прогресс OKR = среднее по KR (незаполненные KR = 0)."""
    krs = list({r['kr'] for r in rows
                if r['okr'] == okr_name and r['kr'] and r['type'] in ('KR', 'TASK')})
    if not krs:
        okr_rows = [r for r in rows if r['type'] == 'OKR' and r['okr'] == okr_name]
        return round(safe_float(okr_rows[0]['progress']) if okr_rows else 0.0, 4)
    progs = [calc_kr_progress(rows, okr_name, kr) for kr in krs]
    return round(sum(progs) / len(progs), 4)


def calc_company_progress(rows, okr_weights):
    """Итоговый % = Σ (прогресс_OKR × вес / total_weight)."""
    total_w = sum(okr_weights.values())
    return round(sum(
        calc_okr_progress(rows, n) * (w / total_w)
        for n, w in okr_weights.items()
    ), 4)


def calc_person_contributions(rows, okr_weights, person_map, support_coeffs):
    """Вклад каждого человека: прогресс × коэф × вес_OKR/total."""
    total_w = sum(okr_weights.values())

    # Кешируем прогресс KR
    kr_cache = {}
    for r in rows:
        if r['type'] == 'KR':
            key = (r['okr'], r['kr'])
            if key not in kr_cache:
                kr_cache[key] = calc_kr_progress(rows, r['okr'], r['kr'])

    # Кешируем прогресс задач
    task_cache = {}
    for r in rows:
        if r['type'] == 'TASK':
            task_cache[(r['okr'], r['kr'], r['task'])] = safe_float(r['progress'])

    def get_okr_w(okr_name):
        w = okr_weights.get(okr_name, 0)
        if not w:
            for k, v in okr_weights.items():
                if okr_name[:15] in k:
                    return v
        return w

    result = {}
    for person, entries in person_map.items():
        score = max_possible = 0.0
        for e in entries:
            okr, kr, task, role = e['okr'], e['kr'], e['task'], e['role']
            w_norm = get_okr_w(okr) / total_w if total_w else 0
            coeff  = LEAD_COEFF if role == 'Ответственный' else \
                     get_support_coeff(support_coeffs, okr, kr, task)
            prog   = (task_cache.get((okr, kr, task)) if task else None) \
                     or kr_cache.get((okr, kr), 0.0)
            score        += safe_float(prog) * coeff * w_norm
            max_possible += coeff * w_norm

        result[person] = {
            'score':    round(score, 6),
            'max':      round(max_possible, 6),
            'realized': round(score / max_possible, 4) if max_possible > 0 else 0.0,
        }
    return result




# ══════════════════════════════════════════════
# MAIN REPORT
# ══════════════════════════════════════════════

def run(filepath=None):
    f = filepath or FILE
    xl = pd.ExcelFile(f)

    rows, okr_weights = parse_main_sheet(xl)
    person_map        = parse_person_sheets(xl)
    support_coeffs    = parse_support_coeffs(xl)

    total_w = sum(okr_weights.values())

    # ── Company progress ──
    company_pct = calc_company_progress(rows, okr_weights)
    if company_pct is None or (isinstance(company_pct, float) and np.isnan(company_pct)):
        company_pct = 0.0

    print("=" * 65)
    print(f"  ПРОГРЕСС КОМПАНИИ К ЦЕЛЯМ 2026")
    print(f"  {'█' * int(company_pct * 40)}{'░' * (40 - int(company_pct * 40))}  {company_pct*100:.1f}%")
    print("=" * 65)

    # ── OKR breakdown ──
    print("\n📌 ПРОГРЕСС ПО OKR:\n")
    okr_results = {}
    for okr_name, weight in okr_weights.items():
        p = calc_okr_progress(rows, okr_name)
        p = 0.0 if (p is None or np.isnan(p)) else p
        okr_results[okr_name] = p
        bar = '█' * int(p * 20) + '░' * (20 - int(p * 20))
        short = okr_name[:40]
        print(f"  {short:<42} [{bar}] {p*100:5.1f}%  (вес {weight}/{total_w} = {weight/total_w*100:.0f}%)")

    # ── KR breakdown ──
    print("\n📋 ПРОГРЕСС ПО KR:\n")
    krs_seen = {}
    for r in rows:
        if r['type'] == 'KR' and r['kr']:
            key = (r['okr'], r['kr'])
            if key not in krs_seen:
                krs_seen[key] = True
                p = calc_kr_progress(rows, r['okr'], r['kr'])
                p = 0.0 if (p is None or np.isnan(p)) else p
                okr_short = r['okr'][:25]
                kr_short  = r['kr'][:45]
                bar = '█' * int(p * 10) + '░' * (10 - int(p * 10))
                print(f"  {okr_short:<27} | {kr_short:<47} [{bar}] {p*100:5.1f}%")

    # ── Person contributions ──
    person_contribs = calc_person_contributions(rows, okr_weights, person_map, support_coeffs)

    print("\n👤 РЕЙТИНГ ВКЛАДА ЛЮДЕЙ:\n")
    print(f"  {'Имя':<22} {'Реализ. потенциала':>20}  {'Абс. вклад':>12}  {'Макс. потенциал':>16}")
    print("  " + "─" * 74)

    sorted_people = sorted(person_contribs.items(), key=lambda x: -x[1]['realized'] if not np.isnan(x[1]['realized']) else 0)
    for person, data in sorted_people:
        realized = data['realized'] if not np.isnan(data['realized']) else 0.0
        bar = '█' * int(realized * 15) + '░' * (15 - int(realized * 15))
        print(f"  {person:<22} [{bar}] {realized*100:5.1f}%  "
              f"  {data['score']:.4f}     /  {data['max']:.4f}")

    print("\n" + "─" * 65)
    print("  ℹ  Реализ. потенциала = сколько % от своих задач выполнено")
    print("  ℹ  Абс. вклад = вклад в общий % прогресса компании")
    print("  ℹ  Данные берутся из колонки 'Progress' листа OKR_2026")
    print("─" * 65 + "\n")

    return {
        'company_pct': company_pct,
        'okr_results': okr_results,
        'person_contribs': person_contribs,
        'okr_weights': okr_weights,
        'rows': rows,
    }


def to_dashboard_json(result):
    """
    Преобразует результат run() в JSON-строки для подстановки в template.html.
    Возвращает dict с ключами okr_data_json, people_json, kr_data_json, company_pct.
    """
    import json

    okr_weights    = result['okr_weights']
    okr_results    = result['okr_results']
    person_contribs = result['person_contribs']
    rows           = result['rows']
    total_w        = sum(okr_weights.values())

    # OKR list: [{name, short, pct, weight, weight_pct}]
    okr_data = []
    for name, weight in okr_weights.items():
        pct = round((okr_results.get(name, 0.0)) * 100, 1)
        # short name: убираем "ОКР1. " префикс
        short = name.split('. ', 1)[1] if '. ' in name else name
        okr_data.append({
            'name':       name,
            'short':      short,
            'pct':        pct,
            'weight':     weight,
            'weight_pct': round(weight / total_w * 100, 1),
        })

    # KR list: [{okr_name, kr_name, pct}]
    kr_data = []
    seen = set()
    for r in rows:
        if r['type'] == 'KR' and r['kr']:
            key = (r['okr'], r['kr'])
            if key not in seen:
                seen.add(key)
                p = calc_kr_progress(rows, r['okr'], r['kr'])
                # Find OKR short name
                okr_short = r['okr'].split('. ', 1)[1] if '. ' in r['okr'] else r['okr']
                kr_data.append({
                    'okr':  r['okr'],
                    'okr_short': okr_short,
                    'kr':   r['kr'],
                    'pct':  round(p * 100, 1),
                })

    # People list: [{name, realized_pct, score, max}]
    people = sorted(
        [{'name': p, 'realized_pct': round(d['realized'] * 100, 1),
          'score': round(d['score'], 4), 'max': round(d['max'], 4)}
         for p, d in person_contribs.items()],
        key=lambda x: -x['realized_pct']
    )

    return {
        'company_pct':   result['company_pct'],
        'okr_data_json': json.dumps(okr_data,  ensure_ascii=False),
        'kr_data_json':  json.dumps(kr_data,   ensure_ascii=False),
        'people_json':   json.dumps(people,    ensure_ascii=False),
    }


if __name__ == '__main__':
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    run(filepath)
