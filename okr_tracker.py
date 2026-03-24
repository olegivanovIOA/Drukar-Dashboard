"""
okr_tracker.py — Трекинг прогресса OKR 2026, Друкар
======================================================
Читает: Друкар_стратегия_2026.xlsx
Выводит:
  1. Общий % прогресса компании
  2. Прогресс по каждому OKR (вес + %)
  3. Прогресс по каждому KR (внутри OKR)
  4. Рейтинг вклада каждого человека

Прогресс берётся из колонки 'Прогресс, %' листа OKR_2026.
Значение 0–1 (или 0–100%, нормализуется автоматически).
Значение "Выполнено" / "Done" в col D или H = 100%.
"""

import pandas as pd
import numpy as np
import sys
import os

FILE = os.path.join(os.path.dirname(__file__), 'Друкар_стратегия_2026.xlsx')
if not os.path.exists(FILE):
    FILE = '/mnt/user-data/uploads/Друкар_стратегия_2026.xlsx'

LEAD_COEFF            = 0.7
DEFAULT_SUPPORT_COEFF = 0.3


def to_progress(v):
    """Парсит значение прогресса → float 0.0–1.0 или None."""
    try:
        f = float(v)
        if f != f: return None   # nan
        if f > 1.0:
            f = f / 100.0
        return round(min(max(f, 0.0), 1.0), 4)
    except:
        return None


DONE_KEYWORDS = {'выполнено', 'done', 'завершено', 'готово', 'completed', '✓', '✅'}

def is_done_status(val):
    """True если значение означает 'выполнено' (100%)."""
    s = str(val).strip().lower()
    return any(kw in s for kw in DONE_KEYWORDS)


# ── БАГ 1 ФИКС: динамический поиск весов ОКР ────────────────────────────────
def parse_okr_weights(raw_df):
    """
    Читает веса OKR — ищет блок где col_A = название ОКР и col_C = число.
    Ранее хардкодило рядки 191–198 что сломалось после правок в таблице.
    """
    weights = {}
    for idx in range(len(raw_df)):
        try:
            name = str(raw_df.iloc[idx, 0]).strip()
            val  = raw_df.iloc[idx, 2]
            # Ищем строки где col_A = "ОКРn. ..." и col_C = целое число (вес)
            if (name.startswith('ОКР') and name != 'nan'
                    and pd.notna(val) and str(val) != 'nan'):
                try:
                    w = int(float(val))
                    if 1 <= w <= 100:   # разумный диапазон весов
                        weights[name] = w
                except:
                    pass
        except:
            pass
    # Если нашли несколько строк с одинаковым ОКР — берём последнюю (служебная зона)
    # Если совсем пусто — равные веса
    if not weights:
        print("  ⚠ Веса ОКР не найдены — используются равные веса")
    return weights


def parse_support_coeffs(xl):
    try:
        df = xl.parse('Весакоэфф', header=0)
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
    raw  = xl.parse('OKR_2026', header=None)
    okr_weights = parse_okr_weights(raw)

    # ── БАГ 2 ФИКС: берём все строки до служебной зоны (≤185) ──────────────
    df = xl.parse('OKR_2026', header=0).iloc[:185].copy()

    def row_type(row):
        okr  = str(row.get('ОКР', '') or '').strip()
        kr   = str(row.get('КР',  '') or '').strip()
        task = str(row.get('Проект / Задача', '') or '').strip()
        stat = str(row.get('Статус/комментарий', '') or '').strip()

        if okr.startswith('ОКР') and okr != 'nan': return 'OKR'
        if kr.startswith('КР')   and kr  != 'nan':
            # ── БАГ 2 ФИКС: "Выполнено" в col_D = KR помечен как выполненный ──
            if is_done_status(task) or is_done_status(stat):
                return 'KR_DONE'
            return 'KR'
        if task and task != 'nan':
            # ── БАГ 2 ФИКС: задача с "Выполнено" в col_D ──────────────────
            if is_done_status(task):
                return 'TASK_DONE'
            return 'TASK'
        return None

    df['_type'] = df.apply(row_type, axis=1)
    df['_okr']  = df['ОКР'].where(df['ОКР'].notna()).ffill()
    df['_kr']   = df['КР'].where(df['КР'].notna()).ffill()

    rows = []
    for _, r in df[df['_type'].notna()].iterrows():
        helpers_raw = str(r.get('Кто помогает', '') or '').strip()
        helpers = [h.strip() for h in helpers_raw.split(',')
                   if h.strip() and h.strip() != 'nan']

        task_name = str(r.get('Проект / Задача', '') or '').strip()
        if task_name in ('nan', ''): task_name = None

        raw_progress = (r.get('Прогресс, %')
                     or r.get('Прогресс')
                     or r.get('Progress'))
        status_raw = str(r.get('Статус/комментарий', '') or '')
        task_raw   = str(r.get('Проект / Задача',    '') or '')

        # ── БАГ 2 ФИКС: "Выполнено" в задаче ИЛИ статусе = 100% ──────────
        if r['_type'] in ('KR_DONE', 'TASK_DONE') or \
           is_done_status(status_raw) or is_done_status(task_raw):
            progress = 1.0
        else:
            progress = to_progress(raw_progress)

        # Нормализуем тип для единообразия
        rtype = r['_type']
        if rtype == 'KR_DONE':   rtype = 'KR'
        if rtype == 'TASK_DONE': rtype = 'TASK'

        rows.append({
            'type':        rtype,
            'okr':         str(r['_okr']).strip(),
            'kr':          str(r['_kr']).strip() if pd.notna(r['_kr']) else None,
            'task':        task_name,
            'responsible': str(r.get('Ответственный', '') or '').strip() or None,
            'helpers':     helpers,
            'progress':    progress,
            'status':      str(status_raw).strip() if status_raw != 'nan' else None,
        })
    return rows, okr_weights


def parse_person_sheets(xl):
    """
    Читает OKR_<Имя> листы.
    БАГ 3 ФИКС: forward-fill ОКР/КР + lookup для строк где col_A пустой.
    """
    # Строим lookup из главного листа
    kr_to_okr   = {}
    task_to_kr  = {}
    task_to_okr = {}
    try:
        main_raw = xl.parse('OKR_2026', header=None)
        cur_okr = None
        cur_kr  = None
        for _, row in main_raw.iterrows():
            okr_val  = str(row.iloc[0]).strip()
            kr_val   = str(row.iloc[1]).strip()
            task_val = str(row.iloc[3]).strip() if len(row) > 3 else ''
            if okr_val.startswith('ОКР') and okr_val != 'nan':
                cur_okr = okr_val
            if kr_val.startswith('КР') and kr_val != 'nan':
                cur_kr = kr_val
                if cur_okr: kr_to_okr[cur_kr] = cur_okr
            if task_val and task_val != 'nan' and cur_kr:
                task_to_kr[task_val]  = cur_kr
                task_to_okr[task_val] = cur_okr
    except Exception as e:
        print(f"  WARNING lookup build: {e}")

    def find_okr_kr(task):
        if not task or task == 'nan': return None, None
        if task in task_to_kr:
            return task_to_okr.get(task), task_to_kr.get(task)
        for k, v in task_to_kr.items():
            if task[:20] in k or k[:20] in task:
                return task_to_okr.get(k), v
        return None, None

    person_map = {}
    for sheet in xl.sheet_names:
        if not sheet.startswith('OKR_') or sheet == 'OKR_2026':
            continue
        person = sheet.replace('OKR_', '')
        try:
            df = xl.parse(sheet, header=0)
        except:
            continue

        entries = []
        cur_okr = None
        cur_kr  = None

        for _, row in df.iterrows():
            okr  = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
            kr   = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
            role = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
            task = str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else ''
            task = task if task != 'nan' else ''

            if okr.startswith('ОКР') and okr != 'nan':
                cur_okr = okr
            if kr.startswith('КР') and kr != 'nan':
                cur_kr = kr
                # ── БАГ 3 ФИКС: ищем ОКР по КР если cur_okr пустой ────────
                if not cur_okr or cur_okr == 'nan':
                    cur_okr = kr_to_okr.get(kr)

            eff_okr = cur_okr
            eff_kr  = cur_kr

            # ── БАГ 3 ФИКС: доп. поиск по задаче ──────────────────────────
            if (not eff_okr or not eff_kr) and task:
                lkp_okr, lkp_kr = find_okr_kr(task)
                eff_okr = eff_okr or lkp_okr
                eff_kr  = eff_kr  or lkp_kr

            # ── БАГ 3 ФИКС: включаем строку даже без ОКР если есть КР ─────
            if role in ('Ответственный', 'Помогаю') and (eff_okr or eff_kr):
                entries.append({
                    'okr':  eff_okr or '',
                    'kr':   eff_kr,
                    'task': task or None,
                    'role': role,
                })

        if entries:
            person_map[person] = entries
    return person_map


# ══════════════════════════════════════════════
# CALCULATION ENGINE (без изменений)
# ══════════════════════════════════════════════

def safe_float(v, default=0.0):
    try:
        f = float(v)
        return default if f != f else f
    except:
        return default


def calc_kr_progress(rows, okr_name, kr_name):
    task_rows = [r for r in rows
                 if r['type'] == 'TASK' and r['okr'] == okr_name and r['kr'] == kr_name]
    if task_rows:
        progs = [safe_float(r['progress']) for r in task_rows]
        return round(sum(progs) / len(progs), 4)
    kr_rows = [r for r in rows
               if r['type'] == 'KR' and r['okr'] == okr_name and r['kr'] == kr_name]
    return round(safe_float(kr_rows[0]['progress']) if kr_rows else 0.0, 4)


def calc_okr_progress(rows, okr_name):
    krs = list({r['kr'] for r in rows
                if r['okr'] == okr_name and r['kr'] and r['type'] in ('KR', 'TASK')})
    if not krs:
        okr_rows = [r for r in rows if r['type'] == 'OKR' and r['okr'] == okr_name]
        return round(safe_float(okr_rows[0]['progress']) if okr_rows else 0.0, 4)
    progs = [calc_kr_progress(rows, okr_name, kr) for kr in krs]
    return round(sum(progs) / len(progs), 4)


def calc_company_progress(rows, okr_weights):
    if not okr_weights:
        # Фолбек: равные веса если словарь пустой
        okrs = list({r['okr'] for r in rows if r['type'] == 'OKR'})
        if not okrs: return 0.0
        progs = [calc_okr_progress(rows, o) for o in okrs]
        return round(sum(progs) / len(progs), 4)
    total_w = sum(okr_weights.values())
    return round(sum(
        calc_okr_progress(rows, n) * (w / total_w)
        for n, w in okr_weights.items()
    ), 4)


def calc_person_contributions(rows, okr_weights, person_map, support_coeffs):
    total_w = sum(okr_weights.values()) if okr_weights else 1.0

    kr_cache   = {}
    kr_by_name = {}
    for r in rows:
        if r['type'] == 'KR':
            key = (r['okr'], r['kr'])
            if key not in kr_cache:
                p = calc_kr_progress(rows, r['okr'], r['kr'])
                kr_cache[key] = p
                if r['kr']: kr_by_name[r['kr']] = p

    task_cache   = {}
    task_by_name = {}
    for r in rows:
        if r['type'] == 'TASK' and r['task']:
            p = safe_float(r['progress'])
            task_cache[(r['okr'], r['kr'], r['task'])] = p
            task_by_name[r['task']] = p

    def get_okr_w(okr_name):
        w = okr_weights.get(okr_name, 0)
        if not w:
            for k, v in okr_weights.items():
                if okr_name[:15] in k: return v
        return w if w else (1.0 / len(okr_weights) if okr_weights else 0.0)

    def get_prog(okr, kr, task):
        if task:
            p = task_cache.get((okr, kr, task))
            if p is not None: return p
            p = task_by_name.get(task)
            if p is not None: return p
        p = kr_cache.get((okr, kr))
        if p is not None: return p
        if kr:
            p = kr_by_name.get(kr)
            if p is not None: return p
        return 0.0

    result = {}
    for person, entries in person_map.items():
        score = max_possible = 0.0
        for e in entries:
            okr, kr, task, role = e['okr'], e['kr'], e['task'], e['role']
            w_norm = get_okr_w(okr) / total_w if total_w else 0
            coeff  = LEAD_COEFF if role == 'Ответственный' else \
                     get_support_coeff(support_coeffs, okr, kr, task)
            if coeff is None or (isinstance(coeff, float) and coeff != coeff):  # nan check
                coeff = DEFAULT_SUPPORT_COEFF
            prog   = get_prog(okr, kr, task)
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

    rows, okr_weights   = parse_main_sheet(xl)
    person_map          = parse_person_sheets(xl)
    support_coeffs      = parse_support_coeffs(xl)

    total_w = sum(okr_weights.values()) if okr_weights else 1

    company_pct = calc_company_progress(rows, okr_weights)
    if company_pct is None or (isinstance(company_pct, float) and np.isnan(company_pct)):
        company_pct = 0.0

    print("=" * 65)
    print(f"  ПРОГРЕСС КОМПАНИИ К ЦЕЛЯМ 2026")
    print(f"  {'█' * int(company_pct * 40)}{'░' * (40 - int(company_pct * 40))}  {company_pct*100:.1f}%")
    print("=" * 65)

    print("\n📌 ПРОГРЕСС ПО OKR:\n")
    okr_results = {}
    for okr_name, weight in okr_weights.items():
        p = calc_okr_progress(rows, okr_name)
        p = 0.0 if (p is None or np.isnan(p)) else p
        okr_results[okr_name] = p
        bar = '█' * int(p * 20) + '░' * (20 - int(p * 20))
        print(f"  {okr_name[:42]:<44} [{bar}] {p*100:5.1f}%  (вес {weight}/{total_w}={weight/total_w*100:.0f}%)")

    print("\n📋 ПРОГРЕСС ПО KR:\n")
    krs_seen = {}
    for r in rows:
        if r['type'] == 'KR' and r['kr']:
            key = (r['okr'], r['kr'])
            if key not in krs_seen:
                krs_seen[key] = True
                p = calc_kr_progress(rows, r['okr'], r['kr'])
                p = 0.0 if (p is None or np.isnan(p)) else p
                bar = '█' * int(p * 10) + '░' * (10 - int(p * 10))
                print(f"  {r['okr'][:25]:<27} | {r['kr'][:45]:<47} [{bar}] {p*100:5.1f}%")

    person_contribs = calc_person_contributions(rows, okr_weights, person_map, support_coeffs)

    print("\n👤 РЕЙТИНГ ВКЛАДА ЛЮДЕЙ:\n")
    print(f"  {'Имя':<22} {'Реализ. потенциала':>20}  {'Абс. вклад':>12}  {'Макс. потенциал':>16}")
    print("  " + "─" * 74)
    sorted_people = sorted(person_contribs.items(),
                           key=lambda x: -x[1]['realized'] if not np.isnan(x[1]['realized']) else 0)
    for person, data in sorted_people:
        realized = data['realized'] if not np.isnan(data['realized']) else 0.0
        bar = '█' * int(realized * 15) + '░' * (15 - int(realized * 15))
        print(f"  {person:<22} [{bar}] {realized*100:5.1f}%  "
              f"  {data['score']:.4f}     /  {data['max']:.4f}")

    print("\n" + "─" * 65)
    print("  ℹ  Реализ. потенциала = сколько % от своих задач выполнено")
    print("  ℹ  Абс. вклад = вклад в общий % прогресса компании")
    print("─" * 65 + "\n")

    return {
        'company_pct':    company_pct,
        'okr_results':    okr_results,
        'person_contribs': person_contribs,
        'okr_weights':    okr_weights,
        'rows':           rows,
    }


def to_dashboard_json(result):
    import json

    okr_weights     = result['okr_weights']
    okr_results     = result['okr_results']
    person_contribs = result['person_contribs']
    rows            = result['rows']
    total_w         = sum(okr_weights.values()) if okr_weights else 1

    okr_data = []
    for name, weight in okr_weights.items():
        pct   = round((okr_results.get(name, 0.0)) * 100, 1)
        short = name.split('. ', 1)[1] if '. ' in name else name
        okr_data.append({
            'name':       name,
            'short':      short,
            'pct':        pct,
            'weight':     weight,
            'weight_pct': round(weight / total_w * 100, 1),
        })

    kr_data = []
    seen = set()
    for r in rows:
        if r['type'] == 'KR' and r['kr']:
            key = (r['okr'], r['kr'])
            if key not in seen:
                seen.add(key)
                p = calc_kr_progress(rows, r['okr'], r['kr'])
                okr_short = r['okr'].split('. ', 1)[1] if '. ' in r['okr'] else r['okr']
                kr_data.append({
                    'okr':       r['okr'],
                    'okr_short': okr_short,
                    'kr':        r['kr'],
                    'pct':       round(p * 100, 1),
                })

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
