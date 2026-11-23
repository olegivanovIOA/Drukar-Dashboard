"""Експорт даних дашборду DRUKAR у JSON — для WhatsApp-офісу агентів.

⚠️ КОНФІДЕНЦІЙНІСТЬ. Репозиторій Drukar-Dashboard ПУБЛІЧНИЙ, тому файли пишуться в папку agent_data/,
яка в .gitignore і НІКОЛИ не комітиться сюди. Workflow копіює її в ОКРЕМИЙ ПРИВАТНИЙ репозиторій
(olegivanovIOA/drukar-agent-data) — його не видно в інтернеті й не індексують пошуковики; читає його лише
офіс агентів за токеном.

Викликається з generate.py ПІСЛЯ того, як index.html / middle.html уже записані, і загорнутий
у try/except: будь-яка помилка тут НЕ впливає на дашборд.

Що пишемо (кожен файл має fetched_at):
  production.json        — виробництво по місяцях (кг PETG/PLA, НФ, брак %, шт), по SKU, теплокарта ліній, норми
  production_daily.json  — по днях: SKU (кг) і лінії (день/ніч)
  sales.json             — відвантаження в ТОННАХ по каналах і місяцях, по SKU (кг) — без грошей
  sales_daily.json       — відвантаження по днях: SKU, опт/роздріб, кг
  okr.json               — OKR: компанія %, OKR, KR, внесок людей
  projects.json          — реєстр проєктів (як на вкладці «Проєкти»)
  hr.json                — плинність кадрів по місяцях
  finance.json           — ГРОШІ (дохід, витрати, прибуток, собівартість, ціни, Календар Грошей).
                           Пишеться, якщо EXPORT_FINANCE_JSON=1 (у workflow ввімкнено: дані йдуть лише
                           в приватний репозиторій).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

OUT_DIR = os.environ.get("AGENT_DATA_DIR", "agent_data")   # НЕ в публічному репо (.gitignore)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write(name: str, payload: dict):
    os.makedirs(OUT_DIR, exist_ok=True)
    payload = {"fetched_at": _ts(), **payload}
    with open(os.path.join(OUT_DIR, name), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  {OUT_DIR}/{name} OK")


def _at(arr, i):
    try:
        return arr[i]
    except (IndexError, TypeError, KeyError):
        return None


def _okr_obj(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def export(data: dict, sales: dict | None, okr: dict | None, hm_labels, hm_data, line_norms,
           calc: dict | None, calc_ext: dict | None, month_order: list[str]):
    errors = []

    def step(name, fn):
        try:
            fn()
        except Exception as e:  # кожен файл незалежний
            errors.append(f"{name}: {e}")
            print(f"  WARNING export {name}: {e}")

    # ---- виробництво ----
    def production():
        keys = ("total_prod", "petg_prod", "pla_prod", "petg_nf_kg", "pla_nf_kg", "nf_pct",
                "petg_waste_kg", "pla_waste_kg", "waste_pct", "petg_pcs", "pla_pcs")
        months = []
        for i, ym in enumerate(month_order):
            row = {"month": ym}
            for k in keys:
                row[k] = _at(data.get(k), i)
            if row["total_prod"] is not None or row["petg_prod"] is not None:
                months.append(row)
        by_sku = []
        for sku, arr in (data.get("prod_by_sku") or {}).items():
            for i, ym in enumerate(month_order):
                v = _at(arr, i)
                if v:
                    by_sku.append({"month": ym, "sku": sku, "kg": v})
        heat = []
        for line, arr in (hm_data or {}).items():
            for i, lbl in enumerate(hm_labels or []):
                v = _at(arr, i)
                if v is not None:
                    heat.append({"month": lbl, "line": line, "kg": v})
        _write("production.json", {
            "about": "Виробництво DRUKAR з журналів локацій. Одиниці: кг; *_pct — відсотки; *_pcs — шт.",
            "months": months, "by_sku": by_sku, "lines_by_month": heat,
            "line_norms": line_norms or {},
        })

    def production_daily():
        skus = [{"d": r[0], "sku": r[1], "kg": r[2]} for r in (data.get("prod_daily") or []) if len(r) >= 3]
        lines = [{"d": r[0], "line": r[1], "loc": r[2], "day_kg": r[3], "night_kg": r[4], "unknown_shift_kg": r[5],
                  "kg": round((r[3] or 0) + (r[4] or 0) + (r[5] or 0), 1)}
                 for r in (data.get("line_daily") or []) if len(r) >= 6]
        _write("production_daily.json", {
            "about": "Виробництво по днях: skus (кг по SKU), lines (кг по лініях і змінах; loc — номер локації).",
            "skus": skus, "lines": lines,
        })

    # ---- продажі (без грошей) ----
    def sales_f():
        s = sales or {}
        sm = s.get("sales_months") or []
        months = [{"month": ym,
                   "stream_t": _at(s.get("sales_opt1_kg"), i),
                   "easy_t": _at(s.get("sales_opt2_kg"), i),
                   "retail_t": _at(s.get("sales_ret_kg"), i)} for i, ym in enumerate(sm)]
        by_sku = []
        for ch, key in (("опт", "sku_sales_opt"), ("роздріб", "sku_sales_ret")):
            for sku, arr in (s.get(key) or {}).items():
                for i, ym in enumerate(month_order):
                    v = _at(arr, i)
                    if v:
                        by_sku.append({"month": ym, "sku": sku, "channel": ch, "kg": v})
        _write("sales.json", {
            "about": "Відвантаження з Журналу Відвантажень. months: тонни по каналах (stream_t — ТОВ Стрім Техно, "
                     "easy_t — EASY3DPRINT, retail_t — роздріб). by_sku: кг по SKU, каналу й місяцю.",
            "months": months, "by_sku": by_sku,
        })

    def sales_daily():
        rows = [{"d": r[0], "sku": r[1], "channel": "опт" if r[2] == "o" else "роздріб", "kg": r[3],
                 "kg_estimated": r[4]} for r in ((sales or {}).get("sku_sales_daily") or []) if len(r) >= 5]
        _write("sales_daily.json", {"about": "Відвантаження по днях: SKU, канал, кг (kg_estimated — кг, "
                                             "оцінені з суми, коли кг не вказано).", "rows": rows})

    # ---- OKR / проєкти / HR ----
    def okr_f():
        o = okr or {}
        _write("okr.json", {
            "about": "OKR стратегії DRUKAR: company_pct — виконання компанії %, okrs — цілі з вагою і %, "
                     "krs — ключові результати, people — внесок людей.",
            "company_pct": round(o["company_pct"] * 100, 1) if o.get("company_pct") is not None else None,
            "okrs": _okr_obj(o.get("okr_data_json")) or [],
            "krs": _okr_obj(o.get("kr_data_json")) or [],
            "people": _okr_obj(o.get("people_json")) or [],
            "history": _okr_obj(o.get("okr_history_json")) or [],
        })

    def projects_f():
        _write("projects.json", {"about": "Проєкти (як на вкладці «Проєкти» дашборду).",
                                 **(data.get("projects") or {})})

    def hr_f():
        _write("hr.json", {"about": "HR DRUKAR: плинність кадрів по місяцях.", **(data.get("hr") or {})})

    # ---- гроші: лише за явним дозволом ----
    def finance_f():
        if os.environ.get("EXPORT_FINANCE_JSON", "0") != "1":
            path = os.path.join(OUT_DIR, "finance.json")
            if os.path.exists(path):
                os.remove(path)          # вимкнули прапорець → прибираємо старий файл
            print("  data/finance.json пропущено (EXPORT_FINANCE_JSON≠1)")
            return
        s = sales or {}
        months = []
        for i, ym in enumerate(month_order):
            row = {"month": ym, "income": _at(data.get("income"), i), "expenses": _at(data.get("expenses"), i),
                   "profit": _at(data.get("profit"), i), "cost_petg_kg": _at(data.get("cost_petg_kg"), i),
                   "cost_pla_kg": _at(data.get("cost_pla_kg"), i)}
            if any(v is not None for k, v in row.items() if k != "month"):
                months.append(row)
        sm = s.get("sales_months") or []
        _write("finance.json", {
            "about": "Гроші DRUKAR, грн: P&L по місяцях, виручка опт/роздріб, середні ціни, календар управлінця, "
                     "тижневий Календар Грошей (9 шарів), калькулятор.",
            "pnl_months": months,
            "revenue_months": [{"month": ym, "opt": _at(s.get("sales_opt"), i), "retail": _at(s.get("sales_ret"), i)}
                               for i, ym in enumerate(sm)],
            "manager_calendar": [{"month": ym, "income": _at(data.get("cal_income"), i),
                                  "out": _at(data.get("cal_out"), i), "bal_start": _at(data.get("cal_bal_start"), i),
                                  "bal_end": _at(data.get("cal_bal_end"), i)}
                                 for i, ym in enumerate(data.get("cal_ym") or [])],
            "money_calendar_weeks": data.get("mc_weeks") or [], "money_calendar_anchor": data.get("mc_anchor"),
            "avg_price_petg": dict(zip(s.get("petg_price_labels") or [], s.get("petg_avg_price") or [])),
            "avg_price_pla": dict(zip(s.get("pla_price_labels") or [], s.get("pla_avg_price") or [])),
            "top_products_revenue": dict(zip(s.get("top_labels") or [], s.get("top_data") or [])),
            "calculator": {**(calc or {}), **(calc_ext or {})},
        })

    for name, fn in (("production", production), ("production_daily", production_daily), ("sales", sales_f),
                     ("sales_daily", sales_daily), ("okr", okr_f), ("projects", projects_f), ("hr", hr_f),
                     ("finance", finance_f)):
        step(name, fn)
    _write("_index.json", {"files": sorted(f for f in os.listdir(OUT_DIR) if f.endswith(".json")),
                           "errors": errors})
