"""市场/资产发现（**每轮实时查询**，不写死市场清单）。

候选来源：
- 股票/指数：global_index（国际指数，含 secid）+ 国内主指数（上证 1.000001）。
- 非股票：forex_list（外汇）、gold_global_futures（黄金）、futures_contract（各期货交易所，取主连）。

返回候选列表：[{key, secid, code, name, group}]。key 由 naming 归一（未命中 → code 兜底）。
"""
from __future__ import annotations

from . import crawler, naming

# 期货交易所（东财固定提供的 6 个场内交易所）→ 对应的「上市合约列表」指标
_FUTURES_METRICS = {
    "shfe": "futures_shfe", "ine": "futures_ine", "dce": "futures_dce",
    "czce": "futures_czce", "gfex": "futures_gfex", "cffex": "futures_cffex",
}
# 外汇代表对（优先项；缺失则取清单首条）——仅用于「代表日历标的」，非市场清单
_FX_PREF = ["EURUSD", "USDCNY", "USDJPY", "GBPUSD", "USDHKD"]
# 黄金代表合约优先项
_GOLD_PREF = ["GC00Y", "AU00Y", "Au99.95"]


def _secid_of_row(row: dict) -> str | None:
    """clist 行 → secid：优先行内 secid，否则 market.code。"""
    sid = row.get("secid")
    if sid:
        return str(sid)
    m, c = row.get("market"), row.get("code")
    if m is not None and c:
        return f"{m}.{c}"
    return None


def discover_candidates(pause_check=None) -> list[dict]:
    cands: list[dict] = []

    # --- 股票/指数：国际指数（动态） ---
    data = crawler.query_metric("global_index", args={"page_size": 500}, pause_check=pause_check)
    for row in (data.get("value") or []):
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").strip()
        sid = _secid_of_row(row)
        key = naming.key_of(code)
        if not sid or key is None:
            continue
        cands.append({"key": key, "secid": sid, "code": code,
                      "name": row.get("name"), "group": "index"})

    # --- 股票/指数：国内主指数（上证）---
    cands.append({"key": "cn", "secid": "1.000001", "code": "000001",
                  "name": "上证指数", "group": "domestic"})

    # --- 非股票：外汇（取代表对）---
    fx = _pick_representative("forex_list", _FX_PREF, key="fx", pause_check=pause_check)
    if fx:
        cands.append(fx)

    # --- 非股票：黄金（取代表合约）---
    gold = _pick_representative("gold_global_futures", _GOLD_PREF, key="gold",
                                pause_check=pause_check)
    if gold:
        cands.append(gold)

    # --- 非股票：各期货交易所（取主连）---
    for ex, metric in _FUTURES_METRICS.items():
        try:
            data = crawler.query_metric(metric, args={"page_size": 300},
                                        pause_check=pause_check)
        except Exception:
            continue  # 单交易所不可得不致命（其日历可与其它合并/另轮重试）
        rep = _pick_main_contract(data.get("value") or [], key=ex)
        if rep:
            cands.append(rep)

    return cands


def _pick_representative(metric: str, prefer: list[str], key: str,
                         pause_check=None) -> dict | None:
    data = crawler.query_metric(metric, args={"page_size": 500}, pause_check=pause_check)
    rows = [r for r in (data.get("value") or []) if isinstance(r, dict)]
    if not rows:
        return None
    chosen = None
    for p in prefer:
        chosen = next((r for r in rows if str(r.get("code")) == p), None)
        if chosen:
            break
    chosen = chosen or rows[0]
    sid = _secid_of_row(chosen)
    if not sid:
        return None
    return {"key": key, "secid": sid, "code": str(chosen.get("code") or ""),
            "name": chosen.get("name"), "group": "non_equity"}


def _pick_main_contract(rows: list[dict], key: str) -> dict | None:
    """期货交易所代表：优先主连（代码以 m/M 结尾，如 fum/cum/URM/TSM），否则首条。"""
    if not rows:
        return None
    main = next((r for r in rows if str(r.get("code") or "").lower().endswith("m")), None)
    chosen = main or rows[0]
    sid = _secid_of_row(chosen)
    if not sid:
        return None
    return {"key": key, "secid": sid, "code": str(chosen.get("code") or ""),
            "name": chosen.get("name"), "group": "futures"}
