"""市场 →（当地时区 + 交易时段规则 + override）；交易日 D 的时段构造（DST / 夜盘 / 跨日）。

- 规则表只覆盖**已知市场键**；**未命中 → 兜底**（`tz_local="UTC"`、`sessions=[]`、source="unknown"）。
- 规则表是**时段规则**、不是「市场清单」：市场集合仍由 `discovery` 每轮动态发现。
- 每个时段同时输出「当地墙钟」与「北京墙钟」（均带日历日）+ `crosses_day_bjt`。

时段模板 = (type, anchor, open_hm, close_hm)：
- anchor ∈ {"D", "D-1", "prev_trade"}；close 所在日 = open 所在日 +（close_hm > open_hm ? 0 : 1）。
  - `D`        当地自然日 D
  - `D-1`      当地自然日 D-1（外汇/贵金属换日：D-1 17:00 → D 17:00）
  - `prev_trade` D 的**上一个交易日**（期货夜盘归属下一交易日）
"""
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")
_DAY = dt.timedelta(days=1)

# key -> (tz_local, [ (type, anchor, open_hm, close_hm), ... ])
# 标注 ✅ 者为 2026-10 分钟数据实测校验；其余为交易所公开时段（近似，可由观测/override 修正）。
KNOWN: dict[str, tuple[str, list[tuple[str, str, str, str]]]] = {
    # 已验证 ✅
    "cn":   ("Asia/Shanghai",      [("morning", "D", "09:30", "11:30"), ("afternoon", "D", "13:00", "15:00")]),
    "hk":   ("Asia/Hong_Kong",     [("morning", "D", "09:30", "12:00"), ("afternoon", "D", "13:00", "16:00")]),
    "us":   ("America/New_York",   [("regular", "D", "09:30", "16:00")]),
    "ca":   ("America/Toronto",    [("regular", "D", "09:30", "16:00")]),
    "jp":   ("Asia/Tokyo",         [("morning", "D", "09:00", "11:30"), ("afternoon", "D", "12:30", "15:30")]),
    "kr":   ("Asia/Seoul",         [("regular", "D", "09:00", "15:30")]),
    "tw":   ("Asia/Taipei",        [("regular", "D", "09:00", "13:30")]),
    "sg":   ("Asia/Singapore",     [("morning", "D", "09:00", "12:00"), ("afternoon", "D", "13:00", "17:00")]),
    "in":   ("Asia/Kolkata",       [("regular", "D", "09:15", "15:30")]),
    "au":   ("Australia/Sydney",   [("regular", "D", "10:00", "16:00")]),
    "uk":   ("Europe/London",      [("regular", "D", "08:00", "16:30")]),
    "de":   ("Europe/Berlin",      [("regular", "D", "09:00", "17:30")]),
    "fr":   ("Europe/Paris",       [("regular", "D", "09:00", "17:30")]),
    "fx":   ("America/New_York",   [("continuous", "D-1", "17:00", "17:00")]),
    "gold": ("America/New_York",   [("continuous", "D-1", "18:00", "17:00")]),
    # 期货（日盘 + 夜盘；夜盘按 prev_trade 锚到上一交易日）
    "shfe": ("Asia/Shanghai",      [("night", "prev_trade", "21:00", "23:00"),
                                    ("morning", "D", "09:00", "10:15"), ("morning2", "D", "10:30", "11:30"),
                                    ("afternoon", "D", "13:30", "15:00")]),
    "ine":  ("Asia/Shanghai",      [("night", "prev_trade", "21:00", "23:00"),
                                    ("morning", "D", "09:00", "10:15"), ("morning2", "D", "10:30", "11:30"),
                                    ("afternoon", "D", "13:30", "15:00")]),
    "dce":  ("Asia/Shanghai",      [("night", "prev_trade", "21:00", "23:00"),
                                    ("morning", "D", "09:00", "10:15"), ("morning2", "D", "10:30", "11:30"),
                                    ("afternoon", "D", "13:30", "15:00")]),
    "czce": ("Asia/Shanghai",      [("night", "prev_trade", "21:00", "23:00"),
                                    ("morning", "D", "09:00", "10:15"), ("morning2", "D", "10:30", "11:30"),
                                    ("afternoon", "D", "13:30", "15:00")]),
    "gfex": ("Asia/Shanghai",      [("morning", "D", "09:00", "10:15"), ("morning2", "D", "10:30", "11:30"),
                                    ("afternoon", "D", "13:30", "15:00")]),
    "cffex": ("Asia/Shanghai",     [("morning", "D", "09:30", "11:30"), ("afternoon", "D", "13:00", "15:00")]),
    # 其它欧洲/亚太（近似）
    "it":   ("Europe/Rome",        [("regular", "D", "09:00", "17:30")]),
    "es":   ("Europe/Madrid",      [("regular", "D", "09:00", "17:30")]),
    "nl":   ("Europe/Amsterdam",   [("regular", "D", "09:00", "17:30")]),
    "be":   ("Europe/Brussels",    [("regular", "D", "09:00", "17:30")]),
    "at":   ("Europe/Vienna",      [("regular", "D", "09:00", "17:30")]),
    "pt":   ("Europe/Lisbon",      [("regular", "D", "08:00", "16:30")]),
    "ie":   ("Europe/Dublin",      [("regular", "D", "08:00", "16:30")]),
    "ch":   ("Europe/Zurich",      [("regular", "D", "09:00", "17:30")]),
    "se":   ("Europe/Stockholm",   [("regular", "D", "09:00", "17:30")]),
    "no":   ("Europe/Oslo",        [("regular", "D", "09:00", "16:30")]),
    "dk":   ("Europe/Copenhagen",  [("regular", "D", "09:00", "17:00")]),
    "fi":   ("Europe/Helsinki",    [("regular", "D", "10:00", "18:30")]),
    "pl":   ("Europe/Warsaw",      [("regular", "D", "09:00", "17:00")]),
    "cz":   ("Europe/Prague",      [("regular", "D", "09:00", "17:00")]),
    "gr":   ("Europe/Athens",      [("regular", "D", "10:15", "17:20")]),
    "is":   ("Atlantic/Reykjavik", [("regular", "D", "09:30", "15:30")]),
    "eu":   ("Europe/Berlin",      [("regular", "D", "09:00", "17:30")]),
    "ru":   ("Europe/Moscow",      [("regular", "D", "10:00", "18:40")]),
    "mx":   ("America/Mexico_City",[("regular", "D", "09:30", "15:00")]),
    "br":   ("America/Sao_Paulo",  [("regular", "D", "10:00", "17:00")]),
    "nz":   ("Pacific/Auckland",   [("regular", "D", "10:00", "17:00")]),
    "id":   ("Asia/Jakarta",       [("morning", "D", "09:00", "12:00"), ("afternoon", "D", "13:30", "15:00")]),
    "my":   ("Asia/Kuala_Lumpur",  [("morning", "D", "09:00", "12:30"), ("afternoon", "D", "14:30", "16:45")]),
    "th":   ("Asia/Bangkok",       [("morning", "D", "10:00", "12:30"), ("afternoon", "D", "14:30", "16:30")]),
    "vn":   ("Asia/Ho_Chi_Minh",   [("morning", "D", "09:15", "11:30"), ("afternoon", "D", "13:00", "15:00")]),
    "ph":   ("Asia/Manila",        [("morning", "D", "09:30", "12:00"), ("afternoon", "D", "13:00", "15:30")]),
    "pk":   ("Asia/Karachi",       [("regular", "D", "09:30", "15:30")]),
    "lk":   ("Asia/Colombo",       [("regular", "D", "09:30", "14:30")]),
    "sa":   ("Asia/Riyadh",        [("regular", "D", "10:00", "15:30")]),
}

# 半日市/临时休市 override：(key, 'YYYY-MM-DD') -> sessions 模板（同 KNOWN 格式）。
# 示例（启用前需按当年实际公告维护）：
#   ("us", "2026-11-27"): [("regular", "D", "09:30", "13:00")],
OVERRIDES: dict[tuple[str, str], list[tuple[str, str, str, str]]] = {}


def _minutes(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def _resolve_anchor(anchor: str, day: dt.date, trading_days: list[dt.date]) -> dt.date | None:
    if anchor == "D":
        return day
    if anchor == "D-1":
        return day - _DAY
    if anchor == "prev_trade":
        prev = [x for x in trading_days if x < day]
        return prev[-1] if prev else None
    return None


def _fmt(moment: dt.datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M")


def build_templates(templates: list[tuple[str, str, str, str]],
                    day: dt.date, tz_local: str,
                    trading_days: list[dt.date]) -> list[dict]:
    """把时段模板实例化为「当地 + 北京」双墙钟的 session 列表。"""
    tz = ZoneInfo(tz_local)
    out: list[dict] = []
    for stype, anchor, open_hm, close_hm in templates:
        open_day = _resolve_anchor(anchor, day, trading_days)
        if open_day is None:
            continue  # 无上一交易日（如首日）→ 该锚定段缺失
        close_day = open_day if _minutes(close_hm) > _minutes(open_hm) else open_day + _DAY
        o_local = dt.datetime.combine(open_day, dt.time(int(open_hm[:2]), int(open_hm[3:])))
        c_local = dt.datetime.combine(close_day, dt.time(int(close_hm[:2]), int(close_hm[3:])))
        o_bjt = o_local.replace(tzinfo=tz).astimezone(BJT)
        c_bjt = c_local.replace(tzinfo=tz).astimezone(BJT)
        out.append({
            "type": stype,
            "local_open": _fmt(o_local), "local_close": _fmt(c_local),
            "bjt_open": _fmt(o_bjt), "bjt_close": _fmt(c_bjt),
            "crosses_day_bjt": o_bjt.date() != c_bjt.date(),
        })
    return out


def market_tz(key: str) -> str:
    """市场键 → 当地 IANA 时区（未命中 → UTC 兜底）。"""
    rule = KNOWN.get(key)
    return rule[0] if rule else "UTC"


def tz_token(key: str) -> str:
    """市场键 → 路径安全的时区 token（IANA 的 '/' 替换为 '_'）。

    用于 date 分区值 `date=<YYYY-MM-DD>_<tz_token>`，令日期**自解释所属时区**。
    **必须用 IANA 名**（不随 DST 变化），不得用 UTC 偏移（DST 切换会使路径不稳定）。
    """
    return market_tz(key).replace("/", "_")


def sessions_for(key: str, day: dt.date, trading_days: list[dt.date]) -> tuple[str, list[dict], str]:
    """交易日 D 的 (tz_local, sessions, source)。

    - 优先 override；其次 KNOWN 规则；未命中 → 空 sessions（source="unknown"）。
    - 非交易日由调用方判断（is_trade_day=False 时不调用本函数）。
    - source ∈ {"override", "rule", "unknown"}。
    """
    tz_local = market_tz(key)
    ov = OVERRIDES.get((key, day.isoformat()))
    if ov is not None:
        return tz_local, build_templates(ov, day, tz_local, trading_days), "override"
    rule = KNOWN.get(key)
    if rule:
        return tz_local, build_templates(rule[1], day, tz_local, trading_days), "rule"
    return tz_local, [], "unknown"
