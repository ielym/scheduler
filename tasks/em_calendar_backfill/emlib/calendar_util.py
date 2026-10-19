"""日历构造工具（date 分区 · 每日全量快照）。

列：`date`(date32) + `is_trade_day`(bool) + `tz_local`(string) + `sessions`(list<struct>)。
`tz_local` = 市场当地 IANA 时区；`sessions` = 该交易所交易日 D 的时段（当地墙钟 + 北京墙钟，见 sessions.py）。
"""
from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path

_SESSION_FIELDS = [
    ("type", "string"),
    ("local_open", "string"),
    ("local_close", "string"),
    ("bjt_open", "string"),
    ("bjt_close", "string"),
    ("crosses_day_bjt", "bool"),
]


def norm_date(v) -> str | None:
    s = str(v or "").strip().replace("/", "-")
    if not s or s in ("-", "--", "0"):
        return None
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else None


def d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def natural_days(start: str, end: str) -> list[str]:
    """[start, end] 的全部自然日（含端点），'YYYY-MM-DD' 升序。"""
    a, b = d(start), d(end)
    if b < a:
        return []
    out, cur, step = [], a, dt.timedelta(days=1)
    while cur <= b:
        out.append(cur.isoformat())
        cur += step
    return out


def _session_struct():
    import pyarrow as pa
    return pa.struct([
        pa.field(name, pa.bool_() if typ == "bool" else pa.string())
        for name, typ in _SESSION_FIELDS
    ])


def write_parquet(rows: list[dict], path: Path) -> str:
    """写 Parquet（新 schema），返回文件 sha256。

    rows 元素：{"date": 'YYYY-MM-DD', "is_trade_day": bool, "tz_local": str,
                "sessions": [ {type, local_open, local_close, bjt_open, bjt_close, crosses_day_bjt}, ... ]}
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("date", pa.date32()),
        pa.field("is_trade_day", pa.bool_()),
        pa.field("tz_local", pa.string()),
        pa.field("sessions", pa.list_(_session_struct())),
    ])
    tbl = pa.table({
        "date": [dt.date.fromisoformat(r["date"]) for r in rows],
        "is_trade_day": [bool(r["is_trade_day"]) for r in rows],
        "tz_local": [str(r.get("tz_local") or "UTC") for r in rows],
        "sessions": [[_norm_session(s) for s in (r.get("sessions") or [])] for r in rows],
    }, schema=schema)
    pq.write_table(tbl, path, compression="zstd")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _norm_session(s: dict) -> dict:
    return {
        "type": str(s.get("type") or ""),
        "local_open": str(s.get("local_open") or ""),
        "local_close": str(s.get("local_close") or ""),
        "bjt_open": str(s.get("bjt_open") or ""),
        "bjt_close": str(s.get("bjt_close") or ""),
        "crosses_day_bjt": bool(s.get("crosses_day_bjt")),
    }
