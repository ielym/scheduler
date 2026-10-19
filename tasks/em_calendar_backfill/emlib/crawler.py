"""em-crawler 调用封装（R1/R3/R8 合规）。

- 统一走 IP 隧道（不假设可直连）：EM_PROXY 由 ip-tunnel 凭证派生。
- 资源错误有限重试：单次外部操作最多 2 次、固定 3s、落日志；耗尽抛 Shortage（exit 75）。
- 响应读取总时长上限：subprocess timeout 保护（防假活）。
- 空结果 = 业务事实（返回空列表，不报 75）。
- 不实现任何预防性限速（R1）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.parse

# 复用 ip-tunnel 凭证（任务自行获得连接配置，不写入调度元数据）
_IP_TUNNEL_SRC = os.environ.get("IP_TUNNEL_SRC", "/mnt/data01/projects/clihub/ip-tunnel/src")
if _IP_TUNNEL_SRC and os.path.isdir(_IP_TUNNEL_SRC) and _IP_TUNNEL_SRC not in sys.path:
    sys.path.insert(0, _IP_TUNNEL_SRC)

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk.executor import Shortage  # noqa: E402

# R3 固定常量（禁止无界/递增重试）
_MAX_RETRIES = 2
_RETRY_GAP_SEC = 3.0
# R8：单请求超时（≤60s，适配暂停宽限）；总读取上限由 subprocess timeout 表达
_REQ_TIMEOUT_SEC = 60.0

# 内部实现常量（不进 task.json.params）
_KLT_DAILY = 101
_LMT_FULL_HISTORY = 20000   # 覆盖约 80 年日线（实测 1.000001 → 1990 起 8743 行）


class CrawlerShortage(Shortage):
    """资源类错误（隧道抖动/超时/连接失败）：上浮为 exit 75。"""


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


def _proxy_url() -> str:
    """ip-tunnel 凭证 → 代理 URL（每次调用取最新，避免出口过期）。"""
    from ip_tunnel.certification import get_credential  # 延迟导入
    cred = get_credential()
    user = urllib.parse.quote(str(cred.get("username") or ""), safe="")
    pwd = urllib.parse.quote(str(cred.get("password") or ""), safe="")
    tunnel = cred.get("tunnel") or ""
    return f"http://{user}:{pwd}@{tunnel}/"


def _is_transient(err: str) -> bool:
    return any(t in err for t in ("RuntimeError", "Connection", "connection",
                                  "timed out", "Timeout", "timeout", "超时", "代理"))


def query_metric(metric: str, positional: str = "", args: dict | None = None,
                 pause_check=None):
    """调 `em-crawler <metric> [positional] [--k ...]`，返回解析后的 JSON 对象。

    资源类失败经 R3 有限重试后仍失败 → 抛 CrawlerShortage（exit 75）；其余异常上抛。
    """
    cmd = ["em-crawler", metric]
    if positional:
        cmd.append(str(positional))
    for k, v in (args or {}).items():
        if v is None or v is False:
            continue
        if v is True:
            cmd.append(f"--{k}")
        else:
            cmd += [f"--{k}", str(v)]

    last_err = ""
    for attempt in range(1 + _MAX_RETRIES):
        if attempt:
            _log(f"  em-crawler 重试 {attempt}/{_MAX_RETRIES}（{_RETRY_GAP_SEC:.0f}s 后）：{metric}")
            time.sleep(_RETRY_GAP_SEC)
        if pause_check is not None:
            pause_check()
        env = dict(os.environ)
        env["EM_HTTP_TIMEOUT"] = str(int(_REQ_TIMEOUT_SEC))
        try:
            env["EM_PROXY"] = _proxy_url()
        except Exception as e:  # 凭证暂不可得 → 资源不足
            raise CrawlerShortage(f"{type(e).__name__}: ip-tunnel 凭证获取失败（外部资源不可得）") from e
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=_REQ_TIMEOUT_SEC + 5, env=env)
        except subprocess.TimeoutExpired:
            last_err = f"TimeoutError: em-crawler {metric}（外部资源不可得）"
            continue
        out = (proc.stdout or "").strip()
        i, j = out.find("{"), out.rfind("}")
        if i < 0 or j < 0:
            last_err = f"em-crawler 未返回 JSON rc={proc.returncode}: {(out or proc.stderr or '')[:160]}"
            if _is_transient(last_err):
                continue
            raise CrawlerShortage(last_err + "（外部资源不可得）")
        try:
            data = json.loads(out[i:j + 1])
        except json.JSONDecodeError as e:
            last_err = f"JSONDecodeError: em-crawler {metric}（外部资源不可得）"
            raise CrawlerShortage(str(e)) from e
        if data.get("ok"):
            return data
        err = str(data.get("error") or "未知错误")
        if "当前不可用" in err or "空值" in err:
            # 源端查无此记录（如某标的无 K 线）：业务事实，返回空（R6，不报 75/不失败）
            return {"ok": True, "value": [], "note": err}
        last_err = err
        if not _is_transient(err):
            # 确定性失败（指标下线/参数错误）→ 业务失败，不重试
            raise RuntimeError(f"em-crawler {metric} 确定性失败：{err}")
        # 瞬时抖动 → 继续重试
    raise CrawlerShortage(f"{last_err}（外部资源不可得）")


def trading_days(secid: str, pause_check=None) -> list[str]:
    """取某标的的交易日集合（日线 K 的日期列）。

    - 单次拉全历史（lmt 为大常量，覆盖全历史）；分页/上限为内部常量。
    - 返回升序去重的 'YYYY-MM-DD' 列表；空列表 = 该标的确无 K 线（业务事实）。
    """
    data = query_metric("secid_kline", positional=secid,
                        args={"klt": _KLT_DAILY, "lmt": _LMT_FULL_HISTORY},
                        pause_check=pause_check)
    rows = data.get("value") or []
    days: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = _norm_date(r.get("date"))
        if d:
            days.add(d)
    return sorted(days)


def _norm_date(v) -> str | None:
    s = str(v or "").strip().replace("/", "-")
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else None
