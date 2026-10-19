"""OSS 访问封装（走 aliyun-oss CLI，凭证自动取回）。

- 幂等真源 = 对象存在性；写入 = 临时对象 → 校验 → copy 到最终路径（原子）。
- 资源错误有限重试 2×3s，耗尽抛 OssShortage（exit 75）。
- 不含任何账号/凭证（由 CLI 自行取回）。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk.executor import Shortage  # noqa: E402

BUCKET = "ielym-data"
PREFIX = "stock_data/calendar"

_MAX_RETRIES = 2
_RETRY_GAP_SEC = 3.0
_TIMEOUT_SEC = 60.0


class OssShortage(Shortage):
    """OSS 资源不可得（上浮为 exit 75）。"""


def _log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}", flush=True)


def _base() -> str:
    return f"oss://{BUCKET}/{PREFIX}"


def obj(name: str) -> str:
    return f"{_base()}/{name}"


def _run(cmd: list[str], retryable_rc: set[int] | None = None,
         pause_check=None) -> int:
    last_err = ""
    for attempt in range(1 + _MAX_RETRIES):
        if attempt:
            _log(f"  OSS 重试 {attempt}/{_MAX_RETRIES}（{_RETRY_GAP_SEC:.0f}s 后）：{cmd[-1]}")
            time.sleep(_RETRY_GAP_SEC)
        if pause_check is not None:
            pause_check()
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=_TIMEOUT_SEC)
        except Exception as e:
            last_err = f"{type(e).__name__}: aliyun-oss（外部资源不可得）"
            continue
        if r.returncode == 0:
            return 0
        if retryable_rc and r.returncode in retryable_rc:
            return r.returncode
        last_err = f"aliyun-oss 退出码 {r.returncode}: {' '.join(cmd[1:3])}（外部资源不可得）"
    raise OssShortage(last_err)


def exists(oss_obj: str, pause_check=None) -> bool:
    """对象是否存在（退出 0=存在，1=不存在；其余为资源错误）。"""
    rc = _run(["aliyun-oss", "exists", oss_obj], retryable_rc={1}, pause_check=pause_check)
    return rc == 0


def upload(local: str, oss_dir: str, pause_check=None) -> None:
    """上传本地文件到 OSS 目录（oss_dir 以 / 结尾）。"""
    _run(["aliyun-oss", "upload", str(local), oss_dir.rstrip("/") + "/"],
         pause_check=pause_check)


def copy(src_obj: str, dst_obj: str, pause_check=None) -> None:
    _run(["aliyun-oss", "cp", src_obj, dst_obj], pause_check=pause_check)


def remove(target: str, pause_check=None) -> None:
    _run(["aliyun-oss", "rm", target], pause_check=pause_check)
