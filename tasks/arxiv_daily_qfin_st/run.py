"""arXiv 论文抓取执行者（新协议 v3）：下载单日论文 PDF（走 IP 隧道代理），上传 OSS。

- 作为执行进程被调度器拉起（SCHED_ATTEMPT_ID 存在）。
- 运行参数（topic/date/query）从 SCHED_CONFIG 指向的 per-date 配置文件读取。
- 恢复/幂等：以 OSS 为准——日期级 _done.json + 单篇对象存在性（既不上传、
  也不下载已存在于 OSS 的对象）；不落任何本地断点文件。
- 协作暂停：安全边界调用 check_pause()（不落盘）。
- 统一外部不足：检索/下载临时失败 → report_shortage → exit 75；永久不可得(400/404/517)跳过。
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

_SCHED_SRC = os.environ.get(
    "SCHED_SRC", "/mnt/data01/projects/scheduler")
for _p in (
    _SCHED_SRC,
    os.environ.get("IP_TUNNEL_SRC", "/mnt/data01/projects/clihub/ip-tunnel/src"),
    os.environ.get("ARXIV_SRC", "/mnt/data01/projects/clihub/arxiv/src"),
):
    if _p and os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import requests  # noqa: E402
from sched_task_sdk import ExecutorTask, run_task  # noqa: E402
from sched_task_sdk.executor import PauseRequested, Shortage  # noqa: E402
from ip_tunnel.certification import get_credential  # noqa: E402
from arxiv.cli import parse_feed  # noqa: E402

# ------------------------------------------------------------------ 常量
ARXIV_API = "https://export.arxiv.org/api/query"
DEFAULT_PAGE_SIZE = 500
# 资源错误有限重试（R3/R9）：首次失败后原地重试最多 2 次（固定短间隔
# 3s，只过滤瞬时抖动），仍失败立即携带错误类型与资源标识上浮调度器
# （exit 75），退避/重排/并发调整由调度器统一决定。重试上限与间隔为
# 固定常量，禁止无界/递增重试；业务事实（目标不可得）与协作暂停不重试。
# 响应读取总时长上限（R8）保留：防坏出口慢速滴数据无限续命（假活）。
_TOTAL_READ_CAP_SEC = 300.0
_MAX_RETRIES = 2        # R3：资源错误任务内重试次数（首次 + 2 次重试）
_RETRY_GAP_SEC = 3.0    # R3：重试间隔（固定短间隔，仅过滤瞬时抖动）
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# attempt 级缓存/临时区：随 attempt 回收；由调度器注入 SCHED_ATTEMPT_WORK_DIR
def _work_dir() -> Path:
    v = os.environ.get("SCHED_ATTEMPT_WORK_DIR")
    return Path(v) if v else Path("/tmp/sched_attempt_work")
OSS_PAPERS_PREFIX = "ielym-data/academic/papers"
DONE_MARKER = "_done.json"

# 外部资源不预设任何限制（qps/并发均未知且会变）；不与其他任务做任何
# 直接协调（无共享锁/槽/令牌）——任务间是否共用同一外部资源是随机的，
# 任何层都不假设。资源问题全部上浮，调度器按事件流反馈统一规避。


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


# ------------------------------------------------------------------ 代理会话（每次请求换 IP；资源错误有限重试，耗尽上浮）
class ProxySession:
    def __init__(self):
        cred = get_credential()
        user = urllib.parse.quote(cred["username"], safe="")
        pwd = urllib.parse.quote(cred["password"], safe="")
        proxy_url = f"http://{user}:{pwd}@{cred['tunnel']}/"
        self.proxies = {"http": proxy_url, "https": proxy_url}

    def request(self, method, url, *, params=None, headers=None, stream=False,
                timeout=120.0, expected_status=200, pause_check=None):
        merged = {"User-Agent": USER_AGENT, "Accept": "*/*"}
        if headers:
            merged.update(headers)
        # 资源错误有限重试（R3）：最多 2 次短间隔重试过滤瞬时抖动，仍失败
        # 立即携带错误类型与资源标识上浮（R4）。400/404 目标不可得是业务
        # 事实，不重试直接穿透交调用方跳过。R8 读取总时长上限保留。
        last_err = None
        for _attempt in range(1 + _MAX_RETRIES):
            if _attempt:
                _log(f"  请求重试 {_attempt}/{_MAX_RETRIES}（{_RETRY_GAP_SEC:.0f}s 后）：{url[:100]}")
                time.sleep(_RETRY_GAP_SEC)
            # 小步听暂停：每次网络请求（含重试）前检查协作暂停。长请求窗口内
            # 收不到暂停信号会被调度器强杀；在此边界优雅停——当前篇本就没传上去，
            # 损失与强杀相同，但少占用通道、少耗时（不额外丢活）。
            if pause_check is not None:
                pause_check()
            try:
                resp = requests.request(method, url, params=params, headers=merged,
                                        proxies=self.proxies, timeout=timeout,
                                        stream=True, allow_redirects=True, verify=True)
                if resp.status_code in (400, 404):
                    resp.raise_for_status()  # 目标不可得：业务事实，交调用方跳过
                if resp.status_code != expected_status:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                # 预读 body（置 _content 后调用方 resp.content 语义与 stream=False 一致）
                _buf = bytearray()
                _t0 = time.time()
                try:
                    for _c in resp.iter_content(65536):
                        # 分块读之间听暂停：一次下载可能停滞数分钟，是最细的可
                        # 停顿粒度。中止本次下载与"被强杀"损失相同（该篇本就没
                        # 传上去），但能立刻响应、不再占通道/被强拽。
                        if pause_check is not None:
                            pause_check()
                        _buf.extend(_c)
                        if time.time() - _t0 > _TOTAL_READ_CAP_SEC:
                            # R8 慢速滴：非瞬时抖动，不重试立即上浮
                            raise Shortage(f"TimeoutError: {url}（外部资源不可得）")
                finally:
                    resp.close()
                resp._content = bytes(_buf)
                return resp
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else None
                if code in (400, 404):
                    raise
                last_err = Shortage(f"HTTP {code}: {url}（外部资源不可得）")
            except (Shortage, PauseRequested):
                raise
            except Exception as e:
                last_err = Shortage(f"{type(e).__name__}: {url}（外部资源不可得）")
        raise last_err

# ------------------------------------------------------------------ arXiv 检索
def _total_results(xml_text):
    """totalResults 解析失败 → 统一外部不足（限流页/错误页不得伪装成 0 命中结案）。"""
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(xml_text)
        ns = {"op": "http://a9.com/-/spec/opensearch/1.1/"}
        el = root.find("op:totalResults", ns)
        if el is None or not (el.text or "").strip():
            raise Shortage("arXiv 响应缺少 totalResults（疑似限流/错误页）")
        return int(el.text)
    except Shortage:
        raise
    except Exception as e:
        raise Shortage(f"arXiv 响应无法解析（疑似限流/错误页）：{e}")


def fetch_records(session, query, *, page_size=DEFAULT_PAGE_SIZE, pause_check=None):
    records = []
    start = 0
    total = None
    while True:
        # 分页之间听暂停：一页检索可能耗时数十秒，是最小可停顿边界
        if pause_check is not None:
            pause_check()
        params = {
            "search_query": query, "start": start, "max_results": page_size,
            "sortBy": "submittedDate", "sortOrder": "ascending",
        }
        url = f"{ARXIV_API}?{urllib.parse.urlencode(params)}"
        _log(f"  检索 page start={start} page_size={page_size}")
        text = session.request("GET", url, timeout=60.0, pause_check=pause_check).text
        page = parse_feed(text.encode("utf-8"))
        records.extend(page)
        if total is None:
            total = _total_results(text)
            _log(f"  arXiv 总命中 {total}，本次返回 {len(page)}")
        start += len(page)
        if not page or len(page) < page_size or (total is not None and start >= total):
            break
    return records


# ------------------------------------------------------------------ PDF 下载
def strip_version(source_id):
    return re.sub(r"v\d+$", "", source_id)


def _pdf_complete(path, expected_min_size=1024):
    if not path.is_file():
        return False
    if path.stat().st_size < expected_min_size:
        return False
    try:
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(-8, 2)
            tail = f.read()
        return head == b"%PDF" and b"%%EOF" in tail
    except OSError:
        return False


class _RecordSkip(Exception):
    """单篇永久不可得（400/404 目标缺失、517 目标被拒），跳过该篇：不重试、不报资源不足。"""


def _is_skip_error(e: BaseException) -> bool:
    code = getattr(e, "code", None)
    status = getattr(getattr(e, "response", None), "status_code", None)
    if code in (400, 404, 517) or status in (400, 404, 517):
        return True
    s = str(e)
    return "Proxy Setup Failed" in s or "status=517" in s


def download_pdf(session, arxiv_id, dest, pause_check=None):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if _pdf_complete(dest):
        return
    url = f"https://arxiv.org/pdf/{arxiv_id}"
    dest.unlink(missing_ok=True)
    # 整文件下载：内部 request() 已含 R3 有限重试；内容截断（坏出口滴出
    # 半截文件）立即上浮；仅 404/非 PDF 等目标不可得属业务事实
    try:
        resp = session.request("GET", url, timeout=60.0, stream=False,
                               pause_check=pause_check)
        data = resp.content
        if data[:4] != b"%PDF":
            raise _RecordSkip(f"{arxiv_id} 非 PDF 内容")
        if b"%%EOF" not in data:
            raise Shortage(f"内容截断: {url}（外部资源不可得）")
        dest.write_bytes(data)
    except (_RecordSkip, Shortage, PauseRequested):
        raise
    except Exception as e:
        if _is_skip_error(e):
            raise _RecordSkip(f"{arxiv_id} {e}") from e
        raise Shortage(f"{type(e).__name__}: {url}（外部资源不可得）") from e


# ------------------------------------------------------------------ OSS 上传（走 aliyun-oss CLI，凭证自动取回）
def _oss_obj(topic, date_str, name) -> str:
    return f"oss://{OSS_PAPERS_PREFIX}/{topic}/{date_str}/{name}"


def _oss_exists(topic, date_str, name, pause_check=None) -> bool:
    url = _oss_obj(topic, date_str, name)
    # 资源错误有限重试（R3）：最多 2 次短间隔重试，仍失败立即上浮；
    # 退出码 1 = 对象不存在（业务事实），不重试
    last_err = None
    for _attempt in range(1 + _MAX_RETRIES):
        if _attempt:
            _log(f"  OSS查询重试 {_attempt}/{_MAX_RETRIES}（{_RETRY_GAP_SEC:.0f}s 后）：{url[:100]}")
            time.sleep(_RETRY_GAP_SEC)
        if pause_check is not None:
            pause_check()   # 每次查询（含重试）前听暂停
        try:
            r = subprocess.run(["aliyun-oss", "exists", url],
                               capture_output=True, timeout=60)
            if r.returncode == 0:
                return True
            if r.returncode == 1:
                return False
            last_err = Shortage(f"aliyun-oss 退出码 {r.returncode}: {url}（外部资源不可得）")
        except PauseRequested:
            raise
        except Exception as e:
            last_err = Shortage(f"{type(e).__name__}: {url}（外部资源不可得）")
    raise last_err


def _oss_upload(local: Path, topic, date_str, pause_check=None):
    oss_dir = f"oss://{OSS_PAPERS_PREFIX}/{topic}/{date_str}/"
    # 资源错误有限重试（R3）：最多 2 次短间隔重试，仍失败立即上浮
    last_err = None
    for _attempt in range(1 + _MAX_RETRIES):
        if _attempt:
            _log(f"  OSS上传重试 {_attempt}/{_MAX_RETRIES}（{_RETRY_GAP_SEC:.0f}s 后）：{oss_dir[:100]}")
            time.sleep(_RETRY_GAP_SEC)
        if pause_check is not None:
            pause_check()   # 每次上传（含重试）前听暂停
        try:
            r = subprocess.run(["aliyun-oss", "upload", str(local), oss_dir],
                               capture_output=True, timeout=60)
            if r.returncode == 0:
                return
            last_err = Shortage(f"aliyun-oss 退出码 {r.returncode}: {oss_dir}（外部资源不可得）")
        except PauseRequested:
            raise
        except Exception as e:
            last_err = Shortage(f"{type(e).__name__}: {oss_dir}（外部资源不可得）")
    raise last_err

# ------------------------------------------------------------------ 任务
def _load_per_date_config():
    """从 SCHED_CONFIG 指向的 per-date 配置文件读取运行参数。"""
    rel = os.environ.get("SCHED_CONFIG") or ""
    ws = Path(os.environ.get("SCHED_WORKSPACE") or Path.cwd())
    path = ws / rel if rel else (ws / "config.json")
    if path.is_file():
        return json.loads(path.read_text("utf-8"))
    return {}


class DateFetcher(ExecutorTask):
    def __init__(self, topic, target_date, full_query, **kw):
        super().__init__(name=f"arxiv_fetch_{topic.lower()}_{target_date}", **kw)
        self.topic = topic
        self.target_date = target_date
        self.full_query = full_query

    def execute(self):
        topic, date_str = self.topic, self.target_date
        _log(f"[{topic} {date_str}] 启动，query={self.full_query[:80]}...")
        local_dir = _work_dir() / "downloads"
        local_dir.mkdir(parents=True, exist_ok=True)

        # 已完成标记 → 跳过整个日期（幂等）。此前也听一次暂停：长检索/下载前
        # 的边界检查，避免长操作窗口内收不到暂停信号。
        self.check_pause()
        if _oss_exists(topic, date_str, DONE_MARKER, pause_check=self.check_pause):
            _log(f"[{topic} {date_str}] 已完成（OSS {DONE_MARKER} 存在），跳过")
            self.complete({"count": 0, "skipped": "already_done"})
            return

        done_ids: set[str] = set()   # 本次 attempt 内已完成篇（内存记账；恢复以 OSS 为准）
        _log(f"[{topic} {date_str}] 开始处理（幂等以 OSS 为准）")

        # 代理会话建立（凭证取回）也属外部资源不足：凭证暂不可得时归为统一
        # 外部不足交调度器退避重试，而非硬失败（exit 1）。
        try:
            session = ProxySession()
        except (Shortage, _RecordSkip, PauseRequested):
            raise  # 统一外部不足/永久跳过/协作暂停直接穿透 SDK 归因
        except Exception as e:
            # 属外部不足：直接上浮交调度器退避重试（不落断点、不做任务内兜底）
            _log(f"[{topic} {date_str}] 代理会话建立失败：{e}")
            self.report_shortage(f"代理会话建立失败 {e}（外部资源不可得）")
            return  # unreachable (report_shortage raises Shortage -> exit 75)
        total = 0
        try:
            records = fetch_records(session, self.full_query,
                                    pause_check=self.check_pause)
            total = len(records)
            _log(f"[{topic} {date_str}] 命中 {total} 篇")
            if not total:
                # 0 命中内部复核：T-7 创建时已越过 arXiv 公布批次（最大滞后 4 天 +
                # 索引尾巴 1 天），持续为 0 即按真实无更新结案（小众主题某天可能
                # 确实无论文）。复核 5 次×60s 排除批次边界抖动；不再上报外部不足
                # ——真 0 场景高优先级任务无限退避会经保护轮阻塞其他任务。
                # 复核中检索异常仍穿透至外层 except → report_shortage（资源问题
                # 与真实无更新在此分流）。
                for _retry in range(1, 6):
                    _log(f"[{topic} {date_str}] 命中 0 篇，内部复核 {_retry}/5（60s 间隔）")
                    self.wait_interruptible(60)
                    records = fetch_records(session, self.full_query,
                                            pause_check=self.check_pause)
                    if records:
                        total = len(records)
                        _log(f"[{topic} {date_str}] 复核命中 {total} 篇，继续处理")
                        break
                if not total:
                    self.complete({"count": 0, "reason": "检索确认无更新"})
                    return
        except PauseRequested:
            raise  # 协作暂停：不得被下面的"检索失败"兜底吞成资源不足（exit 75）
        except Exception as e:
            # 检索失败属外部不足：上浮交调度器退避重试
            _log(f"[{topic} {date_str}] 检索失败：{e}")
            # e 为 Shortage 时已含错误类型+资源标识；其他异常（解析类）带类型包装
            self.report_shortage(str(e) if isinstance(e, Shortage)
                                 else f"检索失败 {type(e).__name__}: {e}（外部资源不可得）")
            return  # unreachable (report_shortage raises)

        def handle_one(rec):
            # 小步听暂停：每篇开始前（任何网络操作之前）检查协作暂停——
            # 与篇末检查一起把最小可停顿粒度压到"单篇/单步"，长请求窗口不会被强杀
            self.check_pause()
            source_id = (rec.get("source_id") or "").strip()
            if not source_id:
                return
            base_id = strip_version(source_id)
            file_id = base_id.replace("/", "_")
            if base_id in done_ids:
                return
            pdf = local_dir / f"{file_id}.pdf"
            meta = local_dir / f"{file_id}.json"
            # 单篇级幂等（OSS 为准）：先查对象是否已存在——已存在则既不上传、
            # 也不下载，只补 OSS 缺失的那一份（避免重复下载/上传）。
            in_oss_pdf = _oss_exists(topic, date_str, f"{file_id}.pdf",
                                     pause_check=self.check_pause)
            in_oss_json = _oss_exists(topic, date_str, f"{file_id}.json",
                                      pause_check=self.check_pause)
            if not in_oss_pdf:
                if not _pdf_complete(pdf):
                    download_pdf(session, base_id, pdf, pause_check=self.check_pause)
                _oss_upload(pdf, topic, date_str, pause_check=self.check_pause)
            if not in_oss_json:
                if not meta.is_file():
                    meta.write_text(json.dumps(rec, ensure_ascii=False),
                                    encoding="utf-8")
                _oss_upload(meta, topic, date_str, pause_check=self.check_pause)
            pdf.unlink(missing_ok=True)
            meta.unlink(missing_ok=True)
            done_ids.add(base_id)
            # 安全边界：检查暂停/停止（不落盘），并上报进展
            self.check_pause()
            self.report_progress(len(done_ids))
            _log(f"[{topic} {date_str}] [{file_id}] 完成 {len(done_ids)}/{total}")

        # 资源类错误在请求层完成 R3 有限重试（request 内 2 次），耗尽后立即
        # 上浮 Shortage → exit 75 交调度器退避重排续跑；禁止任务内多轮兜底/
        # failed_ids 稍后重试/跳过吞掉——那会延迟信号并藏住外部资源劣化。
        # 仅 400/404/517 等目标不可得属于业务事实，标记跳过。
        for rec in records:
            try:
                handle_one(rec)
            except _RecordSkip as e:
                src = strip_version((rec.get("source_id") or "").strip())
                if src:
                    done_ids.add(src)
                    self.check_pause()
                _log(f"[{topic} {date_str}] 跳过 {rec.get('source_id')}：{e}")

        # 全部完成 → 写 OSS 完成标记（含 0 篇场景）
        _log(f"[{topic} {date_str}] 全部完成（{total} 篇），写 OSS {DONE_MARKER}")
        marker = local_dir / DONE_MARKER
        marker.write_text(json.dumps({
            "topic": topic, "date": date_str, "count": total,
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        _oss_upload(marker, topic, date_str, pause_check=self.check_pause)
        marker.unlink(missing_ok=True)
        _log(f"[{topic} {date_str}] 完成")
        self.complete({"count": total})


def main() -> int:
    # 契约探针：只验证入口可执行，不做任何网络/写操作
    if os.environ.get("SCHED_CONTRACT_PROBE"):
        print("probe: run.py contract ok", flush=True)
        return 0
    if not os.environ.get("SCHED_ATTEMPT_ID") and not os.environ.get("SCHED_DEV"):
        print("缺少 SCHED_ATTEMPT_ID（仅允许调度器触发正式执行）", file=sys.stderr, flush=True)
        return 1
    cfg = _load_per_date_config()
    topic = cfg.get("topic")
    date_str = cfg.get("date")
    query = cfg.get("query")
    if not (topic and date_str and query):
        _log(f"缺运行参数（config={os.environ.get('SCHED_CONFIG')}）："
             f"topic={topic!r} date={date_str!r} query={bool(query)}")
        return 1
    full_query = f"{query} AND submittedDate:[{date_str.replace('-', '')}0000 TO {date_str.replace('-', '')}2359]"
    return run_task(DateFetcher(topic=topic, target_date=date_str, full_query=full_query))


if __name__ == "__main__":
    sys.exit(main())