"""共享模块：arXiv 论文检索 + PDF 下载（全部通过 IP 隧道代理）。

所有网络请求走快代理隧道（每次请求换 IP），内置指数退避重试。
使用 requests 库（比 urllib 更稳定处理 CONNECT 隧道）。
"""
from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------- 动态注入依赖路径
# ip_tunnel 与 arxiv 包未 pip-install，运行时从源码目录加载
for _p in (
    os.environ.get("IP_TUNNEL_SRC", "/mnt/data01/projects/clihub/ip-tunnel/src"),
    os.environ.get("ARXIV_SRC", "/mnt/data01/projects/clihub/arxiv/src"),
):
    if _p and os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import requests  # noqa: E402
from ip_tunnel.certification import get_credential  # noqa: E402
from arxiv.cli import parse_feed  # noqa: E402

# ---------------------------------------------------------------- 常量
ARXIV_API = "https://export.arxiv.org/api/query"
DEFAULT_PAGE_SIZE = 500
DEFAULT_MAX_RETRIES = 8          # 代理指数退避重试次数（覆盖隧道瞬时失败）
PDF_DOWNLOAD_RETRIES = 6         # PDF 下载（单篇）额外重试
PDF_RANGE_CHUNK = 2 * 1024 * 1024  # Range 分块大小
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
# --- 跨进程全局代理并发限制 ---
# 快代理隧道 5 req/s + 5Mbps 带宽，并发过高会导致 429/517/带宽不足
# 用 /tmp/sched_proxy_sem 目录作计数：每个 slot = 一个唯一子目录（mkdir 原子创建）
#   mkdir() 原子创建 — 如果已存在就失败，因此天然杜绝竞态
#   进程退出时 atexit 删除自己的 slot 目录
_PROXY_SEM_DIR = Path("/tmp/sched_proxy_sem")
_PROXY_MAX_CONCURRENT = 8       # 最多 8 个进程同时使用代理
_PROXY_SEM_WAIT = 0.5           # 等待 slot 的轮询间隔（秒）


def _proxy_acquire() -> str:
    """获取一个代理 slot（原子创建唯一子目录）。返回 slot 目录路径。"""
    _PROXY_SEM_DIR.mkdir(parents=True, exist_ok=True)
    import uuid
    slot = _PROXY_SEM_DIR / f"{os.getpid()}-{uuid.uuid4().hex[:8]}.slot"
    # 轮询等待空位 — 每个空位用 os.mkdir 原子创建
    for _ in range(300):  # 最多 ~150s
        # 清理僵尸 slot（目录超过 120s 未修改 = 进程已死）
        now = time.time()
        for d in _PROXY_SEM_DIR.iterdir():
            if not d.is_dir():
                continue
            try:
                if now - d.stat().st_mtime > 120:
                    import shutil
                    shutil.rmtree(d, ignore_errors=True)
            except OSError:
                pass
        # 统计现有 slot（只数 .slot 子目录）
        try:
            active = sum(1 for d in _PROXY_SEM_DIR.iterdir()
                         if d.is_dir() and d.name.endswith(".slot"))
        except OSError:
            active = 0
        if active < _PROXY_MAX_CONCURRENT:
            try:
                os.mkdir(slot)
                # 双保险：如果被别人抢先 mkdir 成功了我们再数一次
                active2 = sum(1 for d in _PROXY_SEM_DIR.iterdir()
                              if d.is_dir() and d.name.endswith(".slot"))
                if active2 <= _PROXY_MAX_CONCURRENT:
                    return str(slot)
                else:
                    # 超限了 → 回退
                    import shutil; shutil.rmtree(slot, ignore_errors=True)
            except FileExistsError:
                pass
        time.sleep(_PROXY_SEM_WAIT)
    raise RuntimeError(f"等待代理 slot 超时（>{_PROXY_MAX_CONCURRENT} 并发）")


def _proxy_release(slot_path: str) -> None:
    import shutil
    try: shutil.rmtree(slot_path, ignore_errors=True)
    except OSError: pass


# ---------------------------------------------------------------- 代理初始化
class ProxySession:
    """封装 requests.Session + 隧道代理。每次 .request() 自动换 IP。

    ⚠️ 全局并发限制：构造时从跨进程代理 semaphore 获取一个 slot，
    防止 47 个 run.py 同时打爆 5 req/s 的快代理隧道。进程退出时自动释放。
    """

    def __init__(self, *, retries: int = DEFAULT_MAX_RETRIES):
        import atexit as _atexit
        self._slot = _proxy_acquire()
        _atexit.register(_proxy_release, self._slot)

        cred = get_credential()
        tunnel_user = cred["username"]
        tunnel_pwd = cred["password"]
        tunnel_host = cred["tunnel"]

        # 构造代理 URL
        from urllib.request import quote
        user = quote(tunnel_user, safe="")
        pwd = quote(tunnel_pwd, safe="")
        proxy_url = f"http://{user}:{pwd}@{tunnel_host}/"
        self.proxies = {"http": proxy_url, "https": proxy_url}
        self.retries = retries

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        stream: bool = False,
        timeout: float = 120.0,
        expected_status: int = 200,
        max_retries: int | None = None,
    ) -> requests.Response:
        """带指数退避的代理请求。失败抛 requests.HTTPError 或 RuntimeError。"""
        merged_headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
        if headers:
            merged_headers.update(headers)

        attempts = max_retries if max_retries is not None else self.retries
        last_exc: Exception | None = None
        last_status: int | None = None

        for attempt in range(attempts + 1):
            try:
                resp = requests.request(
                    method, url, params=params, headers=merged_headers,
                    proxies=self.proxies, timeout=timeout, stream=stream,
                    allow_redirects=True, verify=True,
                )
                # arXiv 限流 / 隧道瞬时失败 → 重试
                if resp.status_code in (429, 408, 500, 502, 503, 504, 440, 441, 515, 516, 517):
                    last_status = resp.status_code
                    if attempt < attempts:
                        # arXiv 官方建议 3s 间隔
                        time.sleep(3 * (2 ** attempt) + attempt * 0.5)
                        continue
                if resp.status_code != expected_status:
                    last_status = resp.status_code
                    if attempt < attempts:
                        time.sleep(2 * (2 ** attempt))
                        continue
                    resp.raise_for_status()
                return resp
            except (requests.ConnectionError, requests.Timeout) as e:
                # RemoteDisconnected 等网络层错误 → 重试
                last_exc = e
                if attempt < attempts:
                    time.sleep(1.5 * (2 ** attempt) + 1)
                    continue
            except requests.HTTPError as e:
                last_status = e.response.status_code if e.response is not None else None
                if attempt < attempts:
                    time.sleep(2 * (2 ** attempt))
                    continue
                raise
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if attempt < attempts:
                    time.sleep(1.5 * (2 ** attempt))
                    continue
                raise

        raise RuntimeError(
            f"请求失败（重试 {attempts + 1} 次仍不通过）："
            f"url={url[:80]} status={last_status} exc={last_exc}"
        )

    # ---- Range 分块下载专用（urllib 直接实现，绕开 requests 大文件截断问题） ----
    def range_get(
        self,
        url: str,
        start: int,
        end: int,
        *,
        retries: int = 4,
        timeout: float = 120.0,
    ) -> tuple[int, Any, bytes]:
        """通过隧道下载 [start, end] 字节区间（闭区间），返回 (status, headers, body)。

        使用 urllib.request + ProxyHandler 直接发 GET + Range header，绕过 requests
        库在代理大文件下载时的不稳定性。IncompleteRead（隧道截断）统一按瞬时失败
        做指数退避重试。

        适配旧版 arxiv_topic_agentic/run.py 中 _range_get 的 proven 实现。
        """
        proxy = self.proxies.get("https") or self.proxies.get("http")
        handlers = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
        opener = urllib.request.build_opener(handlers)
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Accept-Encoding": "identity",
                "Range": f"bytes={start}-{end}",
            },
        )
        last: Exception | None = None
        for attempt in range(retries + 1):
            try:
                with opener.open(req, timeout=timeout) as resp:
                    body = resp.read()
                    return resp.status, resp.headers, body
            except Exception as e:  # noqa: BLE001 捕获 IncompleteRead / URLError 等
                last = e
                if attempt < retries:
                    time.sleep(1.5 * (2 ** attempt))
        raise RuntimeError(
            f"Range 下载失败 [{start}-{end}]：{type(last).__name__}: {last}"
        )


# ---------------------------------------------------------------- arXiv 检索
def _total_results(xml_text: str) -> int:
    """从 Atom XML 里解析 opensearch:totalResults。"""
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
        ns = {"op": "http://a9.com/-/spec/opensearch/1.1/"}
        el = root.find("op:totalResults", ns)
        return int(el.text) if el is not None and el.text else 0
    except Exception:  # noqa: BLE001
        return 0


def fetch_records(
    session: ProxySession,
    query: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    log_fn=None,
) -> list[dict]:
    """分页抓取 arXiv 检索结果。query 已经是完整检索式（含 submittedDate 窗）。"""
    records: list[dict] = []
    start = 0
    total = None
    while True:
        params = {
            "search_query": query,
            "start": start,
            "max_results": page_size,
            "sortBy": "submittedDate",
            "sortOrder": "ascending",
        }
        url = f"{ARXIV_API}?{urllib.parse.urlencode(params)}"
        if log_fn:
            log_fn(f"  检索 page start={start} page_size={page_size}")
        resp = session.request("GET", url, timeout=90.0)
        text = resp.text
        page = parse_feed(text.encode("utf-8"))
        records.extend(page)
        if total is None:
            total = _total_results(text)
            if log_fn:
                log_fn(f"  arXiv 显示总命中 {total}，已解析 {len(page)}")
        start += len(page)
        if not page or len(page) < page_size or (total is not None and start >= total):
            break
    return records


# ---------------------------------------------------------------- PDF 下载
def strip_version(source_id: str) -> str:
    """去掉版本号后缀 vN（如 2609.40360v1 → 2609.40360）。"""
    return re.sub(r"v\d+$", "", source_id)


def _pdf_complete(path: Path, expected_min_size: int = 1024) -> bool:
    """检查本地 PDF 文件是否完整。"""
    if not path.is_file():
        return False
    size = path.stat().st_size
    if size < expected_min_size:
        return False
    try:
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(-8, 2)
            tail = f.read()
        return head == b"%PDF" and b"%%EOF" in tail
    except OSError:
        return False


def _pdf_total_size(session: ProxySession, url: str) -> int:
    """用 Range: bytes=0-0 探测 PDF 总字节数。"""
    status, headers, _body = session.range_get(url, 0, 0)
    if status == 206:
        cr = headers.get("Content-Range") or ""
        m = re.search(r"/(\d+)\s*$", cr)
        if m:
            return int(m.group(1))
    raise RuntimeError(f"无法获取 PDF 大小（HTTP {status}）")


def _download_pdf_chunked(
    session: ProxySession,
    url: str,
    dest: Path,
    *,
    chunk_size: int = PDF_RANGE_CHUNK,
) -> None:
    """Range 分块下载：每块独立走隧道（换 IP），逐块拼接并校验总长度。

    适配旧版 arxiv_topic_agentic/run.py 中 _download_pdf_chunked 的 proven 实现。
    """
    total = _pdf_total_size(session, url)
    chunks: list[bytes] = []
    offset = 0
    while offset < total:
        end = min(offset + chunk_size - 1, total - 1)
        status, _headers, body = session.range_get(url, offset, end)
        if status not in (200, 206) or not body:
            raise RuntimeError(
                f"分块下载失败 HTTP {status}（[{offset}-{end}] 空响应）"
            )
        chunks.append(body)
        offset += len(body)
        if offset >= total:
            break
    data = b"".join(chunks)
    if len(data) != total or data[:4] != b"%PDF" or b"%%EOF" not in data:
        raise RuntimeError(
            f"分块拼接不完整（{len(data)}/{total} 字节，缺 %%EOF）"
        )
    dest.write_bytes(data)


def download_pdf(
    session: ProxySession,
    arxiv_id: str,
    dest: Path,
    *,
    log_fn=None,
) -> None:
    """下载单篇 PDF 到 dest。dest 必须是 .pdf 后缀。

    策略（与旧版 arxiv_topic_agentic/run.py 保持一致）：
    1. 先整文件下载（< 20MB 的论文几乎一次完成）
    2. 整文件失败/截断 → Range 分块下载（每块独立走隧道换 IP，规避大文件截断）
    3. 最终校验 %%PDF 头 + %%EOF 尾
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if _pdf_complete(dest):
        return

    url = f"https://arxiv.org/pdf/{arxiv_id}"

    # 清理旧的不完整文件
    dest.unlink(missing_ok=True)

    # 1) 先尝试整文件下载（走 requests，简单直接）
    try:
        resp = session.request(
            "GET", url, timeout=180.0, stream=False,
            max_retries=PDF_DOWNLOAD_RETRIES,
        )
        data = resp.content
        if data[:4] != b"%PDF":
            raise RuntimeError(f"下载内容非 PDF（{data[:16]!r}）")
        if b"%%EOF" in data:
            dest.write_bytes(data)
            return
        # PDF 头对但缺 EOF → 截断，改用 Range 分块
        if log_fn:
            log_fn(
                f"  {arxiv_id} 整文件截断（{len(data)}B，缺 EOF），改用 Range 分块"
            )
    except Exception as e:  # noqa: BLE001
        if log_fn:
            log_fn(f"  {arxiv_id} 整文件下载失败：{e}，改用 Range 分块")

    # 2) 整文件失败/截断 → Range 分块下载（urllib + ProxyHandler，proven 实现）
    dest.unlink(missing_ok=True)
    _download_pdf_chunked(session, url, dest)

    if not _pdf_complete(dest):
        raise RuntimeError(
            f"{arxiv_id} 分块拼接后仍不完整（{dest.stat().st_size}B）"
        )


def local_pdf_dir(topic: str, date_str: str) -> Path:
    """本地存储目录：/mnt/data01/data/{topic}/{yyyy-mm-dd}"""
    return Path(f"/mnt/data01/data/{topic}/{date_str}")


def already_downloaded(local_dir: Path, file_id: str) -> bool:
    """检查某篇论文 PDF 是否已下载到本地。"""
    p = local_dir / f"{file_id}.pdf"
    return _pdf_complete(p)
