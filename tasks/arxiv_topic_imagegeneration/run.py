"""arXiv 主题论文每日采集（T-3）：检索 -> 经 IP 隧道下载 PDF -> 上传 OSS。

任务参数（每个任务一份独立副本，禁止跨任务共享）：
    TOPIC : 主题名（用于 OSS / 本地缓存目录）
    QUERY : arXiv search_query 主体（运行时统一追加 T-3 日期窗口）

契约设计：
- 检索日期在首次运行冻结进 resume_point["date"]；未完成跨天重试时日期不变。
- 去重唯一以 OSS 对象存在性为准：每轮按日期前缀批量列举已有对象；
  本地目录仅作下载暂存，上传成功即删，全部完成后清理当天暂存目录。
- 检索清单分页落 cache/manifests/<date>.json，被抢占后从未完成页继续。
- 单篇失败 skip_item 落 pending_skipped，本轮跑完以 110 请求二刷；
  二刷成功清账，404 等永久失败转 dead_letter；连续多篇失败判定代理/网络
  整体故障，101 快速退让交由调度器指数退避重试。
- 所有访问 arxiv 的网络请求（API 检索 / PDF 下载）均走 ip-tunnel 隧道
  （每次请求自动换 IP）；OSS 访问走 aliyun-oss CLI 的内网 endpoint。
- SCHED_DRY_RUN=1 时零真实网络/上传，产物只写 cache/dry/ 与 results/。
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timedelta, timezone
from pathlib import Path

import urllib.error
import urllib.parse
import urllib.request

# 复用已安装 CLI 的库（隧道 HTTP + arXiv Atom 解析；不 fork 子进程、不经 CLI 的 JSON 输出）
for _p in (
    os.environ.get("IP_TUNNEL_SRC", "/mnt/data01/projects/clihub/ip-tunnel/src"),
    os.environ.get("ARXIV_SRC", "/mnt/data01/projects/clihub/arxiv/src"),
):
    if _p and os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from ip_tunnel.http import build_proxy_url, http_request  # noqa: E402
from ip_tunnel.certification import get_credential  # noqa: E402
from arxiv.cli import parse_feed as parse_atom_feed  # noqa: E402
from sched_task_sdk import SchedTask, Step, run_task

# ===================== 任务参数（每份副本独立） =====================
TOPIC = "ImageGeneration"
QUERY = '(all:"image generation" OR all:"text-to-image" OR all:"image diffusion" OR all:"image synthesis" OR all:"text-to-image generation" OR all:"text-to-image diffusion" OR all:"flow matching" OR all:"diffusion transformer" OR all:"latent diffusion" OR all:"rectified flow" OR all:"generative adversarial")'
# ====================================================================

OSS_BUCKET = "ielym-data"
OSS_DIR_PREFIX = f"oss://{OSS_BUCKET}/academic/papers/{TOPIC}"
LOCAL_CACHE_ROOT = Path("/mnt/data01/data/papers_cache")
ARXIV_API = "https://export.arxiv.org/api/query"
PAGE_SIZE = 100          # arXiv API 单页条数（http_request 默认 gzip，线路上仅十几~几十KB，
                          # 远低于隧道截断阈值；条数大还能减少分页请求数）
MAX_TOTAL = 2000         # 单日单主题硬上限，超出视为检索式异常（LOGIC 转人工）
API_GAP_SEC = 3.0        # arXiv 官方建议页间约 3 秒
CONSEC_FAIL_LIMIT = 5    # 连续 N 篇失败判定隧道/目标站点整体故障 -> 101
PDF_CHUNK_SIZE = 2 * 1024 * 1024  # PDF Range 分块大小（每块独立隧道连接=换 IP）
PDF_FULL_DEADLINE_SEC = 60   # 整文件请求硬墙钟（秒）
PDF_RANGE_DEADLINE_SEC = 60  # 单个 Range 块硬墙钟（秒）
# 坏窗代理会「慢速 drip」（连接不断、零星给字节），socket 静默超时永不触发，
# 故下载必须再加一层墙钟兜底。
SHANGHAI_TZ = timezone(timedelta(hours=8))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
_VERSION_RE = re.compile(r"v\d+$")


def _hard_call(func, deadline: float, *args, **kwargs):
    """硬墙钟调用：墙钟到点即放弃等待并抛 FutureTimeout。

    被放弃的底层读线程无法强杀，但其 socket 自带静默超时（45/60s），
    最迟多活一个静默超时后自然消亡；shutdown(wait=False) 不阻塞主流程。
    """
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pdf-dl")
    future = pool.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=deadline)
    finally:
        pool.shutdown(wait=False)


class ResourceError(RuntimeError):
    """资源类错误（隧道不可用 / 限流 / 连接失败）：快速失败交调度器退让。"""


def strip_version(arxiv_id: str) -> str:
    return _VERSION_RE.sub("", arxiv_id.strip())


class ArxivTopicTask(SchedTask):
    def __init__(self) -> None:
        super().__init__()
        self.ws = Path(self.workspace)
        self.run_id = os.environ.get("SCHED_RUN_ID", "local")
        self.dry = self.dry_run
        self.root = self.ws / "cache" / "dry" if self.dry else self.ws / "cache"
        self.manifest_dir = self.root / "manifests"
        self.log_dir = self.ws / "cache" / "logs"
        for d in (self.manifest_dir, self.log_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.log = self._build_logger()
        self.local_day_dir = (
            self.root / "papers_cache" / TOPIC
            if self.dry
            else LOCAL_CACHE_ROOT / TOPIC
        )
        self._cred: dict | None = None

    # ---------- 基础设施 ----------

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"arxiv_{TOPIC}_{self.run_id}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh = logging.FileHandler(
            self.log_dir / f"run-{self.run_id}.log", encoding="utf-8"
        )
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
        logger.propagate = False
        return logger

    @staticmethod
    def _atomic_write_json(path: Path, payload) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(path)

    def _done_path(self) -> Path:
        return self.root / "done.json"

    def _load_done(self) -> dict:
        p = self._done_path()
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                self.log.warning("done.json 损坏，按空记录处理: %s", p)
        return {}

    def _t_minus_3(self) -> str:
        return (datetime.now(SHANGHAI_TZ).date() - timedelta(days=3)).isoformat()

    def _save_resume(self, date: str, phase: str, **extra) -> None:
        rp = {"date": date, "phase": phase}
        rp.update(extra)
        self.state.resume_point = rp
        self.state.flush()

    def _fail_resource(self, message: str, code: int = 101) -> None:
        self.log.error("资源错误，快速退让：%s", message)
        self.state.mark_failed("resource", message)
        self.state.flush()
        sys.exit(code)

    # ---------- 隧道代理 ----------

    @staticmethod
    def _oss_env() -> dict:
        """OSS 走内网 endpoint，显式剥除代理变量，禁止经隧道转发。"""
        banned = {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                  "http_proxy", "https_proxy", "all_proxy"}
        return {k: v for k, v in os.environ.copy().items() if k not in banned}

    def _ensure_cred(self) -> dict:
        """隧道凭据 dict：ip_tunnel 库凭此为每个请求组新代理 URL（新连接=换 IP）。"""
        if self._cred:
            return self._cred
        last_err = "unknown"
        for attempt in range(3):
            try:
                self._cred = get_credential()
                self.log.info("已获取隧道凭据（不落盘）：%s", self._cred.get("tunnel"))
                return self._cred
            except Exception as exc:
                last_err = f"{type(exc).__name__}: {exc}"
            self.log.warning("取隧道凭据失败(第%s次)：%s", attempt + 1, last_err)
            time.sleep(2 * (2 ** attempt))
        self._fail_resource(f"获取隧道凭据连续失败: {last_err}")
        return {}  # 不可达，仅为类型检查

    # ---------- arXiv 检索 ----------

    def _search_manifest(self, date: str):
        """分页检索，生成器（页是可抢占步）；返回记录列表。"""
        manifest_path = self.manifest_dir / f"{date}.json"
        records: list[dict] = []
        start = 0
        if manifest_path.exists():
            old = json.loads(manifest_path.read_text(encoding="utf-8"))
            if old.get("complete"):
                self.log.info("复用已完成检索清单：%s（%d 篇）", date, len(old["records"]))
                return old["records"]
            records = old.get("records", [])
            start = len(records)
            self.log.info("从断点续检 %s：已有 %d 篇", date, start)

        ymd = date.replace("-", "")
        full_query = (
            f"{QUERY} AND submittedDate:[{ymd}0000 TO {ymd}2359]"
        )
        while True:
            if start >= MAX_TOTAL:
                raise ValueError(
                    f"LOGIC: 命中数超过硬上限 {MAX_TOTAL}，疑似检索式异常: "
                    f"topic={TOPIC} date={date}"
                )
            yield Step("search_page", {"date": date, "start": start})
            params = {
                "search_query": full_query,
                "start": start,
                "max_results": PAGE_SIZE,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            }
            url = f"{ARXIV_API}?{urllib.parse.urlencode(params)}"
            # http_request 每请求新建隧道连接（换 IP）、默认 gzip 传输（线路体积小，
            # 实测 100 条的 Atom feed 压缩后仅十几~几十KB，远低于隧道截断阈值）；
            # 其内部已对部分瞬时错误重试，IncompleteRead / gzip 损坏等截断不在重试
            # 范围，由本层再做 5 次退避（跨度约 40s 撑过代理坏窗），仍失败才 101。
            page = None
            last_fetch_err = ""
            for fetch_attempt in range(5):
                res = http_request(
                    "GET", url, cred=self._ensure_cred(),
                    retries=2, timeout=90.0,
                )
                if res.get("ok"):
                    try:
                        page = parse_atom_feed(res["body"])
                        break
                    except Exception as exc:
                        last_fetch_err = f"arxiv 响应解析失败(start={start}): {exc}"
                else:
                    last_fetch_err = (
                        f"arxiv 抓取失败(start={start}): "
                        f"HTTP {res.get('status')} {str(res.get('reason'))[:120]}"
                    )
                if fetch_attempt < 4:
                    self.log.warning(
                        "arxiv 抓取瞬时失败(第%s/5次)，换新隧道连接重试：%s",
                        fetch_attempt + 1, last_fetch_err[:160],
                    )
                    time.sleep((4, 8, 12, 16)[fetch_attempt])
            if page is None:
                self._fail_resource(f"arxiv 连续 5 次抓取失败: {last_fetch_err}")

            records.extend(page)
            self._atomic_write_json(
                manifest_path, {"date": date, "complete": False, "records": records}
            )
            self._save_resume(
                date, "search", start=start + len(page), found=len(records)
            )
            self.log.info("检索 %s start=%d 本页 %d 篇 / 累计 %d 篇",
                          date, start, len(page), len(records))
            if len(page) < PAGE_SIZE:
                self._atomic_write_json(
                    manifest_path, {"date": date, "complete": True, "records": records}
                )
                break
            start += PAGE_SIZE
            time.sleep(API_GAP_SEC)
        return records

    def _dry_search(self, date: str):
        """dry 模式：两页假数据，保证信号能在中途命中。"""
        records = []
        for page in range(2):
            yield Step("search_page_dry", {"date": date, "page": page})
            time.sleep(0.3)
            records.extend([
                {
                    "source_id": f"dry{page}{i:02d}v1",
                    "pdf_url": f"https://arxiv.org/pdf/dry{page}{i:02d}v1",
                    "title": f"dry paper {page}-{i}",
                }
                for i in range(4)
            ])
            self._save_resume(date, "search", start=len(records), found=len(records))
            self.log.info("[DRY] 检索第 %d 页，累计 %d 篇", page + 1, len(records))
        return records

    # ---------- OSS ----------

    def _oss_prefix(self, date: str) -> str:
        return f"{OSS_DIR_PREFIX}/{date}/"

    def _list_oss_objects(self, date: str) -> set[str]:
        if self.dry:
            return set()
        prefix = self._oss_prefix(date)
        try:
            proc = subprocess.run(
                ["aliyun-oss", "ls", prefix, "-r", "--json"],
                capture_output=True, text=True, timeout=60, env=self._oss_env(),
            )
        except Exception as exc:
            self._fail_resource(f"OSS 列举进程异常: {exc}", code=100)
        if proc.returncode != 0:
            self._fail_resource(
                f"OSS 列举失败: {(proc.stderr or proc.stdout or '')[:200]}",
                code=100,
            )
        names: set[str] = set()
        try:
            envelope = json.loads(proc.stdout)
            for line in (envelope.get("data") or "").splitlines():
                parts = line.split()
                # ossutil 表格行：日期 ... oss://bucket/key（ObjectName 在末列）
                if not parts or not parts[-1].startswith("oss://"):
                    continue
                key = parts[-1]
                if not key.startswith(prefix):
                    continue
                name = key[len(prefix):]
                if name and not name.endswith("/"):
                    names.add(name)
        except Exception as exc:
            self._fail_resource(f"OSS 列举结果解析失败: {exc}", code=100)
        self.log.info("OSS %s 已有对象 %d 个", prefix, len(names))
        return names

    def _upload_pdf(self, local_file: Path, date: str) -> None:
        cmd = ["aliyun-oss", "upload", str(local_file), self._oss_prefix(date),
               "--json"]
        last = "unknown"
        for attempt in range(3):
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=180,
                    env=self._oss_env(),
                )
                envelope = json.loads(proc.stdout)
                if proc.returncode == 0 and envelope.get("ok"):
                    return
                last = str(envelope.get("error") or envelope)[:200]
            except Exception as exc:
                last = f"{type(exc).__name__}: {exc}"
            self.log.warning("OSS 上传 %s 失败(第%s次)：%s",
                             local_file.name, attempt + 1, last)
            time.sleep(2 * (2 ** attempt))
        raise ResourceError(f"OSS upload failed after retries: {last}")

    # ---------- PDF 下载 ----------

    def _pdf_range_get(self, url: str, start: int, end: int):
        """经隧道 Range 取 [start,end] 闭区间；每块新连接=换 IP，截断自动重试。"""
        proxy = build_proxy_url(self._ensure_cred())
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT, "Accept": "*/*",
            "Accept-Encoding": "identity", "Range": f"bytes={start}-{end}",
        })
        last: Exception | None = None

        def _do_open():
            with opener.open(req, timeout=60) as resp:
                return resp.status, resp.headers, resp.read()

        for attempt in range(4):
            try:
                return _hard_call(_do_open, PDF_RANGE_DEADLINE_SEC)
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return 404, exc.headers, b""
                last = exc
            except Exception as exc:
                last = exc
            if attempt < 3:
                self.log.warning("PDF Range[%d-%d] 瞬时失败(第%s次)：%s",
                                 start, end, attempt + 1, str(last)[:120])
                time.sleep(1.5 * (2 ** attempt))
        raise RuntimeError(f"Range 下载失败 [{start}-{end}]：{type(last).__name__}: {last}")

    def _download_pdf_chunked(self, url: str) -> bytes:
        """Range 分块下载拼接（2MB/块，各块独立换 IP），规避隧道大文件随机截断。"""
        status, headers, _ = self._pdf_range_get(url, 0, 0)
        if status == 404:
            raise FileNotFoundError("HTTP 404")
        m = re.search(r"/(\d+)\s*$", headers.get("Content-Range") or "")
        total = int(m.group(1)) if m else 0
        if status != 206 or not total:
            raise RuntimeError(f"无法探测 PDF 大小（HTTP {status}）")
        chunks, offset = [], 0
        while offset < total:
            end = min(offset + PDF_CHUNK_SIZE - 1, total - 1)
            status, _h, body = self._pdf_range_get(url, offset, end)
            if status not in (200, 206) or not body:
                raise RuntimeError(f"分块失败 HTTP {status}（[{offset}-{end}] 空响应）")
            chunks.append(body)
            offset += len(body)
        data = b"".join(chunks)
        if len(data) != total or b"%%EOF" not in data:
            raise RuntimeError(f"分块拼接不完整（{len(data)}/{total}，缺 %%EOF）")
        return data

    def _download_pdf(self, record: dict, dest: Path) -> tuple[bool, str]:
        """返回 (成功?, 原因)；404 原因以 fatal: 前缀表示永久失败。"""
        url = record.get("pdf_url") or (
            f"https://arxiv.org/pdf/{strip_version(record.get('source_id', ''))}"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        cred = self._ensure_cred()
        # 1) 整文件快路径：仅 1 次尝试、45s 静默超时。坏窗里代理常慢速 drip
        # （持续有零星字节、静默超时不触发），久拖无意义——快速失败转 Range
        # 分块（小块成功率高、总耗时可控）才是可靠路径。
        try:
            res = _hard_call(http_request, PDF_FULL_DEADLINE_SEC,
                             "GET", url, cred=cred, retries=0, timeout=45.0)
        except FutureTimeout:
            res = {"ok": False, "status": 0, "body": None}
        if res.get("status") == 404:
            return False, "fatal: HTTP 404"
        data = res["body"] if res.get("ok") else None
        if data is None or data[:4] != b"%PDF" or b"%%EOF" not in data:
            # 2) 整文件截断/失败 -> Range 分块兜底（每块换 IP）
            try:
                self.log.info("整文件未取到完整 PDF，转 Range 分块下载：%s", url.rsplit("/", 1)[-1])
                data = self._download_pdf_chunked(url)
            except FileNotFoundError:
                return False, "fatal: HTTP 404"
            except Exception as exc:
                return False, f"{type(exc).__name__}: {str(exc)[:200]}"
        if data[:4] != b"%PDF" or b"%%EOF" not in data:
            return False, "下载内容不是完整 PDF（缺 %PDF 头或 %%EOF）"
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(dest)
        return True, ""

    # ---------- pending_skipped 维护 ----------

    def _remove_pending(self, item: str) -> None:
        path = self.ws / "cache" / "pending_skipped.jsonl"
        if not path.exists():
            return
        kept = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and json.loads(line).get("item") != item
        ]
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(
            "\n".join(kept) + ("\n" if kept else ""), encoding="utf-8"
        )
        tmp.replace(path)

    def _record_map(self, records: list[dict]) -> dict[str, dict]:
        return {strip_version(r["source_id"]): r for r in records}

    # ---------- 完成处理 ----------

    def _finish_day(self, date: str, stats: dict) -> None:
        done = self._load_done()
        done[date] = {
            "finished_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
            **stats,
        }
        self._atomic_write_json(self._done_path(), done)
        latest_dir = (self.root / "results") if self.dry else (self.ws / "results")
        self._atomic_write_json(
            latest_dir / "latest.json", {"topic": TOPIC, "date": date, **stats}
        )
        # 本地仅暂存：全部上传确认后清理当天目录
        day_dir = self.local_day_dir / date
        if day_dir.exists():
            for child in day_dir.iterdir():
                if child.is_file():
                    child.unlink()
            try:
                day_dir.rmdir()
            except OSError:
                pass
        self.state.resume_point = {}
        self.state.progress = 100
        self.log.info("日期 %s 完成：%s", date, json.dumps(stats, ensure_ascii=False))

    # ---------- 单篇处理 ----------

    def _process_one(self, record: dict, date: str,
                     oss_objects: set[str], stats: dict) -> None:
        key = strip_version(record["source_id"])
        name = f"{key}.pdf"
        if name in oss_objects:
            stats["already_existed"] += 1
            return
        if self.dry:
            time.sleep(0.5)
            self.log.info("[DRY] 下载并上传 %s", name)
            return

        dest = self.local_day_dir / date / name
        ok, reason = self._download_pdf(record, dest)
        if not ok:
            if reason.startswith("fatal:"):
                stats["dead_letter"] += 1
                self.dead_letter(key, reason)
                self._remove_pending(key)
                self.log.error("永久失败转 dead_letter：%s (%s)", key, reason)
            else:
                stats["skipped"] += 1
                self.skip_item(key, f"download: {reason[:200]}")
                self.log.error("下载失败入二刷：%s (%s)", key, reason)
            tmp = dest.with_name(dest.name + ".tmp")
            if tmp.exists():
                tmp.unlink()
            return
        try:
            self._upload_pdf(dest, date)
        except Exception as exc:
            stats["skipped"] += 1
            self.skip_item(key, f"upload: {str(exc)[:200]}")
            self.log.error("上传失败入二刷：%s (%s)", key, exc)
            return
        finally:
            if dest.exists():
                dest.unlink()
        oss_objects.add(name)
        stats["uploaded"] += 1
        self.log.info("已上传 %s", f"{self._oss_prefix(date)}{name}")

    # ---------- 主流程 ----------

    def steps(self, resume: dict):
        if os.environ.get("SCHED_TRIGGER") == "second_pass":
            yield from self._steps_second_pass(resume)
        else:
            yield from self._steps_main(resume)

    def _steps_main(self, resume: dict):
        resume = resume or {}
        date = resume.get("date") or self._t_minus_3()

        # dry 各闸进程独立，禁止因冒烟留下的 dry done 标记短路后续闸/演练
        done = {} if self.dry else self._load_done()
        if date in done:
            self.log.info("日期 %s 已完成，当日不重跑", date)
            latest_dir = (self.root / "results") if self.dry else (self.ws / "results")
            self._atomic_write_json(
                latest_dir / "latest.json",
                {"topic": TOPIC, "date": date, "status": "already_done",
                 **{k: v for k, v in done[date].items() if k != "finished_at"}},
            )
            self.state.resume_point = {}
            return

        self._save_resume(date, "init")
        self.log.info("开始采集 topic=%s date=T-3=%s dry=%s", TOPIC, date, self.dry)
        yield Step("prepare", {"date": date, "topic": TOPIC})

        if self.dry:
            records = yield from self._dry_search(date)
        else:
            self._ensure_cred()
            records = yield from self._search_manifest(date)

        oss_objects = self._list_oss_objects(date)
        stats = {"papers_found": len(records), "uploaded": 0,
                 "already_existed": 0, "skipped": 0, "dead_letter": 0,
                 "ok": True}
        consec_fail = 0
        records_by_key = self._record_map(records)
        # dry 无 OSS 可对账，直接按断点 index 续跑以便验证续跑起点；
        # 正式模式仍从头迭代，但以 OSS 对象存在性为准跳过已上传论文（权威去重）。
        start_idx = 0
        if self.dry and resume.get("phase") == "papers":
            start_idx = int(resume.get("index", -1)) + 1
            if start_idx > 0:
                self.log.info("[DRY] 从断点 index=%d 续跑（共 %d 篇）",
                              start_idx, len(records))
        for idx, record in enumerate(records[start_idx:], start=start_idx):
            name = f"{strip_version(record.get('source_id', '') )}.pdf"
            yield Step("paper", {"id": name, "index": idx, "total": len(records)})
            before = stats["uploaded"] + stats["already_existed"]
            before_dead = stats["dead_letter"]
            self._process_one(record, date, oss_objects, stats)
            after = stats["uploaded"] + stats["already_existed"]
            after_dead = stats["dead_letter"]
            if self.dry:
                consec_fail = 0  # dry 模拟处理全部视为成功
            elif after > before or after_dead > before_dead:
                consec_fail = 0  # 上传成功 / 404 转死信，均不计代理故障
            else:
                consec_fail += 1
            self.state.progress = int((idx + 1) / max(len(records), 1) * 100)
            self._save_resume(
                date, "papers", last=name, index=idx, total=len(records),
                uploaded=stats["uploaded"], skipped=stats["skipped"],
            )
            if consec_fail >= CONSEC_FAIL_LIMIT:
                self._fail_resource(
                    f"连续 {consec_fail} 篇论文处理失败，疑似隧道/目标站点故障，"
                    f"已落 pending，退让后重跑 date={date}",
                    code=101,
                )

        pending = [
            rec for rec in self.read_pending_skipped()
            if rec.get("item") in records_by_key
        ]
        if pending:
            msg = f"{len(pending)} 篇待二刷 date={date}"
            self.log.warning(msg)
            self.state.mark_failed("data_risk", msg)
            self.state.flush()
            sys.exit(110)

        self._finish_day(date, stats)

    def _steps_second_pass(self, resume: dict):
        date = (resume or {}).get("date")
        if not date:
            raise ValueError(
                "LOGIC: second_pass 缺少 resume_point.date，无法定位二刷日期"
            )
        manifest_path = self.manifest_dir / f"{date}.json"
        if not manifest_path.exists():
            raise ValueError(f"LOGIC: second_pass 找不到检索清单 {manifest_path}")
        records = json.loads(manifest_path.read_text(encoding="utf-8"))["records"]
        records_by_key = self._record_map(records)
        pending = self.read_pending_skipped()
        self.log.info("二刷开始 date=%s 待补 %d 篇", date, len(pending))
        if not self.dry:
            self._ensure_cred()
        oss_objects = self._list_oss_objects(date)

        stats = {"papers_found": len(records), "uploaded": 0,
                 "already_existed": len(oss_objects), "skipped": 0,
                 "dead_letter": 0, "ok": True, "second_pass": True}
        consec_fail = 0
        for idx, rec in enumerate(pending):
            key = rec["item"]
            name = f"{key}.pdf"
            yield Step("retry_paper", {"id": name, "index": idx,
                                       "total": len(pending)})
            if name in oss_objects:
                self._remove_pending(key)
                continue
            record = records_by_key.get(key, {"source_id": key})
            before = stats["uploaded"]
            self._process_one(record, date, oss_objects, stats)
            if stats["uploaded"] > before or name in oss_objects:
                consec_fail = 0
            else:
                consec_fail += 1
            self._save_resume(
                date, "second_pass", last=name, index=idx,
                total=len(pending),
            )
            if consec_fail >= CONSEC_FAIL_LIMIT:
                self._fail_resource(
                    f"二刷连续 {consec_fail} 篇失败，疑似隧道故障 date={date}",
                    code=101,
                )

        remaining = self.read_pending_skipped()
        if remaining:
            msg = f"二刷后仍有 {len(remaining)} 篇失败 date={date}"
            self.log.warning(msg)
            self.state.mark_failed("data_risk", msg)
            self.state.flush()
            sys.exit(110)

        stats["uploaded"] = max(stats["uploaded"], 0)
        self._finish_day(date, stats)


if __name__ == "__main__":
    sys.exit(run_task(ArxivTopicTask()))
