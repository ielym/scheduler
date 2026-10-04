"""自动生成的 run.py 薄封装：下载某一天的 arXiv 论文 PDF。"""
from __future__ import annotations
import os, sys
from pathlib import Path  # noqa: E402

_THIS_DIR = Path(__file__).resolve().parent
_TASKS_DIR = _THIS_DIR.parent
_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")

sys.path.insert(0, _SCHED_SRC)                      # sched_task_sdk
sys.path.insert(0, str(_TASKS_DIR))                 # tasks/  (shared.arxiv_fetcher)
sys.path.insert(0, str(_TASKS_DIR / "shared"))      # shared/ (arxiv_topic_base)

from arxiv_topic_base import run_date_fetcher  # noqa: E402

if __name__ == "__main__":
    sys.exit(run_date_fetcher())
