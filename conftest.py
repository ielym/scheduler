"""pytest 根配置：把项目根加入 sys.path，便于 import sched_v002 / sched_task_sdk。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))