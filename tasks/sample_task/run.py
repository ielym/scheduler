"""示例任务：演示 sched_task_sdk 托管断点/心跳/抢占退出(99)/错误分类。

执行模型：
- 3 段处理单元（chunk），每段 yield 一次 Step，再 sleep 模拟耗时工作；
- 每完成一段，resume_point 记为 {"index": i+1} 并 flush（断点即落盘）；
- 收到 SIGTERM：只需置 _stop_requested，不打断当前段；主循环在下一段开始前
  清理退出 99（干净抢占语义），下次恢复从 resume_point 继续；
- dry_run 下每段 sleep 1.0s（闸5 断点测试需 SIGTERM 打在段中）；真实运行更快。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from sched_task_sdk import SchedTask, Step, run_task

TOTAL = 3


class SampleTask(SchedTask):
    def steps(self, resume):
        start = int((resume or {}).get("index", 0))
        for i in range(start, TOTAL):
            yield Step("process", {"index": i})
            # 模拟耗时工作；dry_run 稍慢以保证闸5 的 SIGTERM 命中段中
            time.sleep(1.0 if self.dry_run else 0.3)
            self.state.resume_point = {"index": i + 1}
            self.state.flush()

        # 全部完成：写交付物（outputs.expect 校验的目标）
        out = Path(self.workspace) / "results"
        out.mkdir(parents=True, exist_ok=True)
        (out / "final.json").write_text(
            json.dumps({"processed": TOTAL, "chunks": list(range(TOTAL))}, ensure_ascii=False),
            encoding="utf-8",
        )
        # 生成器正常结束，run() 会在循环结束后 mark_success


if __name__ == "__main__":
    sys.exit(run_task(SampleTask()))