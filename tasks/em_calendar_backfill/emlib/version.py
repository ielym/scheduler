"""任务口径/落盘版本常量。

- 变更（表 schema / 字段 / 布局）即 +1；生产者据此重发作业（job_id 变化），
  完成标记亦随版本命名（`_meta/<key>.v{VERSION}.meta.json`）。
- **升级迁移**：新版本首跑前需清理 OSS 旧 `stock_data/calendar/` 对象（旧 schema 数据可随时由 K 线重建）。
"""
from __future__ import annotations

# v2: market=<key>/date=<D> 每日全量快照（列 date + is_trade_day）
# v3: + tz_local + sessions（当地/北京双墙钟）——本版
VERSION = "3"
