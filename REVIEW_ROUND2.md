# 任务调度重构 · 需求实现综合审查报告（第二轮）

> 审查对象：`/mnt/data01/projects/scheduler_v002`（sched_v002 核心包 + sched_task_sdk + 示例任务 + 测试套件，HEAD=14a4e80）
> 需求基线：`/mnt/data01/projects/todo_list/1003/任务调度重构/results/任务调度重构_需求说明.md`（R1–R24）
> 审查方式：全量代码通读 + 逐条需求对照 + 4 组针对性实验实证 + 测试套件运行（81 passed）
> 审查日期：2026-10-03
> 与上轮关系：第一轮报告（REVIEW.md）所列 5 个 P0、8 个 P1 已全部修复并有回归测试；本报告为**修复后代码的独立复审**，结论基于当前 HEAD 重新取证。

---

## 0. 结论摘要

第一轮审查发现的关键缺陷（once 无限重触发、准出 over-commit、画像单位污染、预估无钳制、抢占缺口误算）经代码与回归测试双重核实**均已正确修复**；G5 错误归因链、崩溃恢复、渐进式提交、max_instances、110 二刷、软资源降额、窗口收口、manual 心跳等 P1 项亦已落地。测试套件从 56 项增至 81 项并全部通过。

**但本轮批判性复审仍发现 1 个 P0 级缺陷（抢占优先级反转，实验实证，直接违反最高优先级需求 R1）、6 个 P1 级需求偏差、若干 P2 风险，且 R23（推送远端 git）至今未落实。**

| 级别 | 数量 | 概述 |
|---|---|---|
| P0 关键缺陷（已实证） | 1 | 抢占选择无优先级下限：低优等待任务可抢占高优运行任务（优先级反转） |
| P1 需求偏差 | 6 | G5 兜底残留断裂、退避任务占位阻塞准出、同 job 多实例状态串扰、非调度进程占用无防护、注册期闸门无降权+env 注入、stop() 双 finalize 竞态 |
| P2 风险/死代码 | 8 | manual 豁免超限退让、饿死看门狗提权越界、stop 时退出码 -15 误判 logic、死配置字段 ×5、账本实测双校验未接线、run_id 碰撞等 |
| 流程违规 | 1 | R23：本地已有 3 个 commit，但**无远端、从未推送** |

**总体判定：不满足"完成标准"。R1（最高优先级）存在实证反例，须按第 4 章修复后复审。**

---

## 1. 需求实现验证矩阵（R1–R24）

图例：✅ 满足 ⚠️ 部分满足/有偏差 ❌ 未满足

| 需求 | 判定 | 验证结论与证据 |
|---|---|---|
| **R1** 高优任务占据全部资源并执行完毕（最高优先级） | ❌ | 缺口式抢占已修复（[service.py#L412-L431](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L412-L431)），但 `select_preempt_targets` **未以等待者优先级为下限**：等待 P=50 的任务可抢占运行中 P=90 的任务（实验 1 实证，见 3.1-N1）。且 [test_preempt.py#L11-L21](file:///mnt/data01/projects/scheduler_v002/tests/test_preempt.py#L11-L21) 把"低优不够就抢高优"固化为预期行为，方向性错误 |
| **R2** 可抢占性一律由优先级决定 | ⚠️ | 无用户级"不可抢占"开关 ✅；但 manual/contract_exempt 整体豁免抢占（[preempt.py#L46](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L46)）：contract_exempt 豁免是 R3 的必要推论可接受，manual 豁免缺乏需求依据；叠加 N1 后"优先级决定"原则双向失真 |
| **R3** 不可恢复任务仅手动触发、禁止自动 | ✅ | schema 模型校验 + 闸1 复核 + refresh 从触发引擎摘除（[service.py#L116-L127](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L116-L127)）+ 崩溃恢复不自动重入队，四重防护。注：需求补充条款"禁止人工触发"与 R3 主文"手动触发"存在字面矛盾，实现依 R3 主文（CLI `run` 仅入队不直跑，符合"运行只能由调度器决定"） |
| **R4** 资源评估防打爆 | ⚠️ | 账本虚拟占用已防同轮 over-commit（回归测试实证）；但**准出不感知非调度进程占用**：`Capacity.available_memory_mb()`（MemAvailable）定义后无任何调用方，`can_admit` 只看账本（[admission.py#L64-L95](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L64-L95)）。外部进程吃掉内存时照样准出至整机上限 → 打爆风险仍在（见 3.2-N5） |
| **R5** 冒烟/历史评估资源 | ✅ | 三级画像 + P95 滑窗 + 平均核数修正（[service.py#L517-L526](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L517-L526)），回归测试 `test_finalize_records_avg_cpu_cores` 锁定 |
| **R6** 创建时强制冒烟预估 | ✅ | 注册必经闸3（[contract.py#L133-L146](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L133-L146)）。瑕疵：`smoke.sample_size` 字段声明未使用 |
| **R7** 超机器上限拒绝加入 | ✅ | 闸4 对声明值与冒烟实测双重硬拒（[contract.py#L148-L162](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L148-L162)）；预估钳制保证"合法准入者永不饿死" |
| **R8** 未超上限但被占 → 允许加入待释放 | ✅ | WQ 持久化等待 + 每 tick 重扫 |
| **R9** 硬资源硬门禁/软资源不纳入拒绝 | ✅ | 内存/CPU 账本硬判，软资源令牌桶；注册时软资源不拒 |
| **R10** 同优先级排队等待不判失效 | ⚠️ | 双轨排序 + 饿死看门狗 ✅；但 N1 实证**同优先级运行任务可被同优先级等待任务抢占**（实验 2），与"同优先级最低限度排队"冲突；饿死提权至 10000 超出 priority 值域 0~100，见 3.3 |
| **R11** 软资源合理分配防超限 | ✅ | 令牌桶 + 错误率降额/恢复闭环已接线（[service.py#L283-L301](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L283-L301)、[L528-L531](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L528-L531)），回归测试覆盖。瑕疵：声明了未配置容量的软资源的任务将永久等待（不判失效，符合 R10，但无任何告警通道） |
| **R12** 不得同时刻同时提交；禁止创建即运行 | ⚠️ | 注册不自动运行 ✅；once 到期摘除 ✅；max_instances/overflow 已生效 ✅。字面偏差：资源充足时同一 tick 仍可准出多个任务（虚拟账本保证其安全，属"字面未守、风险已控"，见 3.2-N3 附带讨论） |
| **R13** 渐进式动态提交 | ⚠️ | 冷启动且有固定资源阻塞时"一次只准出 1 项 + 健康观察期"已实现并有回归测试；但仅在 `blocked_fixed 非空且 running==0` 时生效——资源充足场景无渐进确认链；观察期内"第二个任务严重资源不足及时停止"依赖 monitor 超限退让（间接成立） |
| **R14** 固定资源不足先等待、调度其他任务 | ⚠️ | 扫描跳过被阻者继续准出 ✅；但"被非调度任务占用"场景无防护（同 R4/N5） |
| **R15** 不可控资源失败等待自然完毕不强杀 | ⚠️ | 101 走退避重试不终止本体 ✅；但 `MAX_ATTEMPTS=5` 后 giveup 转人工，任务永久离开自动调度轨道，与 R17"仍须最终完成"存在张力 |
| **R16** 不因排队过长终止任务 | ✅ | 无排队超时；画像钳制后"永久无法准出"路径已消除 |
| **R17** 退让而非扑杀，任务仍须最终完成 | ⚠️ | 抢占=优雅 SIGTERM+断点 ✅；崩溃恢复重入队 ✅；但 giveup（R15）与数据风险 3 轮上限（[service.py#L581-L588](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L581-L588)）均以"转人工"终局，需求"必须最终完成"仅靠人工兜底 |
| **R18** 区分逻辑/资源错误；统一错误码上报 | ⚠️ | state.json `last_error.category` 精确通道已接通（[runner.py#L234](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L234)、[L260-L266](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L260-L266)）✅；但**无自报时退出码 1 兜底仍判 logic**（实验 3 实证），兜底语义与 G5 相反（见 3.2-N2） |
| **R19** 未设置/设置不正确一律按资源错误 | ⚠️ | SDK 路径（unclassified→resource）✅ 有回归测试；非 SDK 路径（任务写不出 state 或绕开 SDK）：`classify_exit_code(1)=logic → failed 转人工`，与 R19 相反 |
| **R20** 硬性恢复能力（重试/二刷） | ⚠️ | 闸5 断点验证 + 崩溃恢复重入队 + 110 二刷 ✅；但**同 job 多实例共享 `cache/state.json`**：`max_instances>1` 时并发实例的 resume_point/心跳/last_error 互相覆盖（见 3.2-N4），恢复能力在并发场景失真 |
| **R21** 单条数据异常不卡整体 | ✅ | `skip_item`/`dead_letter` + 110 二刷闭环（上限 3 轮转人工）。瑕疵：二刷重放 `pending_skipped.jsonl` 依赖任务作者自觉，SDK 未提供自动消费 |
| **R22** 代码层强制约束 | ⚠️ | 行为级契约闸较强；但仍无"强制继承固定基类"静态检查——手写伪造 state.json 的脚本可过闸（行为约束等价但强度低于需求表述） |
| **R23** 修改前推送远端 git | ❌ | 本地仓库已有 3 个 commit（基线/修复/测试），但 `git remote -v` 为空，**从未推送远端**，误改/磁盘故障仍不可回退 |
| **R24** 自主执行协议 | ⚠️ | 监督循环异常自愈 ✅、状态原子持久化+决策审计 ✅、崩溃恢复分级 ✅；"完成后自检"无独立证据 |

**统计：✅ 9 项 / ⚠️ 13 项 / ❌ 2 项（R1、R23）。**

---

## 2. 测试现状评估

- 81 项全部通过（19.8s）。新增回归测试有效锁定 Bug1–5 与 D1–8，端到端抢占测试（`test_end_to_end_preempt_high_wins`）质量较高。
- **本轮新发现的关键盲区**：
  1. 无"等待者优先级 ≤ 运行者优先级不得抢占"用例——`test_select_min_priority_first` 反而把优先级反转固化为预期；
  2. 无"非 SDK/无 state 的任务退出码 1"兜底归因用例（现有 G5 测试都走 SDK，state 通道必然存在）；
  3. 无"退避期任务在 WQ 中参与准出扫描"用例；
  4. 无 `max_instances>1` 并发实例的状态隔离用例；
  5. 无 `stop()` 优雅停机的行为用例（双 finalize、-15 误判均未暴露）；
  6. 无"外部进程占内存"的准出抑制用例；
  7. 契约闸/冒烟闸仍零降权、零 env 黑名单覆盖。

---

## 3. 问题清单（本轮新发现，均已取证）

### 3.1 P0 关键缺陷

**N1｜抢占选择无优先级下限 → 优先级反转（违反 R1/R2/R10，已实验实证）**
- 位置：[preempt.py#L36-L69](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L36-L69)、调用处 [service.py#L399-L431](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L399-L431)
- 机理：`select_preempt_targets` 按 priority 升序贪心选取，**从不比较候选者与等待者的优先级**。`_tick_preempt` 也未传入等待者 priority 作为过滤条件。
- 实证（本轮实验 1/2）：
  - 运行任务 P=90 占 4 核，等待任务 P=50 需 2 核 → `select_preempt_targets` 返回 `[P=90 任务]`：**低优等待者抢占了高优运行者**，R1"高优必须执行完毕"被直接击穿；
  - 同优先级（P=50 vs P=50）同样互抢，违反 R10"同优先级最低限度排队等待"。
- 加剧因素：[test_preempt.py#L11-L21](file:///mnt/data01/projects/scheduler_v002/tests/test_preempt.py#L11-L21) 断言"低优不够就继续抢 high(90)"，把错误行为写进了测试基线，修复时必须同步改写。
- 次生问题：manual/contract_exempt 任务自身豁免抢占，却可作为等待方**触发**他人被抢；高优任务到达时无法反抢 manual 任务 → 高优被低优 manual 任务无限阻滞，与 R1 张力。

### 3.2 P1 需求偏差

**N2｜G5 兜底残留断裂（R19 部分未满足，已实验实证）**
- SDK 路径已修复（unclassified→resource），但任务未写出 state（进程崩于 SDK 初始化前、非 SDK 脚本过闸后运行期失败）时走退出码兜底：`classify_exit_code(1)=logic` → `failed` 转人工（实验 3）。R19 要求"未设置或设置不正确一律按资源错误"，兜底分支语义相反。
- 修复取向见 4.2-F2（把默认兜底改为 unclassified→resource，宁可多重试不可错杀——这正是 G5 的本意）。

**N3｜退避/冷却期任务参与准出扫描，占位阻塞他人（R14/R16 效率缺陷）**
- 位置：[service.py#L240-L281](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L240-L281) vs [L303-L312](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L303-L312)
- 机理：`_handle_retry` 重入队的任务在 backoff 期内留在 WQ；`admission_scan` 不感知 backoff，若其排前（高优或饿死提权）会被扫描准出并计入本轮虚拟账本，随后 `_admit` 发现退避期又放回。后果：退避任务每 tick 空转 pop/enqueue，且其虚拟占用会把本可准出的后续任务挡在当轮之外——最长可延迟他人 1 小时（退避上限）。不构成终止，但属调度正确性缺陷。

**N4｜同 job 多实例共享 `cache/state.json`（R20/R21 数据完整性）**
- 位置：[state.py#L23-L25](file:///mnt/data01/projects/scheduler_v002/sched_task_sdk/state.py#L23-L25)、[runner.py#L304-L322](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L304-L322)
- `max_instances` 允许至 20，但同一 job 的所有并发实例读写同一个 `task_dir/cache/state.json`：resume_point 互相覆盖（A 实例从 B 的断点续跑）、心跳 mtime 互相续命（卡死实例被另一实例的心跳掩盖）、`last_error.category` 张冠李戴（B 的错误分类作用于 A 的终态判定）。schema 默认 `max_instances=1` 缓解了暴露面，但一旦调大即踩雷。

**N5｜非调度进程占用不纳入准出（R4/R14 场景缺口）**
- `Capacity.available_memory_mb()`（MemAvailable）与 `book_available_memory_mb()` 定义后无调用方；`Ledger.set_actual` 同样无调用方，resources.py 宣称的"账本值/实测值双校验"未实现。外部进程（非调度任务）吃掉内存时，`can_admit` 仍按整机总量准出 → 实际 OOM。cgroup `memory.max` 能保护单个任务不超额，但保护不了**整机**被打爆（多个任务合计 ≤ 总量却仍超出实际可用）。

**N6｜注册期闸门无降权 + env 注入（安全，S1/S2 叠加放大）**
- [contract.py#L53-L57](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L53-L57) `_spawn_bounded` 无 `preexec_fn`：闸2/3/5 以**调度器自身用户（生产上常为 root）**执行待审任务代码。
- 实验 4 实证：`task.json.env` 可注入 `PATH=/evil`、`LD_PRELOAD=/x.so` 并完整透传；叠加 `build_env` 全量继承 `os.environ`（[runner.py#L46](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L46)），形成"注册即提权 + 机密泄露"双通道。
- 上轮 S1（`_drop_privileges` 静默失败）未修复：用户不存在时直接 return，任务以调度器用户运行而无人感知。

**N7｜`stop()` 双 finalize 竞态 + 退出码 -15 误判（终态一致性）**
- [service.py#L99-L113](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L99-L113)：stop 先 `cancel.set()` 再直接 kill 进程组并 `_finalize(INTERRUPTED)`；worker 线程的 `runner.execute` 随后也会 `_finalize`。两路并发 → runlog 出现同一 run 的矛盾双终态记录（如 failed+interrupted），账本/软资源重复释放（有钳制未出事故）。
- 更隐蔽的是：SIGTERM 后进程先于 cancel 检查退出 → `exit_code=-15` → 落入通用兜底 → 判 `failed`(logic) → `_after_finalize` 记 terminal 转人工。**优雅停机的任务可能被打上"逻辑错误"烙印**（若 stop 的 finalize 后于 worker 的 finalize，还会漏掉 INTERRUPTED 重入队之外的正确语义）。

### 3.3 P2 风险与死代码

1. **manual 任务豁免超限退让**（[monitor.py#L58-L59](file:///mnt/data01/projects/scheduler_v002/sched_v002/monitor.py#L58-L59)）：manual 任务实测超预估多少倍都不会被退让，可持续超占内存，与 R4 张力（心跳放宽 ×2 已修，超限豁免仍在）。
2. **饿死看门狗提权越界**（[admission.py#L36-L41](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L36-L41)）：starved 任务 pri 提至 10000，远超值域 0~100，排扫描最前吞噬资源 → 诱发对高优任务的抢占抖动（叠加 N1 更危险）。建议提权幅度不超过 `max(priority)+1` 或以次优先级队列实现。
3. **死配置/死字段**：`smoke.sample_size`、`retention_days`、`max_log_mb`、`diff_check`、`task.json.runtime.user`（_admit 只用 settings.run_user）、`can_admit.hard_mem_reserve_ratio`、`Ledger.set_actual`、`runner.TRUNCATE_HEAD` 均为零调用死代码——配置与行为脱节误导使用者。
4. **run_id 熵不足**：`YYYYMMDDHHMMSS + 4 位 hex`（16bit），同秒双 run 碰撞概率 1/65536，run_id 同时是日志与 cgroup 名，碰撞即串数据。
5. **闸5 信号时机硬编码**：`signal_after_sec=1.5` 对重导入任务（torch 等）可能 SIGTERM 打在处理器安装前 → exit -15 → 误拒合法任务。
6. **once 触发记录误标**：`_tick_triggers` 对 once 任务也写 `trigger="interval"`（[service.py#L227](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L227)），审计语义失真。
7. **线程安全**：`_tick_preempt`（[service.py#L420](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L420)）与 `_close_expired_windows`（[L811](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L811)）无锁遍历 `self.running`，worker 线程 `_finalize` 并发 pop 可致 `RuntimeError: dictionary changed size during iteration`——被 `_loop` 兜底捕获但丢整轮 tick（触发/准出/抢占/监测全部顺延一秒）。
8. **上轮遗留仍未修**：`_pid_belongs_to` cmdline 子串匹配（误杀风险）、锁文件 0644 且永不清理、`load_settings` 解析失败静默重置。
9. **HTTP 预留配置**：`0.0.0.0 + auth 默认关闭` 仍在 config 中，当前无服务实现，属火种。

---

## 4. 修复方案

### 4.1 P0 修复（代码级）

**F1｜抢占必须"以下犯上"禁止（修 N1）**

```python
# preempt.py：增加 waiter_priority 下限过滤
def select_preempt_targets(running, need_cpu, need_mem_mb, need_soft,
                           waiter_priority: int) -> list[RunningInfo]:
    candidates = [r for r in running
                  if not r.manual and not r.contract_exempt
                  and r.priority < waiter_priority]   # 严格低于等待者
    ...

# service.py _tick_preempt：
targets = preempt.select_preempt_targets(running_infos, gap_cpu, gap_mem, gap_soft,
                                         waiter_priority=int(top.get("priority") or 0))
```

- 同步改写 `test_select_min_priority_first`：高优候选必须被排除，缺口不足时返回 `[]`（等待者继续排队——这正是 R10/R17 退让语义）。
- 新增用例：低优等待 vs 高优运行 → 不抢占；同优先级 → 不抢占；高优等待 vs 多个低优运行 → 按升序逐个点名直到缺口补足。

### 4.2 P1 修复

**F2｜G5 兜底语义反转（修 N2）**：`classify_exit_code` 默认分支（非保留非 0）由 `LOGIC` 改为 `UNCLASSIFIED`（is_retryable 已为 True）；逻辑错误的唯一权威通道是任务自报 `last_error.category=logic` 或显式 `fail(..., category=logic())`。这正是 R19"宁错杀为重试、倒逼正确分类"的本意。注意同步更新 `EXIT_LOGIC=1` 的注释语义。

**F3｜退避/冷却任务移出准出扫描（修 N3）**：

```python
def _tick_admission(self):
    with self._lock:
        items = [it for it in self.state.wq
                 if not self._in_cooldown(it["job_id"]) and not self._in_backoff(it["job_id"])]
```

**F4｜多实例状态隔离（修 N4）**：SDK `StateStore` 路径改为 `cache/runs/<SCHED_RUN_ID>/state.json`（心跳/last_error 按实例隔离），resume_point 提升为 job 级 `cache/resume_point.json`（scheduler 与 monitor 各读其所）；schema 层面对 `max_instances>1` 的任务在闸1 增加"并发实例 resume 语义需任务自证"的显式确认字段，或保守起见闸1 直接拒绝 `max_instances>1` 直至隔离落地。

**F5｜外部占用感知（修 N5）**：`can_admit` 增加第二道判定 `est_mem_mb > 0 and est_mem_mb > capacity.available_memory_mb() - safety_margin` → 判固定资源不足等待（退让语义，不拒绝）；`safety_margin` 进 ResourceConfig（如 512MB）。同时把 monitor 的实测采样接到 `ledger.set_actual`，让"账本/实测双校验"名实相符，status 输出可用于对账。

**F6｜闸门降权 + env 黑名单（修 N6）**：
1. `_spawn_bounded` 增加 `preexec_fn=lambda: runner._drop_privileges(settings.run_user)`；
2. `_drop_privileges` 失败（用户不存在/setuid OSError）改为抛异常 → 该 run/该闸判失败，绝不静默提权；
3. `build_env` 白名单化：`PATH/HOME/LANG/TZ/PYTHONPATH` + SCHED_* + task env，且 task env 命中黑名单（`LD_PRELOAD/LD_LIBRARY_PATH/LD_AUDIT/PYTHONSTARTUP/PATH/BASH_ENV/ENV`）即注册校验失败。

**F7｜stop 竞态与 -15 归因（修 N7）**：
1. `stop()` 只 `cancel.set()` 并 join worker 线程（超时再强杀），**不直接 _finalize**——终态收敛唯一定点在 worker；
2. `runner.execute` 循环改为先查 cancel/preempt 再查 poll（或在 kind=="exited" 且 `spec.cancel.is_set()/spec.preempt.is_set()` 时按信号语义归因，覆盖 rc 先到的竞态）。

### 4.3 P2 与流程修复

1. manual 任务超限退让：与 manual 心跳同款"放宽不豁免"——阈值 ×2 但保留退让；
2. 饿死提权幅度钳制在值域内（如 `pri = max(all_priorities)+1`，上限 101）；
3. 删除或接线全部死配置（sample_size 用于多次冒烟取峰值、retention_days 接 jsonl 清理、runtime.user 覆盖 settings.run_user 或删字段）；
4. run_id 熵扩至 `token_hex(4)` 以上或加 job 前缀；
5. 闸5 `signal_after_sec` 进 SmokeSpec 可配；
6. once 触发写 `trigger="once"`；
7. `_tick_preempt`/`_close_expired_windows` 遍历前 `with self._lock: running = list(...)`；
8. `_pid_belongs_to` 增加 `/proc/<pid>` 启动时间与 active 记录 started_at 的一致性校验；
9. **R23（前置）**：`git remote add origin <url> && git push -u origin main`，并在此后每个修复独立提交推送。

### 4.4 测试策略优化（新增用例 → 覆盖缺陷映射）

| 新增测试 | 覆盖 |
|---|---|
| 低优等待不得抢占高优运行；同优先级不互抢（改写 test_select_min_priority_first） | N1 |
| 无 state.json 的裸脚本退出码 1 → resource 重试 | N2 |
| 退避期任务不进入 admission_scan、不挡后续任务 | N3 |
| max_instances=2 并发实例的 state 隔离（或闸1 拒绝用例） | N4 |
| MemAvailable 被人为压低时准出抑制、恢复后准出 | N5 |
| 注册闸以非 root 执行 + env 黑名单命中即拒 | N6 |
| stop() 后 runlog 单终态、WQ 恢复语义唯一 | N7 |
| 饿死提权后高优仍先准出（提权不越界） | P2-2 |

另建议保留上轮建议：CI 双环境（root+cgroup / 非 root+无 cgroup）各跑一遍。

---

## 5. 风险评估报告

| 风险 | 等级 | 触发条件 | 影响 | 缓解状态 |
|---|---|---|---|---|
| 优先级反转抢占 | **高** | 低优任务资源不足排队时 | 高优任务被 SIGTERM 打断，R1 最高优先级失效；频繁反转可致双方抖动 | 未缓解，须 F1 |
| 注册期 root 执行+env 注入 | **高** | 注册任何第三方任务目录 | 提权执行任意代码、窃取调度器环境机密 | 未缓解，须 F6 |
| 外部进程占内存照准出 | **高** | 机器上有非调度大户 | 整机 OOM，防打爆承诺破窗 | 未缓解，须 F5 |
| 多实例状态串扰 | 中 | max_instances>1 | 断点错乱、错误归因张冠李戴 | 默认 max_instances=1 部分缓解，须 F4 |
| 退避任务阻塞准出 | 中 | 任何资源错误重试期间 | 同机其他任务被无谓延迟最长 1h | 未缓解，须 F3 |
| 停机双终态/-15 误判 | 中 | 每次优雅停机 | 历史记录自相矛盾、任务被错标逻辑错误 | 未缓解，须 F7 |
| 无远端 git 备份 | 中 | 误改/磁盘故障 | 两轮成果不可恢复 | **须立即执行 4.3-9** |
| G5 兜底错杀 | 中 | 任务崩于 SDK 初始化前 | 可重试故障被转人工 | 未缓解，须 F2 |
| manual 超限无退让 | 低 | manual 任务内存失控 | 单机资源被手动任务挤占 | 心跳监测部分缓解 |
| 死配置误导 | 低 | 使用者按文档配置 | 配置不生效且无任何告警 | 须 4.3-3 |

---

## 6. 复审建议

1. 先执行 R23 补救（建仓推送远端），再按 F1→F6→F5 顺序修复（P0/安全优先）；
2. 每个 Fix 独立提交并附映射回归测试（4.4 表）；
3. 完成后重跑：本报告 3.1 实验（优先级反转必须返回空集）+ 全套件 + 端到端抢占/停机/崩溃恢复三场景；
4. 以第 1 章矩阵为复审清单，R1/R23 转 ✅、其余 ⚠️ 逐项附书面豁免理由后方可宣告完成。
