"""control 协议：退出码终结决策 + state.json control 段解析。"""

from __future__ import annotations

from datetime import datetime, timedelta

from sched import control as ctrl_mod
from sched import errors


class TestExitDecision:
    def test_exit_0_is_terminal_done(self):
        d = ctrl_mod.decide_from_exit(0, ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry=None)
        assert d.terminal is True
        assert d.action == "done"

    def test_exit_99_is_resume_same_intent(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_PREEMPTED,
                                       ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry=None)
        assert d.terminal is False
        assert d.action == "resume"

    def test_resource_error_back_to_pending(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_RESOURCE,
                                       ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry={"backoff_base_sec": 5})
        assert d.terminal is False
        assert d.action == "retry"
        assert d.not_before  # 有固定退避防热循环

    def test_resource_error_never_terminal(self):
        # 资源不足不是任务失败，永远回退 pending 等待，不终结
        d = ctrl_mod.decide_from_exit(errors.EXIT_EXTERNAL_RESOURCE,
                                       ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry={"backoff_base_sec": 5})
        assert d.terminal is False
        assert d.action == "retry"

    def test_stop_directive_overrides_exit_code(self):
        # 即使 exit_code=0，只要 stop directive 就终结
        d = ctrl_mod.decide_from_exit(0, ctrl_mod.ControlDirective(action="stop"),
                                       lifetime_retry=None)
        assert d.terminal is True
        assert d.action == "done"

    def test_no_retry_on_nonzero_exit_is_terminal(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_RESOURCE,
                                       ctrl_mod.ControlDirective(action="no_retry"),
                                       lifetime_retry=None)
        assert d.terminal is True
        assert d.action == "done"

    def test_reschedule_directive_sets_not_before(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_OK,
                                       ctrl_mod.ControlDirective(action="reschedule", delay_sec=120),
                                       lifetime_retry=None)
        assert d.terminal is False
        assert d.action == "skip"
        assert d.not_before
        now = datetime.now().astimezone()
        target = datetime.fromisoformat(d.not_before)
        assert abs((target - now - timedelta(seconds=120)).total_seconds()) < 5

    def test_data_risk_110_is_non_terminal(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_DATA_RISK,
                                       ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry=None)
        assert d.terminal is False
        assert d.action == "second_pass"

    def test_none_exit_code_default_terminal(self):
        d = ctrl_mod.decide_from_exit(None, ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry=None)
        assert d.terminal is True

    def test_unknown_nonzero_falls_back_to_resource_retry(self):
        d = ctrl_mod.decide_from_exit(42, ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry={"backoff_base_sec": 5})
        assert d.terminal is False
        assert d.action == "retry"  # 未分类按资源处理，回退 pending

    def test_exit_102_skips_schedule(self):
        d = ctrl_mod.decide_from_exit(errors.EXIT_SKIP_SCHEDULE,
                                       ctrl_mod.ControlDirective(action="none"),
                                       lifetime_retry=None)
        assert d.terminal is False
        assert d.action == "skip"
        assert d.not_before  # 有兜底延迟


class TestControlDirective:
    def test_reads_action_from_state(self):
        st = {"schema_version": 1, "current_step": "done",
              "control": {"action": "stop", "delay_sec": 0, "reason": "手动停止"}}
        d = ctrl_mod.read_control_directive(st)
        assert d.action == "stop"
        assert d.reason == "手动停止"

    def test_defaults_to_none_when_no_control(self):
        st = {"schema_version": 1, "current_step": "done"}
        d = ctrl_mod.read_control_directive(st)
        assert d.action == "none"

    def test_invalid_action_falls_back_to_none(self):
        st = {"schema_version": 1, "control": {"action": "garbage"}}
        d = ctrl_mod.read_control_directive(st)
        assert d.action == "none"

    def test_delay_sec_parsed_as_float(self):
        st = {"control": {"action": "reschedule", "delay_sec": "42.5"}}
        d = ctrl_mod.read_control_directive(st)
        assert d.action == "reschedule"
        assert d.delay_sec == 42.5

    def test_delay_sec_nonnegative(self):
        st = {"control": {"action": "reschedule", "delay_sec": -5}}
        d = ctrl_mod.read_control_directive(st)
        assert d.delay_sec == 0.0