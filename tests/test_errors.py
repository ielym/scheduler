from sched import errors


def test_classify_exit_code_success():
    assert errors.classify_exit_code(0) == ("resource", "success")


def test_classify_preempted():
    assert errors.classify_exit_code(99) == ("unclassified", "preempted")


def test_classify_resource():
    cat, reason = errors.classify_exit_code(100)
    assert cat == "resource"
    assert reason == "exit_code_100"


def test_classify_sigkill_pending():
    cat, reason = errors.classify_exit_code(137)
    assert cat == "unclassified"
    assert "sigkill" in reason


def test_classify_g5_unclassified_fallback():
    # G5：无任务自报分类时，未知非 0 退出码（含通用 1）一律按 resource 重试
    cat, reason = errors.classify_exit_code(1)
    assert cat == "unclassified"
    assert "unclassified" in reason


def test_is_retryable():
    assert errors.is_retryable_category("resource")
    assert errors.is_retryable_category("unclassified")
    assert not errors.is_retryable_category("logic")