from transform_service.status import JobStatusTracker


def test_status_tracker_basic_logs(monkeypatch):
    tr = JobStatusTracker(object(), "jid", {"status_table": "jt", "step_status_table": "st"})
    tr.start_job("n", 1, "h")
    tr.update_job_status("RUNNING")
    tr.start_step("s", [])
    tr.complete_step("s")
    tr.fail_step("s", "err")
    assert tr.get_completed_steps() == []
    assert tr.get_failed_steps() == []
