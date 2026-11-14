"""Fencing 测试：重复领取与旧计算恢复时，只有当前有效计算能发布结果和导出文件。"""
import json
from datetime import datetime, timedelta

from src.schemas.entities import RunInput
from src.services import run as service
from src.services import statistics
from src.utils import repository as repo
from src.utils.identity import now

from conftest import seed_inputs


def _stale_claim(run_id):
    row = repo.get("runs", run_id)
    row["claimed_at"] = (datetime.fromisoformat(now()) - timedelta(seconds=service.STALE_CLAIM_SECONDS + 5)).isoformat()
    repo.put("runs", row)


def test_late_success_cannot_overwrite_newer_completed_result(memory_repo, fake_redis, artifact_dir, monkeypatch):
    dictionary, dataset, study = seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]

    # 第一代 worker 认领后挂起（模拟计算很慢 / 进程被冻住）
    first, attempt1 = service.claim(rid)
    assert attempt1 == 1
    snap = first["snapshot"]
    first_result = statistics.analyze(snap["dataset"]["rows"], snap["dictionary"], snap["study"])

    # 心跳超时，第二代 worker 接管并完整跑完、发布结果与导出文件
    _stale_claim(rid)
    second = service.execute(rid)
    assert second["status"] == "completed" and second["attempt"] == 2
    artifact = artifact_dir / (rid + ".json")
    assert artifact.exists()
    on_disk = json.loads(artifact.read_text(encoding="utf-8"))

    # 第一代 worker 此刻才带着“成功结果”回来发布 —— 必须被拒绝
    def late_finish(r):
        r["result"] = first_result
        r["status"] = "completed"
        r["completed_at"] = now()
        return r
    published, ok = service._publish(rid, attempt1, late_finish)
    assert ok is False
    current = repo.get("runs", rid)
    assert current["attempt"] == 2 and current["status"] == "completed"
    # 迟到结果同样不能覆盖导出文件
    assert json.loads(artifact.read_text(encoding="utf-8"))["checksum"] == on_disk["checksum"]


def test_late_failure_cannot_fail_a_newer_run(memory_repo, fake_redis, artifact_dir):
    seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]

    _, attempt1 = service.claim(rid)
    _stale_claim(rid)
    done = service.execute(rid)
    assert done["status"] == "completed" and done["attempt"] == 2

    def late_fail(r):
        r["status"] = "failed"
        r["error"] = "旧worker迟到的失败"
        return r
    published, ok = service._publish(rid, attempt1, late_fail)
    assert ok is False
    current = repo.get("runs", rid)
    assert current["status"] == "completed" and not current.get("error")


def test_concurrent_claim_only_one_wins(memory_repo, fake_redis):
    seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]

    winners = []

    def worker():
        row, attempt = service.claim(rid)
        if attempt is not None:
            winners.append(attempt)

    import threading
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert winners == [1]
    assert repo.get("runs", rid)["attempt"] == 1


def test_heartbeat_is_rejected_for_stale_generation(memory_repo, fake_redis):
    seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]
    _, attempt1 = service.claim(rid)
    assert service.heartbeat(rid, attempt1) is True

    _stale_claim(rid)
    _, attempt2 = service.claim(rid)
    assert attempt2 == 2
    assert repo.get("runs", rid)["status"] == "recovering"
    # worker 接管后激活为处理态
    repo.compare_and_put(
        "runs", rid,
        lambda r: {**r, "status": "running", "heartbeat_at": now()},
        (("status", "recovering"), ("attempt", attempt2)))
    # 旧世代的迟到心跳不能续租新世代
    assert service.heartbeat(rid, attempt1) is False
    assert service.heartbeat(rid, attempt2) is True
