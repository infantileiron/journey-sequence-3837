"""入队通道的回归：inline 直跑、10 任务配额、Redis 不可用时明确失败。"""
import pytest

from src.schemas.entities import RunInput
from src.services import run as service
from src.utils import repository as repo

from conftest import seed_inputs


def test_inline_mode_completes_without_queue(memory_repo, fake_redis, artifact_dir, monkeypatch):
    seed_inputs(memory_repo)
    monkeypatch.setattr(service, "TASK_MODE", "inline")
    done = service.create(RunInput(study_id="study-1"))
    assert done["status"] == "completed"
    assert done["attempt"] == 1 and done["result"]["checksum"]
    assert fake_redis.llen(service.QUEUE_KEY) == 0
    assert (artifact_dir / (done["id"] + ".json")).exists()


def test_queue_capacity_counts_recovering_runs(memory_repo, fake_redis, monkeypatch):
    seed_inputs(memory_repo)
    monkeypatch.setattr(service, "TASK_MODE", "redis")
    for _ in range(10):
        service.create(RunInput(study_id="study-1"))
    with pytest.raises(ValueError, match="队列已满"):
        service.create(RunInput(study_id="study-1"))

    # 一个中断进入 recovering 仍占配额；完成一个后释放名额
    first = repo.all_rows("runs")[0]
    first["status"] = "recovering"
    repo.put("runs", first)
    with pytest.raises(ValueError, match="队列已满"):
        service.create(RunInput(study_id="study-1"))
    first["status"] = "completed"
    repo.put("runs", first)
    extra = service.create(RunInput(study_id="study-1"))
    assert extra["status"] == "queued"


def test_redis_outage_marks_run_failed(memory_repo, monkeypatch):
    seed_inputs(memory_repo)

    class BrokenRedis:
        def rpush(self, *a, **k):
            raise ConnectionError("redis down")

    monkeypatch.setattr("redis.Redis.from_url", lambda _url: BrokenRedis())
    with pytest.raises(ValueError, match="队列不可用"):
        service.create(RunInput(study_id="study-1"))
    stored = repo.all_rows("runs")[0]
    assert stored["status"] == "failed" and "队列不可用" in stored["error"]
