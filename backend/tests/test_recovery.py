"""排队恢复与中断接管：worker 强杀后运行不卡死，新 worker 自动重投并完成。"""
from datetime import datetime, timedelta

from src.schemas.entities import RunInput
from src.services import run as service
from src.utils import repository as repo
from src.utils.identity import now

from conftest import seed_inputs


def _create_run():
    return service.create(RunInput(study_id="study-1"))


def test_queued_job_runs_to_completion_and_writes_artifact(memory_repo, fake_redis, artifact_dir):
    seed_inputs(memory_repo)
    run = _create_run()
    assert run["status"] == "queued"
    assert fake_redis.llen(service.QUEUE_KEY) == 1

    # worker 主循环先 blpop 出队，再执行
    queued_id = fake_redis.blpop(service.QUEUE_KEY, timeout=1)[1].decode()
    finished = service.execute(queued_id)
    assert finished["status"] == "completed"
    assert finished["attempt"] == 1
    assert finished["result"]["checksum"]
    assert (artifact_dir / (run["id"] + ".json")).exists()
    # 队列消息已被消费
    assert fake_redis.llen(service.QUEUE_KEY) == 0


def test_interrupted_running_run_is_requeued_and_recovered(memory_repo, fake_redis, artifact_dir):
    seed_inputs(memory_repo)
    run = _create_run()

    # worker 出队、原子认领后被强杀：状态停在 running（心跳已停），队列已空
    assert fake_redis.blpop(service.QUEUE_KEY, timeout=1)[1].decode() == run["id"]
    claimed, attempt = service.claim(run["id"])
    assert attempt == 1
    assert claimed["status"] == "running"
    assert fake_redis.llen(service.QUEUE_KEY) == 0

    # 未超心跳窗口时不会被接管（仍由当前 worker 持有）
    assert service.requeue_stale() == 0
    again, second = service.claim(run["id"])
    assert second is None and again["status"] == "running"

    # 心跳超时 → 启动恢复扫描重新入队
    stale = repo.get("runs", run["id"])
    stale["claimed_at"] = (datetime.fromisoformat(now()) - timedelta(seconds=service.STALE_CLAIM_SECONDS + 5)).isoformat()
    repo.put("runs", stale)
    assert service.requeue_stale() == 1
    assert fake_redis.llen(service.QUEUE_KEY) == 1

    # 认领中断任务时先进入 recovering（页面可见“等待恢复”），世代号递增
    recovered, new_attempt = service.claim(run["id"])
    assert new_attempt == 2
    assert recovered["status"] == "recovering"

    # 另一个 worker 对同一 id 的重复领取必须失败
    duplicate, dup_attempt = service.claim(run["id"])
    assert dup_attempt is None and duplicate["attempt"] == 2

    # 模拟完整 worker 出队执行：重新制造中断态后由 execute 端到端接管
    recovered["status"] = "running"
    recovered["claimed_at"] = stale["claimed_at"]
    repo.put("runs", recovered)
    done = service.execute(run["id"])
    assert done["status"] == "completed"
    assert done["attempt"] == 3
    assert done["result"]["checksum"]
    assert (artifact_dir / (run["id"] + ".json")).exists()


def test_stale_recovering_row_can_be_taken_over(memory_repo, fake_redis, artifact_dir):
    seed_inputs(memory_repo)
    run = _create_run()
    rid = run["id"]

    # worker 已接管但在 recovering→running 之间再次被强杀
    claimed, attempt = service.claim(rid)
    assert attempt == 1 and claimed["status"] == "running"
    row = repo.get("runs", rid)
    row["status"] = "recovering"
    row["attempt"] = 2
    row["claimed_at"] = (datetime.fromisoformat(now()) - timedelta(seconds=service.STALE_CLAIM_SECONDS + 5)).isoformat()
    repo.put("runs", row)

    assert service.requeue_stale() == 1
    done = service.execute(fake_redis.blpop(service.QUEUE_KEY, timeout=1)[1].decode())
    assert done["status"] == "completed"
    assert done["attempt"] == 3
    assert done["result"]["checksum"]


def test_duplicate_queue_message_is_ignored_after_completion(memory_repo, fake_redis, artifact_dir):
    seed_inputs(memory_repo)
    run = _create_run()
    service.execute(run["id"])
    assert repo.get("runs", run["id"])["status"] == "completed"

    # 恢复扫描把一条历史消息重复投回（或旧消息延迟到达）
    fake_redis.rpush(service.QUEUE_KEY, run["id"])
    skipped = service.execute(run["id"])
    assert skipped["status"] == "completed"
    assert skipped["attempt"] == 1  # 没有产生新的世代
