import json
import logging
import threading
import redis
from src.models.entities import SequenceRun
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.config.settings import TASK_MODE, REDIS_URL, ARTIFACT_DIR

QUEUE_KEY = "journey-sequence:jobs"
# 心跳超过该秒数仍停留在 running 的运行，视为 worker 中断、可被重新认领。
STALE_CLAIM_SECONDS = 60

log = logging.getLogger("run.service")


def list_runs():
    return repo.all_rows("runs")


def _build_snapshot(study_id):
    """固定入队时刻的旅程数据、编码册与挖掘方案，后续修改不混入本次结果。"""
    study = repo.get("studies", study_id)
    dataset = repo.get("datasets", study["dataset_id"])
    dictionary = repo.get("dictionaries", dataset["dictionary_id"])
    return {"study": dict(study), "dataset": dict(dataset), "dictionary": dict(dictionary)}


def _is_stale(run, at):
    claimed = run.get("claimed_at")
    if not claimed:
        return True
    try:
        from datetime import datetime
        return (datetime.fromisoformat(at) - datetime.fromisoformat(claimed)).total_seconds() > STALE_CLAIM_SECONDS
    except ValueError:
        return True


def claim(id_):
    """原子认领一个排队中或中断遗留的运行，返回 (运行, 认领世代)；认领失败返回 (当前行, None)。"""
    at = now()
    existing = repo.get("runs", id_)
    stale = existing["status"] in ("running", "recovering") and _is_stale(existing, at)
    if existing["status"] != "queued" and not stale:
        return existing, None

    def mutate(run):
        run["attempt"] = int(run.get("attempt") or 0) + 1
        run["status"] = "recovering" if stale else "running"
        run["error"] = ""
        run["claimed_at"] = at
        run["heartbeat_at"] = at
        return run

    run, ok = repo.compare_and_put("runs", id_, mutate, (("status", existing["status"]),))
    return (run, run["attempt"]) if ok else (run, None)


def heartbeat(id_, attempt):
    """续租心跳；仅当前有效世代可写，迟到 worker 的心跳被拒绝。"""
    def mutate(run):
        run["heartbeat_at"] = now()
        return run
    _, ok = repo.compare_and_put("runs", id_, mutate, (("status", "running"), ("attempt", attempt)))
    return ok


def _publish(id_, attempt, mutator):
    """终态发布的唯一入口：只允许当前有效世代写入，迟到的成功/失败不能覆盖新结果。"""
    run = repo.get("runs", id_)
    if run.get("attempt") != attempt or run["status"] != "running":
        log.warning("丢弃迟到回写 run=%s attempt=%s 当前 status=%s attempt=%s",
                    id_, attempt, run["status"], run.get("attempt"))
        return run, False
    row, ok = repo.compare_and_put("runs", id_, mutator, (("status", "running"), ("attempt", attempt)))
    return row, ok


def execute(id_):
    """worker 执行入口：认领 → 心跳 → 以入队快照重算 → 带 fencing 发布结果与导出文件。"""
    run, attempt = claim(id_)
    if attempt is None:
        log.info("运行 %s 已被其他 worker 领取或已结束，跳过", id_)
        return run
    # 接管的运行先经历 recovering（等待恢复），进入正式处理态后置为 running，
    # 页面据此区分“等待恢复”与“处理中”。
    if run["status"] == "recovering":
        def activate(r):
            r["status"] = "running"
            r["heartbeat_at"] = now()
            return r
        run, ok = repo.compare_and_put("runs", id_, activate, (("status", "recovering"), ("attempt", attempt)))
        if not ok:
            return run
    try:
        snap = run.get("snapshot") or _build_snapshot(run["study_id"])
        stop = threading.Event()

        def keepalive():
            while not stop.wait(STALE_CLAIM_SECONDS / 3):
                if not heartbeat(id_, attempt):
                    return

        thread = threading.Thread(target=keepalive, name=f"hb-{id_}", daemon=True)
        thread.start()
        try:
            result = analyze(snap["dataset"]["rows"], snap["dictionary"], snap["study"])
        finally:
            stop.set()
            thread.join(timeout=1)

        def finish(r):
            r["result"] = result
            r["status"] = "completed"
            r["error"] = ""
            r["completed_at"] = now()
            r["heartbeat_at"] = now()
            return r
        published, ok = _publish(id_, attempt, finish)
        if ok:
            # 只有成功发布的当前世代才允许落导出文件，迟到结果不得写盘覆盖。
            ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
            (ARTIFACT_DIR / (id_ + ".json")).write_text(
                json.dumps(result, ensure_ascii=False), encoding="utf-8")
        return published
    except Exception as exc:
        logging.exception("运行 %s 第 %s 次尝试失败", id_, attempt)

        def fail(r):
            r["status"] = "failed"
            r["error"] = str(exc)
            r["completed_at"] = now()
            r["heartbeat_at"] = now()
            return r
        published, _ = _publish(id_, attempt, fail)
        return published


def requeue_stale(limit=20):
    """把中断遗留（running 且心跳超时）的运行重新投回队列，等待自动恢复。"""
    client = redis.Redis.from_url(REDIS_URL)
    at = now()
    queued = 0
    for run in repo.all_rows("runs"):
        if run["status"] in ("running", "recovering") and _is_stale(run, at):
            client.rpush(QUEUE_KEY, run["id"])
            queued += 1
            if queued >= limit:
                break
    return queued


def create(data):
    repo.get("studies", data.study_id)
    if sum(r["status"] in ("queued", "running", "recovering") for r in repo.all_rows("runs")) >= 10:
        raise ValueError("运行队列已满，请等待已有任务完成")
    run = SequenceRun(new_id("run"), data.study_id, created_at=now())
    run.snapshot = _build_snapshot(data.study_id)
    run = repo.put("runs", run)
    if TASK_MODE == "inline":
        return execute(run["id"])
    try:
        redis.Redis.from_url(REDIS_URL).rpush(QUEUE_KEY, run["id"])
    except Exception:
        def mark_failed(r):
            r["status"] = "failed"
            r["error"] = "队列不可用，请恢复Redis后重新提交"
            return r
        repo.compare_and_put("runs", run["id"], mark_failed, (("status", "queued"),))
        raise ValueError("队列不可用，请恢复Redis后重新提交")
    return run
