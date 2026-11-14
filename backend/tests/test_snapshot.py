"""入队快照：排队期间修改旅程数据、编码册或方案，不影响已入队运行的重算口径。"""
import copy

from src.schemas.entities import RunInput
from src.services import run as service
from src.utils import repository as repo

from conftest import seed_inputs


def test_run_uses_enqueued_snapshot_after_dataset_and_study_change(memory_repo, fake_redis, artifact_dir):
    dictionary, dataset, study = seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]
    expected_checksum = None

    # 入队后、处理前，业务侧修改了旅程数据、编码册与挖掘方案
    changed_dataset = copy.deepcopy(dataset)
    changed_dataset["rows"].append(
        {"journey_id": "j4", "path": ["A", "C"], "outcome": "exited"})
    changed_dataset["rows"][0]["path"] = ["A", "C"]
    repo.put("datasets", changed_dataset)

    changed_study = copy.deepcopy(study)
    changed_study["min_support"] = 0.5
    changed_study["max_length"] = 2
    repo.put("studies", changed_study)

    changed_dict = copy.deepcopy(dictionary)
    changed_dict["entries"].append({"code": "D", "label": "侧厅"})
    repo.put("dictionaries", changed_dict)

    # 用入队时快照离线算一遍期望值
    from src.services.statistics import analyze
    expected = analyze(dataset["rows"], dictionary, study)

    done = service.execute(rid)
    assert done["status"] == "completed"
    assert done["result"]["checksum"] == expected["checksum"]
    assert done["result"]["journeys"] == 3  # 不是修改后的 4
    assert done["result"]["study"]["min_support"] == 0.1
    assert {e["code"] for e in done["result"]["dictionary"]["entries"]} == {"A", "B", "C"}


def test_recovery_recomputes_from_original_snapshot(memory_repo, fake_redis, artifact_dir):
    dictionary, dataset, study = seed_inputs(memory_repo)
    run = service.create(RunInput(study_id="study-1"))
    rid = run["id"]
    _, attempt1 = service.claim(rid)

    # 中断期间方案被改过
    changed_study = copy.deepcopy(study)
    changed_study["collapse_repeats"] = False
    repo.put("studies", changed_study)

    from datetime import datetime, timedelta
    from src.utils.identity import now
    row = repo.get("runs", rid)
    row["claimed_at"] = (datetime.fromisoformat(now()) - timedelta(seconds=service.STALE_CLAIM_SECONDS + 5)).isoformat()
    repo.put("runs", row)

    done = service.execute(rid)
    assert done["status"] == "completed" and done["attempt"] == 2
    # 恢复重算仍用入队时口径（折叠连续重复，j3 的 A,A 被折叠）
    assert done["result"]["collapse_repeats"] is True
    from src.services.statistics import analyze
    assert done["result"]["checksum"] == analyze(dataset["rows"], dictionary, study)["checksum"]
