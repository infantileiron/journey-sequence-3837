"""HTTP 层：/runs/recover 端点装配与返回结构。"""
import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.services import auth
from src.schemas.entities import RunInput

from conftest import seed_inputs


@pytest.fixture
def client():
    app.dependency_overrides[auth.current] = lambda: {"email": "analyst@qishan.test", "role": "analyst"}
    app.dependency_overrides[auth.editor] = lambda: {"email": "analyst@qishan.test", "role": "analyst"}
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_recover_endpoint_returns_requeue_count(memory_repo, fake_redis, artifact_dir, client):
    seed_inputs(memory_repo)
    from src.services import run as service
    created = service.create(RunInput(study_id="study-1"))
    fake_redis.blpop(service.QUEUE_KEY, timeout=1)
    service.claim(created["id"])

    # 心跳新鲜时不重投
    resp = client.post("/runs/recover")
    assert resp.status_code == 200
    assert resp.json() == {"requeued": 0}

    # 心跳超时后端点触发重投
    from datetime import datetime, timedelta
    from src.utils import repository as repo
    from src.utils.identity import now
    row = repo.get("runs", created["id"])
    row["claimed_at"] = (datetime.fromisoformat(now()) - timedelta(seconds=service.STALE_CLAIM_SECONDS + 5)).isoformat()
    repo.put("runs", row)
    resp = client.post("/runs/recover")
    assert resp.status_code == 200 and resp.json() == {"requeued": 1}
    assert fake_redis.llen(service.QUEUE_KEY) == 1
