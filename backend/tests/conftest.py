"""测试替身：用内存表替换 MySQL 仓储，保留 SELECT ... FOR UPDATE 的行锁语义。

业务代码只依赖 repository 的 get/put/all_rows/compare_and_put 四个函数，
因此替换后跑通的并发控制逻辑与生产实现一致（CAS 条件 + 行锁 + 提交后可见）。
"""
import threading
import fakeredis
import pytest

from src.utils import repository as repo
from src.config import settings

_TABLES: dict[str, dict[str, dict]] = {}
_LOCKS: dict[str, threading.Lock] = {}
_GLOBAL = threading.RLock()


def _table(name):
    with _GLOBAL:
        if name not in _TABLES:
            _TABLES[name] = {}
        if name not in _LOCKS:
            _LOCKS[name] = threading.Lock()
        return _TABLES[name], _LOCKS[name]


def _connect():
    return None


def _init_db():
    _TABLES.clear()
    _LOCKS.clear()
    for name in repo.TABLES:
        _TABLES[name] = {}
        _LOCKS[name] = threading.Lock()


def _all_rows(table):
    t, _ = _table(table)
    with _GLOBAL:
        import json
        return [json.loads(json.dumps(v)) for _, v in sorted(t.items())]


def _get(table, id_):
    from fastapi import HTTPException
    import json
    t, _ = _table(table)
    with _GLOBAL:
        if id_ not in t:
            raise HTTPException(404, "记录不存在")
        return json.loads(json.dumps(t[id_]))


def _put(table, item, insert_only=False):
    import json
    if not isinstance(item, dict):
        from dataclasses import asdict
        item = asdict(item)
    t, lock = _table(table)
    with lock:
        if insert_only and item["id"] in t:
            return item
        t[item["id"]] = json.loads(json.dumps(item, ensure_ascii=False))
    return item


def _compare_and_put(table, id_, mutator, expected):
    import json
    t, lock = _table(table)
    with lock:  # 对应 SELECT ... FOR UPDATE 行锁
        if id_ not in t:
            from fastapi import HTTPException
            raise HTTPException(404, "记录不存在")
        current = json.loads(json.dumps(t[id_]))
        if any(current.get(key) != value for key, value in expected):
            return current, False
        updated = mutator(json.loads(json.dumps(current)))
        if not isinstance(updated, dict):
            from dataclasses import asdict
            updated = asdict(updated)
        t[id_] = json.loads(json.dumps(updated, ensure_ascii=False))
        return json.loads(json.dumps(updated)), True


@pytest.fixture
def memory_repo(monkeypatch):
    _init_db()
    monkeypatch.setattr(repo, "connect", _connect)
    monkeypatch.setattr(repo, "init_db", _init_db)
    monkeypatch.setattr(repo, "all_rows", _all_rows)
    monkeypatch.setattr(repo, "get", _get)
    monkeypatch.setattr(repo, "put", _put)
    monkeypatch.setattr(repo, "compare_and_put", _compare_and_put)
    return _TABLES


@pytest.fixture
def fake_redis(monkeypatch):
    server = fakeredis.FakeRedis(decode_responses=False)

    def _from_url(_url):
        return server

    monkeypatch.setattr("redis.Redis.from_url", _from_url)
    return server


@pytest.fixture
def artifact_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "ARTIFACT_DIR", tmp_path)
    from src.services import run as run_service
    monkeypatch.setattr(run_service, "ARTIFACT_DIR", tmp_path)
    return tmp_path


def seed_inputs(memory_repo):
    dictionary = {
        "id": "dict-1", "name": "编码册",
        "entries": [{"code": "A", "label": "入口"}, {"code": "B", "label": "主厅"},
                    {"code": "C", "label": "商店"}],
    }
    dataset = {
        "id": "ds-1", "name": "旅程", "source": "demo.csv", "dictionary_id": "dict-1",
        "rows": [
            {"journey_id": "j1", "path": ["A", "B", "C"], "outcome": "converted"},
            {"journey_id": "j2", "path": ["A", "B"], "outcome": "exited"},
            {"journey_id": "j3", "path": ["A", "A", "B", "C"], "outcome": "converted"},
        ],
    }
    study = {
        "id": "study-1", "name": "方案", "dataset_id": "ds-1",
        "min_support": 0.1, "max_length": 3, "collapse_repeats": True,
    }
    memory_repo.setdefault("dictionaries", {})["dict-1"] = dictionary
    memory_repo.setdefault("datasets", {})["ds-1"] = dataset
    memory_repo.setdefault("studies", {})["study-1"] = study
    return dictionary, dataset, study
