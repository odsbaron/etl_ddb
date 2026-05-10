import pandas as pd
import pytest

from etl.writers.strategies.append_only import AppendOnlyStrategy
from etl.writers.strategies.factory import build_write_strategy
from etl.writers.strategies.key_delete_insert import KeyDeleteInsertStrategy
from etl.writers.strategies.window_overwrite import WindowOverwriteStrategy


class MockSession:
    def __init__(self):
        self.scripts = []

    def run(self, script):
        self.scripts.append(script)


def test_factory_builds_window_overwrite_strategy():
    strategy = build_write_strategy({"writer_strategy": "window_overwrite", "time_column": "t"})
    assert isinstance(strategy, WindowOverwriteStrategy)


def test_factory_builds_append_only_strategy():
    strategy = build_write_strategy({"writer_strategy": "append_only"})
    assert isinstance(strategy, AppendOnlyStrategy)


def test_factory_builds_key_delete_insert_strategy():
    strategy = build_write_strategy({"writer_strategy": "key_delete_insert", "key_columns": ["id"]})
    assert isinstance(strategy, KeyDeleteInsertStrategy)


def test_secumain_sync_uses_primary_key_delete_insert_config():
    import yaml

    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)
    job = next(j for j in config["jobs"] if j["name"] == "secumain_sync")

    assert job["writer_strategy"] == "key_delete_insert"
    assert job["key_columns"] == ["id"]
    assert job["cursor_columns"] == ["sjqy_update_time", "ID"]
    assert job["initial_cursor"] == {
        "sjqy_update_time": "1900-01-01 00:00:00",
        "ID": 0,
    }
    assert "initial_last_val" not in job


def test_factory_rejects_unknown_strategy():
    with pytest.raises(ValueError, match="Unknown writer_strategy"):
        build_write_strategy({"writer_strategy": "bad"})


def test_append_only_does_not_run_pre_write_script():
    session = MockSession()
    strategy = AppendOnlyStrategy()
    strategy.pre_write(pd.DataFrame({"id": [1]}), "batch", session, "dfs://db", "t")
    assert session.scripts == []


def test_key_delete_insert_deletes_matching_keys_before_append():
    session = MockSession()
    strategy = KeyDeleteInsertStrategy(["id"])
    df = pd.DataFrame({"id": [1, 2, 2]})
    strategy.pre_write(df, "batch", session, "dfs://db", "t")
    assert len(session.scripts) == 1
    assert "delete from loadTable('dfs://db','t') where id in [1, 2]" in session.scripts[0]
