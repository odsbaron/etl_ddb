import sqlite3
import json
import os
import tempfile
import pytest

from etl.checkpoint import CheckpointManager


class TestCheckpointManager:
    @pytest.fixture
    def db_path(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        yield tmp.name
        os.unlink(tmp.name)

    @pytest.fixture
    def mgr(self, db_path):
        return CheckpointManager(db_path)

    def test_load_returns_empty_dict_for_unknown_job(self, mgr):
        result = mgr.load("nonexistent")
        assert result == {}

    def test_save_and_load_round_trip(self, mgr):
        data = {"last_val": "2025-06-01 12:00:00"}
        mgr.save("job_a", data)
        loaded = mgr.load("job_a")
        assert loaded == data

    def test_save_overwrites_existing_checkpoint(self, mgr):
        mgr.save("job_a", {"last_val": "first"})
        mgr.save("job_a", {"last_val": "second"})
        assert mgr.load("job_a") == {"last_val": "second"}

    def test_different_jobs_dont_conflict(self, mgr):
        mgr.save("job_a", {"last_val": "aaa"})
        mgr.save("job_b", {"last_val": "bbb"})
        assert mgr.load("job_a") == {"last_val": "aaa"}
        assert mgr.load("job_b") == {"last_val": "bbb"}

    def test_table_is_created_when_file_is_new(self, db_path):
        mgr = CheckpointManager(db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints'"
        )
        assert cur.fetchone() is not None
        conn.close()
