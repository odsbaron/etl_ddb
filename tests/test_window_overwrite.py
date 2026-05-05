import pandas as pd
import pytest
from etl.writers.strategies.window_overwrite import WindowOverwriteStrategy


class MockSession:
    def __init__(self):
        self.scripts = []

    def run(self, script):
        self.scripts.append(script)


class TestWindowOverwriteStrategy:
    @pytest.fixture
    def strategy(self):
        return WindowOverwriteStrategy("updateTime")

    def test_is_duplicate_returns_false_for_new_batch(self, strategy):
        assert strategy.is_duplicate("batch_1") is False

    def test_post_write_marks_batch_as_processed(self, strategy):
        strategy.post_write("batch_1")
        assert strategy.is_duplicate("batch_1") is True

    def test_is_duplicate_only_returns_true_for_post_written_batches(self, strategy):
        strategy.post_write("batch_a")
        assert strategy.is_duplicate("batch_a") is True
        assert strategy.is_duplicate("batch_b") is False

    def test_pre_write_generates_delete_for_time_window(self, strategy):
        session = MockSession()
        df = pd.DataFrame({"updateTime": pd.to_datetime([
            "2025-06-01 10:00:00",
            "2025-06-01 11:00:00",
            "2025-06-01 12:00:00",
        ])})
        strategy.pre_write(df, "batch_1", session)
        assert len(session.scripts) == 1
        script = session.scripts[0]
        assert "delete from" in script.lower()
        assert "between" in script.lower()

    def test_processed_batches_are_independent_instances(self):
        s1 = WindowOverwriteStrategy("t")
        s2 = WindowOverwriteStrategy("t")
        s1.post_write("x")
        assert s2.is_duplicate("x") is False
