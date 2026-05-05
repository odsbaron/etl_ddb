import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY
from etl.writers.dolphindb_writer import DolphinDBWriter


DDB_CFG = {
    "host": "localhost", "port": 8848,
    "user": "admin", "password": "pass",
    "db_path": "dfs://test_db",
}


class FakeStrategy:
    def __init__(self):
        self.duplicates = set()
        self.pre_writes = []
        self.post_writes = []

    def is_duplicate(self, batch_id):
        return batch_id in self.duplicates

    def pre_write(self, df, batch_id, session):
        self.pre_writes.append((batch_id, len(df)))

    def post_write(self, batch_id):
        self.post_writes.append(batch_id)
        self.duplicates.add(batch_id)


class TestDolphinDBWriter:
    @patch("etl.writers.dolphindb_writer.ddb.Session")
    @patch("etl.writers.dolphindb_writer.ddb.TableAppender")
    def test_write_batch_calls_strategy_pre_write(self, mock_appender_cls, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_appender = MagicMock()
        mock_appender_cls.return_value = mock_appender

        strategy = FakeStrategy()
        writer = DolphinDBWriter(DDB_CFG, "orders", strategy)
        df = pd.DataFrame({"col": [1, 2, 3]})
        writer.write_batch(df, "batch_1")

        assert len(strategy.pre_writes) == 1
        assert strategy.pre_writes[0] == ("batch_1", 3)

    @patch("etl.writers.dolphindb_writer.ddb.Session")
    @patch("etl.writers.dolphindb_writer.ddb.TableAppender")
    def test_write_batch_calls_table_appender_append(self, mock_appender_cls, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_appender = MagicMock()
        mock_appender_cls.return_value = mock_appender

        strategy = FakeStrategy()
        writer = DolphinDBWriter(DDB_CFG, "orders", strategy)
        df = pd.DataFrame({"col": [1]})
        writer.write_batch(df, "batch_1")

        mock_appender.append.assert_called_once()
        pd.testing.assert_frame_equal(mock_appender.append.call_args[0][0], df)

    @patch("etl.writers.dolphindb_writer.ddb.Session")
    @patch("etl.writers.dolphindb_writer.ddb.TableAppender")
    def test_write_batch_calls_strategy_post_write(self, mock_appender_cls, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_appender = MagicMock()
        mock_appender_cls.return_value = mock_appender

        strategy = FakeStrategy()
        writer = DolphinDBWriter(DDB_CFG, "orders", strategy)
        writer.write_batch(pd.DataFrame({"col": [1]}), "batch_x")

        assert "batch_x" in strategy.post_writes

    @patch("etl.writers.dolphindb_writer.ddb.Session")
    @patch("etl.writers.dolphindb_writer.ddb.TableAppender")
    def test_write_batch_skips_duplicate(self, mock_appender_cls, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_appender = MagicMock()
        mock_appender_cls.return_value = mock_appender

        strategy = FakeStrategy()
        strategy.duplicates.add("dup_batch")
        writer = DolphinDBWriter(DDB_CFG, "orders", strategy)
        writer.write_batch(pd.DataFrame({"col": [1]}), "dup_batch")

        mock_appender.append.assert_not_called()
        assert len(strategy.pre_writes) == 0

    @patch("etl.writers.dolphindb_writer.ddb.Session")
    def test_close_closes_session(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        writer = DolphinDBWriter(DDB_CFG, "orders", FakeStrategy())
        writer.close()

        mock_session.close.assert_called_once()
