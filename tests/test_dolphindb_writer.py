import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from etl.writers.dolphindb_writer import DolphinDBWriter


DDB_CFG = {
    "host": "localhost", "port": 8848,
    "user": "admin", "password": "pass",
    "db_path": "dfs://test_db",
}

JOB_CFG = {
    "dolphindb_table": "orders",
    "time_column": "updateTime",
    "field_mapping": [
        {"mysql": "update_time", "dolphindb": "updateTime", "type": "TIMESTAMP"},
        {"mysql": "secu_code", "dolphindb": "secuCode", "type": "SYMBOL"},
    ],
}


class FakeStrategy:
    def __init__(self):
        self.duplicates = set()
        self.pre_writes = []
        self.post_writes = []

    def is_duplicate(self, batch_id):
        return batch_id in self.duplicates

    def pre_write(self, df, batch_id, session, db_path, table_name):
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

    @patch("etl.writers.dolphindb_writer.build_bootstrap_script")
    @patch("etl.writers.dolphindb_writer.ddb.Session")
    def test_bootstrap_runs_once_when_job_config_provided(self, mock_session_cls, mock_bootstrap_script):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_bootstrap_script.return_value = "bootstrap script"

        writer = DolphinDBWriter(DDB_CFG, "orders", FakeStrategy(), JOB_CFG)
        writer.bootstrap()
        writer.bootstrap()

        mock_bootstrap_script.assert_called_once_with({"db_path": "dfs://test_db"}, JOB_CFG)
        mock_session.run.assert_called_once_with("bootstrap script")

    @patch("etl.writers.dolphindb_writer.build_bootstrap_script")
    @patch("etl.writers.dolphindb_writer.ddb.Session")
    def test_bootstrap_skips_when_disabled(self, mock_session_cls, mock_bootstrap_script):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        writer = DolphinDBWriter(DDB_CFG, "orders", FakeStrategy(), {**JOB_CFG, "bootstrap": False})
        writer.bootstrap()

        mock_bootstrap_script.assert_not_called()
        mock_session.run.assert_not_called()

    @patch("etl.writers.dolphindb_writer.ddb.Session")
    def test_close_closes_session(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        writer = DolphinDBWriter(DDB_CFG, "orders", FakeStrategy())
        writer.close()

        mock_session.close.assert_called_once()
