import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

from etl.pipeline import ETLPipeline

MYSQL_CFG = {"host": "h", "port": 3306, "user": "u", "password": "p", "database": "d"}
DDB_CFG = {"host": "h", "port": 8848, "user": "u", "password": "p", "db_path": "dfs://db"}
JOB_CFG = {
    "name": "order_sync",
    "mysql_table": "orders",
    "dolphindb_table": "orders",
    "chunk_size": 100,
    "time_column": "modified_at",
    "field_mapping": [
        {"mysql": "id", "dolphindb": "id", "type": None},
    ],
}


class TestETLPipeline:
    @patch("etl.pipeline.DolphinDBWriter")
    @patch("etl.pipeline.build_write_strategy")
    @patch("etl.pipeline.DataTransformer")
    @patch("etl.pipeline.MySQLChunkReader")
    def test_run_processes_all_batches_and_saves_checkpoints(
        self, mock_reader_cls, mock_xfrm_cls, mock_strategy_builder, mock_writer_cls
    ):
        checkpoint_mgr = MagicMock()
        checkpoint_mgr.load.return_value = {"last_val": "2025-01-01"}

        mock_reader = MagicMock()
        mock_reader.read_batches.return_value = iter([
            (pd.DataFrame({"id": [1, 2]}), "batch_1"),
            (pd.DataFrame({"id": [3]}), "batch_2"),
        ])
        mock_reader.get_checkpoint.side_effect = [
            {"last_val": "2025-01-02"},
            {"last_val": "2025-01-03"},
        ]
        mock_reader_cls.return_value = mock_reader

        mock_xfrm = MagicMock()
        mock_xfrm.transform.side_effect = lambda df: df
        mock_xfrm_cls.return_value = mock_xfrm

        mock_strategy = MagicMock()
        mock_strategy_builder.return_value = mock_strategy

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        pipeline = ETLPipeline(JOB_CFG, MYSQL_CFG, DDB_CFG, checkpoint_mgr)
        pipeline.run()

        assert mock_writer_cls.call_args.args == (DDB_CFG, "orders", mock_strategy, JOB_CFG)
        assert mock_writer.write_batch.call_count == 2
        assert checkpoint_mgr.save.call_count == 2
        checkpoint_mgr.save.assert_has_calls([
            call("order_sync", {"last_val": "2025-01-02"}),
            call("order_sync", {"last_val": "2025-01-03"}),
        ])

    @patch("etl.pipeline.DolphinDBWriter")
    @patch("etl.pipeline.build_write_strategy")
    @patch("etl.pipeline.DataTransformer")
    @patch("etl.pipeline.MySQLChunkReader")
    def test_run_closes_reader_and_writer_on_success(
        self, mock_reader_cls, mock_xfrm_cls, mock_strategy_builder, mock_writer_cls
    ):
        checkpoint_mgr = MagicMock()
        checkpoint_mgr.load.return_value = {}

        mock_reader = MagicMock()
        mock_reader.read_batches.return_value = iter([])
        mock_reader.get_checkpoint.return_value = {"last_val": 0}
        mock_reader_cls.return_value = mock_reader

        mock_xfrm_cls.return_value = MagicMock()
        mock_strategy_builder.return_value = MagicMock()

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        pipeline = ETLPipeline(JOB_CFG, MYSQL_CFG, DDB_CFG, checkpoint_mgr)
        pipeline.run()

        mock_reader.close.assert_called_once()
        mock_writer.close.assert_called_once()

    @patch("etl.pipeline.DolphinDBWriter")
    @patch("etl.pipeline.build_write_strategy")
    @patch("etl.pipeline.DataTransformer")
    @patch("etl.pipeline.MySQLChunkReader")
    def test_run_closes_reader_and_writer_on_exception(
        self, mock_reader_cls, mock_xfrm_cls, mock_strategy_builder, mock_writer_cls
    ):
        checkpoint_mgr = MagicMock()
        checkpoint_mgr.load.return_value = {}

        mock_reader = MagicMock()
        mock_reader.read_batches.side_effect = RuntimeError("connection lost")
        mock_reader_cls.return_value = mock_reader

        mock_xfrm_cls.return_value = MagicMock()
        mock_strategy_builder.return_value = MagicMock()

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        pipeline = ETLPipeline(JOB_CFG, MYSQL_CFG, DDB_CFG, checkpoint_mgr)
        with pytest.raises(RuntimeError, match="connection lost"):
            pipeline.run()

        mock_reader.close.assert_called_once()
        mock_writer.close.assert_called_once()
