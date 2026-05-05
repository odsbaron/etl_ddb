import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from etl.readers.mysql_chunk_reader import MySQLChunkReader


MYSQL_CFG = {
    "host": "localhost", "port": 3306, "user": "test",
    "password": "test", "database": "test",
}

JOB_CFG = {
    "mysql_table": "orders",
    "chunk_size": 3,
    "incremental_col": "modified_at",
}


class TestMySQLChunkReader:
    @patch("etl.readers.mysql_chunk_reader.pymysql.connect")
    def test_yields_correct_number_of_batches(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        rows = [
            [1, "a", "2025-01-01"],
            [2, "b", "2025-01-02"],
            [3, "c", "2025-01-03"],
            [4, "d", "2025-01-04"],
            [5, "e", "2025-01-05"],
        ]

        call_count = [0]

        def mock_read_sql(sql, conn):
            call_count[0] += 1
            if call_count[0] == 1:
                return pd.DataFrame(rows[:3], columns=["id", "name", "modified_at"])
            elif call_count[0] == 2:
                return pd.DataFrame(rows[3:], columns=["id", "name", "modified_at"])
            else:
                return pd.DataFrame(columns=["id", "name", "modified_at"])

        with patch("pandas.read_sql", side_effect=mock_read_sql):
            reader = MySQLChunkReader(MYSQL_CFG, JOB_CFG, {})
            batches = list(reader.read_batches())
            reader.close()

        assert len(batches) == 2
        assert len(batches[0][0]) == 3
        assert len(batches[1][0]) == 2

    @patch("etl.readers.mysql_chunk_reader.pymysql.connect")
    def test_advances_last_val_after_each_batch(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        rows = [
            [1, "a", "2025-01-01"],
            [2, "b", "2025-01-03"],
        ]

        call_count = [0]

        def mock_read_sql(sql, conn):
            call_count[0] += 1
            if call_count[0] == 1:
                return pd.DataFrame(rows, columns=["id", "name", "modified_at"])
            return pd.DataFrame(columns=["id", "name", "modified_at"])

        with patch("pandas.read_sql", side_effect=mock_read_sql):
            reader = MySQLChunkReader(MYSQL_CFG, JOB_CFG, {})
            list(reader.read_batches())
            cp = reader.get_checkpoint()
            reader.close()

        assert cp["last_val"] == "2025-01-03"

    @patch("etl.readers.mysql_chunk_reader.pymysql.connect")
    def test_returns_batch_id_with_each_chunk(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        rows = [[1, "a", "2025-01-01"]]
        call_count = [0]

        def mock_read_sql(sql, conn):
            call_count[0] += 1
            if call_count[0] == 1:
                return pd.DataFrame(rows, columns=["id", "name", "modified_at"])
            return pd.DataFrame(columns=["id", "name", "modified_at"])

        with patch("pandas.read_sql", side_effect=mock_read_sql):
            reader = MySQLChunkReader(MYSQL_CFG, JOB_CFG, {})
            df, batch_id = next(reader.read_batches())
            reader.close()

        assert isinstance(batch_id, str)
        assert "orders" in batch_id

    @patch("etl.readers.mysql_chunk_reader.pymysql.connect")
    def test_empty_result_stops_iteration(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with patch("pandas.read_sql", return_value=pd.DataFrame()):
            reader = MySQLChunkReader(MYSQL_CFG, JOB_CFG, {})
            batches = list(reader.read_batches())
            reader.close()

        assert len(batches) == 0

    @patch("etl.readers.mysql_chunk_reader.pymysql.connect")
    def test_starts_from_checkpoint_value(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        captured_sql = []

        def mock_read_sql(sql, conn):
            captured_sql.append(sql)
            return pd.DataFrame(columns=["id", "name", "modified_at"])

        checkpoint = {"last_val": "2025-03-01 12:00:00"}
        with patch("pandas.read_sql", side_effect=mock_read_sql):
            reader = MySQLChunkReader(MYSQL_CFG, JOB_CFG, checkpoint)
            list(reader.read_batches())
            reader.close()

        assert "> '2025-03-01 12:00:00'" in captured_sql[0]
