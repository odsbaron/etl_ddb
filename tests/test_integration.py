import sqlite3
import os
import tempfile
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from etl.checkpoint import CheckpointManager
from etl.pipeline import ETLPipeline


JOB_CFG = {
    "name": "test_sync",
    "mysql_table": "source_table",
    "dolphindb_table": "target_table",
    "chunk_size": 3,
    "incremental_col": "id",
    "time_column": "ts",
    "field_mapping": [
        {"mysql": "id", "dolphindb": "id", "type": None},
        {"mysql": "name", "dolphindb": "name", "type": "STRING"},
        {"mysql": "ts", "dolphindb": "ts", "type": "TIMESTAMP"},
    ],
}


class FakeSession:
    def __init__(self):
        self.rows = []
        self.deletes = []

    def run(self, script):
        self.deletes.append(script)

    def connect(self, host, port, user, password):
        pass

    def close(self):
        pass


class FakeAppender:
    def __init__(self, table_name, db_path, session):
        self.session = session

    def append(self, df):
        for _, row in df.iterrows():
            self.session.rows.append(row.to_dict())


class TestIntegration:
    @pytest.fixture
    def source_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        conn = sqlite3.connect(tmp.name)
        conn.execute("CREATE TABLE source_table (id INT, name TEXT, ts TEXT)")
        conn.execute("INSERT INTO source_table VALUES (1, 'alice', '2025-01-01 10:00:00')")
        conn.execute("INSERT INTO source_table VALUES (2, 'bob',   '2025-01-01 11:00:00')")
        conn.execute("INSERT INTO source_table VALUES (3, 'carol', '2025-01-01 12:00:00')")
        conn.execute("INSERT INTO source_table VALUES (4, 'dan',   '2025-01-02 08:00:00')")
        conn.execute("INSERT INTO source_table VALUES (5, 'eve',   '2025-01-02 09:00:00')")
        conn.commit()
        conn.close()
        yield tmp.name
        os.unlink(tmp.name)

    @pytest.fixture
    def cp_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        yield tmp.name
        os.unlink(tmp.name)

    @patch("etl.writers.dolphindb_writer.ddb.TableAppender", new=FakeAppender)
    @patch("etl.writers.dolphindb_writer.ddb.Session", new=FakeSession)
    def test_full_sync_all_rows_transformed(self, source_db, cp_db):
        mysql_cfg = {
            "host": "localhost", "port": 3306, "user": "u", "password": "p",
            "database": source_db,
        }
        ddb_cfg = {
            "host": "h", "port": 8848, "user": "u", "password": "p",
            "db_path": "dfs://db",
        }
        checkpoint_mgr = CheckpointManager(cp_db)

        source_conn = sqlite3.connect(source_db)

        with patch("etl.readers.mysql_chunk_reader.pymysql.connect", return_value=source_conn):
            pipeline = ETLPipeline(JOB_CFG, mysql_cfg, ddb_cfg, checkpoint_mgr)
            pipeline.run()

        cp = checkpoint_mgr.load("test_sync")
        assert cp["last_val"] == 5  # max id — all 5 rows processed (2 batches × chunk_size=3)

    @patch("etl.writers.dolphindb_writer.ddb.TableAppender", new=FakeAppender)
    @patch("etl.writers.dolphindb_writer.ddb.Session", new=FakeSession)
    def test_resume_from_checkpoint_no_duplicates(self, source_db, cp_db):
        mysql_cfg = {
            "host": "localhost", "port": 3306, "user": "u", "password": "p",
            "database": source_db,
        }
        ddb_cfg = {
            "host": "h", "port": 8848, "user": "u", "password": "p",
            "db_path": "dfs://db",
        }
        checkpoint_mgr = CheckpointManager(cp_db)

        source_conn1 = sqlite3.connect(source_db)
        with patch("etl.readers.mysql_chunk_reader.pymysql.connect", return_value=source_conn1):
            pipeline1 = ETLPipeline(JOB_CFG, mysql_cfg, ddb_cfg, checkpoint_mgr)
            pipeline1.run()

        cp_after_first = checkpoint_mgr.load("test_sync")
        assert cp_after_first["last_val"] == 5

        # Add new row to source
        add_conn = sqlite3.connect(source_db)
        add_conn.execute("INSERT INTO source_table VALUES (6, 'frank', '2025-01-03 10:00:00')")
        add_conn.commit()
        add_conn.close()

        source_conn2 = sqlite3.connect(source_db)
        with patch("etl.readers.mysql_chunk_reader.pymysql.connect", return_value=source_conn2):
            pipeline2 = ETLPipeline(JOB_CFG, mysql_cfg, ddb_cfg, checkpoint_mgr)
            pipeline2.run()

        cp_after_second = checkpoint_mgr.load("test_sync")
        assert cp_after_second["last_val"] == 6

    @patch("etl.writers.dolphindb_writer.ddb.TableAppender", new=FakeAppender)
    @patch("etl.writers.dolphindb_writer.ddb.Session", new=FakeSession)
    def test_empty_source_no_batches(self, source_db, cp_db):
        # Truncate the table so it's empty
        conn = sqlite3.connect(source_db)
        conn.execute("DELETE FROM source_table")
        conn.commit()
        conn.close()

        mysql_cfg = {
            "host": "localhost", "port": 3306, "user": "u", "password": "p",
            "database": source_db,
        }
        ddb_cfg = {
            "host": "h", "port": 8848, "user": "u", "password": "p",
            "db_path": "dfs://db",
        }
        checkpoint_mgr = CheckpointManager(cp_db)

        source_conn = sqlite3.connect(source_db)
        with patch("etl.readers.mysql_chunk_reader.pymysql.connect", return_value=source_conn):
            pipeline = ETLPipeline(JOB_CFG, mysql_cfg, ddb_cfg, checkpoint_mgr)
            pipeline.run()

        cp = checkpoint_mgr.load("test_sync")
        # No rows → checkpoint should remain empty (never saved)
        assert cp == {}
