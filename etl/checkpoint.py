import sqlite3
import json
from datetime import datetime
from pathlib import Path


class CheckpointManager:
    def __init__(self, db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_table()

    def _init_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                job_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        self.conn.commit()

    def load(self, job_name: str) -> dict:
        cur = self.conn.execute(
            "SELECT checkpoint_data FROM checkpoints WHERE job_name = ?",
            (job_name,),
        )
        row = cur.fetchone()
        if row:
            return json.loads(row[0])
        return {}

    def save(self, job_name: str, data: dict):
        self.conn.execute(
            "REPLACE INTO checkpoints (job_name, checkpoint_data, updated_at) VALUES (?, ?, ?)",
            (job_name, json.dumps(data), datetime.now().isoformat()),
        )
        self.conn.commit()
