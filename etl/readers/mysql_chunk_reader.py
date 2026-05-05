import pymysql
import pandas as pd
from typing import Iterator, Tuple
from etl.readers.base import BaseReader


class MySQLChunkReader(BaseReader):
    def __init__(self, mysql_config, job_config, checkpoint: dict):
        self.conn = pymysql.connect(**mysql_config)
        self.table = job_config["mysql_table"]
        self.chunk_size = job_config.get("chunk_size", 100000)
        self.order_col = job_config.get("incremental_col", "id")
        self.last_val = checkpoint.get("last_val", 0)
        self.batch_seq = 0

    def read_batches(self) -> Iterator[Tuple[pd.DataFrame, str]]:
        while True:
            sql = (
                f"SELECT * FROM {self.table} "
                f"WHERE {self.order_col} > '{self.last_val}' "
                f"ORDER BY {self.order_col} LIMIT {self.chunk_size}"
            )
            df = pd.read_sql(sql, self.conn)
            if df.empty:
                break
            self.last_val = df[self.order_col].max()
            self.batch_seq += 1
            batch_id = f"{self.table}_{self.batch_seq}_{self.last_val}"
            yield df, batch_id

    def get_checkpoint(self) -> dict:
        return {"last_val": self.last_val}

    def close(self):
        self.conn.close()
