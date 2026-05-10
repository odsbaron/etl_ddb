import datetime as dt
from typing import Iterator, Tuple

import keyring
import pandas as pd
import pymysql

from etl.readers.base import BaseReader


class MySQLChunkReader(BaseReader):
    def __init__(self, mysql_config, job_config, checkpoint: dict):
        mysql_config = mysql_config.copy()
        if "password_keyring" in mysql_config:
            keyring_config = mysql_config.pop("password_keyring")
            mysql_config["password"] = keyring.get_password(
                keyring_config["service"], keyring_config["username"]
            )
        self.conn = pymysql.connect(**mysql_config)
        self.table = job_config["mysql_table"]
        self.chunk_size = job_config.get("chunk_size", 100000)
        self.order_col = job_config.get("incremental_col", "id")
        self.cursor_columns = self._cursor_columns(job_config)
        self.initial_cursor = self._initial_cursor(job_config)
        self.last_cursor = self._load_cursor(checkpoint)
        self.batch_seq = 0
        self.max_batches = job_config.get("max_batches")
        mapped_columns = [m["mysql"] for m in job_config.get("field_mapping", [])]
        self.columns = mapped_columns + [
            col for col in self.cursor_columns if col not in mapped_columns
        ]
        self.where = job_config.get("where")

    def read_batches(self) -> Iterator[Tuple[pd.DataFrame, str]]:
        while True:
            if self.max_batches is not None and self.batch_seq >= self.max_batches:
                break
            columns = ", ".join(self._quote_identifier(column) for column in self.columns) if self.columns else "*"
            predicates = []
            cursor_predicate = self._cursor_predicate()
            if cursor_predicate:
                predicates.append(cursor_predicate)
            if self.where:
                predicates.append(f"({self.where})")
            where_clause = f"WHERE {' AND '.join(predicates)} " if predicates else ""
            order_by = ", ".join(self._quote_identifier(column) for column in self.cursor_columns)
            sql = (
                f"SELECT {columns} FROM {self._quote_identifier(self.table)} "
                f"{where_clause}"
                f"ORDER BY {order_by} LIMIT {self.chunk_size}"
            )
            df = pd.read_sql(sql, self.conn)
            if df.empty:
                break
            self.last_cursor = {
                column: df.iloc[-1][column]
                for column in self.cursor_columns
                if column in df.columns
            }
            self.batch_seq += 1
            batch_id = f"{self.table}_{self.batch_seq}_{self.last_cursor.get(self.order_col)}"
            yield df, batch_id

    def get_checkpoint(self) -> dict:
        cursor = {
            column: self._checkpoint_value(value)
            for column, value in self.last_cursor.items()
        }
        return {
            "last_val": cursor.get(self.order_col),
            "last_cursor": cursor,
        }

    def _cursor_columns(self, job_config):
        cursor_columns = job_config.get("cursor_columns") or [self.order_col]
        if self.order_col not in cursor_columns:
            cursor_columns = [self.order_col, *cursor_columns]
        elif cursor_columns[0] != self.order_col:
            cursor_columns = [self.order_col, *[col for col in cursor_columns if col != self.order_col]]
        return cursor_columns

    def _initial_cursor(self, job_config):
        initial_cursor = dict(job_config.get("initial_cursor", {}))
        if "initial_last_val" in job_config:
            initial_cursor.setdefault(self.order_col, job_config["initial_last_val"])
        return initial_cursor

    def _load_cursor(self, checkpoint):
        if not checkpoint:
            return {}
        if checkpoint.get("last_cursor"):
            cursor = dict(checkpoint["last_cursor"])
            if self.order_col not in cursor and "last_val" in checkpoint:
                cursor[self.order_col] = checkpoint["last_val"]
            return cursor
        if "last_val" in checkpoint:
            return {self.order_col: checkpoint["last_val"]}
        return {}

    def _cursor_predicate(self):
        cursor = self.last_cursor or self.initial_cursor
        if not cursor:
            return None
        predicates = []
        equals = []
        for column in self.cursor_columns:
            if column not in cursor:
                break
            literal = self._sql_literal(cursor[column])
            quoted = self._quote_identifier(column)
            predicates.append("(" + " AND ".join([*equals, f"{quoted} > {literal}"]) + ")")
            equals.append(f"{quoted} = {literal}")
        if not predicates:
            return None
        return "(" + " OR ".join(predicates) + ")"

    def _checkpoint_value(self, value):
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if hasattr(value, "item"):
            value = value.item()
        if isinstance(value, dt.datetime):
            return value.isoformat(sep=" ")
        if isinstance(value, dt.date):
            return value.isoformat()
        return value

    def _sql_literal(self, value):
        value = self._checkpoint_value(value)
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _quote_identifier(self, identifier):
        return ".".join(
            f"`{part.replace('`', '``')}`"
            for part in identifier.split(".")
        )

    def close(self):
        self.conn.close()
