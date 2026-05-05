import dolphindb as ddb
from etl.writers.base import BaseWriter


class DolphinDBWriter(BaseWriter):
    def __init__(self, ddb_config, table_name, strategy):
        self.session = ddb.Session()
        self.session.connect(
            ddb_config["host"],
            ddb_config["port"],
            ddb_config["user"],
            ddb_config["password"],
        )
        self.db_path = ddb_config["db_path"]
        self.table_name = table_name
        self.strategy = strategy

    def write_batch(self, df, batch_id: str):
        if self.strategy.is_duplicate(batch_id):
            return
        self.strategy.pre_write(df, batch_id, self.session)
        appender = ddb.TableAppender(self.table_name, self.db_path, self.session)
        appender.append(df)
        self.strategy.post_write(batch_id)

    def close(self):
        self.session.close()
