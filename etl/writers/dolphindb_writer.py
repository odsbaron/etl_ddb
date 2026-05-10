import keyring
import dolphindb as ddb

from etl.schema import build_bootstrap_script
from etl.writers.base import BaseWriter


class DolphinDBWriter(BaseWriter):
    def __init__(self, ddb_config, table_name, strategy, job_config=None):
        self.session = ddb.Session()
        password = ddb_config["password"]
        if "password_keyring" in ddb_config:
            keyring_config = ddb_config["password_keyring"]
            password = keyring.get_password(
                keyring_config["service"], keyring_config["username"]
            )
        self.session.connect(
            ddb_config["host"],
            ddb_config["port"],
            ddb_config["user"],
            password,
        )
        self.db_path = ddb_config["db_path"]
        self.table_name = table_name
        self.strategy = strategy
        self.job_config = job_config
        self._bootstrapped = False

    def bootstrap(self):
        if self._bootstrapped or not self.job_config or not self.job_config.get("bootstrap", True):
            return
        self.session.run(build_bootstrap_script({"db_path": self.db_path}, self.job_config))
        self._bootstrapped = True

    def write_batch(self, df, batch_id: str):
        if self.strategy.is_duplicate(batch_id):
            return
        self.bootstrap()
        self.strategy.pre_write(df, batch_id, self.session, self.db_path, self.table_name)
        appender = ddb.TableAppender(self.db_path, self.table_name, self.session)
        appender.append(df)
        self.strategy.post_write(batch_id)

    def close(self):
        self.session.close()
