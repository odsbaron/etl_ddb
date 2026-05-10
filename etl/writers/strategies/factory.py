from etl.writers.strategies.append_only import AppendOnlyStrategy
from etl.writers.strategies.key_delete_insert import KeyDeleteInsertStrategy
from etl.writers.strategies.window_overwrite import WindowOverwriteStrategy


def build_write_strategy(job_config):
    strategy = job_config.get("writer_strategy", "window_overwrite")
    if strategy == "window_overwrite":
        return WindowOverwriteStrategy(job_config.get("time_column", "updateTime"))
    if strategy == "append_only":
        return AppendOnlyStrategy()
    if strategy == "key_delete_insert":
        return KeyDeleteInsertStrategy(job_config["key_columns"])
    raise ValueError(f"Unknown writer_strategy: {strategy}")
