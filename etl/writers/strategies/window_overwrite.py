import pandas as pd


class WindowOverwriteStrategy:
    def __init__(self, time_col):
        self.time_col = time_col
        self.processed_batches = set()

    def is_duplicate(self, batch_id):
        return batch_id in self.processed_batches

    def pre_write(self, df, batch_id, session, db_path, table_name):
        ts_min = self._ddb_temporal_literal(df[self.time_col].min())
        ts_max = self._ddb_temporal_literal(df[self.time_col].max())
        script = f"""
            t = loadTable('{db_path}','{table_name}')
            delete from t where {self.time_col} between {ts_min} : {ts_max}
        """
        session.run(script)

    def _ddb_temporal_literal(self, value):
        timestamp = pd.Timestamp(value)
        return f"timestamp('{timestamp.strftime('%Y.%m.%dT%H:%M:%S.%f')[:-3]}')"

    def post_write(self, batch_id):
        self.processed_batches.add(batch_id)
