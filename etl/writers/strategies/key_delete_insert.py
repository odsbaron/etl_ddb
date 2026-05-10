class KeyDeleteInsertStrategy:
    def __init__(self, key_columns):
        self.key_columns = key_columns
        self.processed_batches = set()

    def is_duplicate(self, batch_id):
        return batch_id in self.processed_batches

    def pre_write(self, df, batch_id, session, db_path, table_name):
        if df.empty:
            return
        t = f"loadTable('{db_path}','{table_name}')"
        if len(self.key_columns) == 1:
            key_col = self.key_columns[0]
            values = self._ddb_vector(df[key_col].dropna().unique())
            script = f"delete from {t} where {key_col} in {values}"
        else:
            predicates = []
            for _, row in df[self.key_columns].drop_duplicates().iterrows():
                parts = [f"{col}={self._ddb_literal(row[col])}" for col in self.key_columns]
                predicates.append("(" + " and ".join(parts) + ")")
            script = f"delete from {t} where " + " or ".join(predicates)
        session.run(script)

    def _ddb_vector(self, values):
        return "[" + ", ".join(self._ddb_literal(value) for value in values) + "]"

    def _ddb_literal(self, value):
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if hasattr(value, "isoformat"):
            return "timestamp('" + value.strftime("%Y.%m.%dT%H:%M:%S.%f")[:-3] + "')"
        return str(value)

    def post_write(self, batch_id):
        self.processed_batches.add(batch_id)
