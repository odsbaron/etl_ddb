class AppendOnlyStrategy:
    def __init__(self):
        self.processed_batches = set()

    def is_duplicate(self, batch_id):
        return batch_id in self.processed_batches

    def pre_write(self, df, batch_id, session, db_path, table_name):
        pass

    def post_write(self, batch_id):
        self.processed_batches.add(batch_id)
