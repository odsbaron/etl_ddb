from etl.readers.mysql_chunk_reader import MySQLChunkReader
from etl.transformers.base import DataTransformer
from etl.writers.dolphindb_writer import DolphinDBWriter
from etl.writers.strategies.factory import build_write_strategy


class ETLPipeline:
    def __init__(self, job_config, mysql_cfg, ddb_cfg, checkpoint_mgr):
        self.job = job_config
        self.mysql_cfg = mysql_cfg
        self.ddb_cfg = ddb_cfg
        self.checkpoint_mgr = checkpoint_mgr

    def run(self):
        cp = self.checkpoint_mgr.load(self.job["name"])
        reader = MySQLChunkReader(self.mysql_cfg, self.job, cp)
        transformer = DataTransformer(self.job["field_mapping"])
        strategy = build_write_strategy(self.job)
        writer = DolphinDBWriter(
            self.ddb_cfg,
            self.job["dolphindb_table"],
            strategy,
            self.job,
        )

        try:
            for df, batch_id in reader.read_batches():
                transformed = transformer.transform(df)
                writer.write_batch(transformed, batch_id)
                self.checkpoint_mgr.save(
                    self.job["name"], reader.get_checkpoint()
                )
        finally:
            reader.close()
            writer.close()
