# MySQL to DolphinDB ETL 系统设计文档

## 一、总体架构与执行流程

整个 ETL 系统基于 Apache Airflow 进行任务编排，使用 Python 处理数据抽取、转换和加载，并通过 SQLite 记录断点实现断点续传与幂等写入。

### 架构图

```
graph TB
    subgraph "Airflow DAG"
        A[读取断点] --> B[分批抽取+转换]
        B --> C[分批写入 DolphinDB]
        C --> D[更新断点]
    end
    B --> E[(MySQL)]
    C --> F[(DolphinDB)]
    D --> G[(SQLite)]
```

### 执行流程

1. **读取断点 (CheckpointTask)** – 从 SQLite 中获取上次同步的最后位置（如最大自增ID或时间戳）。
2. **抽取与转换 (Extract & Transform)** – 根据断点从 MySQL 流式分块读取，进行字段映射、类型转换等清洗。
3. **加载 (Load)** – 采用时间窗口覆盖或 batch_id 去重策略，写入 DolphinDB 分布式表。
4. **更新断点 (UpdateCheckpoint)** – 写入成功后立即保存断点，确保任务中断后可从断点继续。
5. **异常与重试** – Airflow 自动重试失败任务，结合断点机制实现精准续跑。

## 二、项目目录结构

```
mysql_to_dolphindb_etl/
├── config/                     # 配置文件
│   ├── config.yaml             # 数据库连接、表映射、调度参数
│   └── logging.conf            # 日志配置
├── dags/                       # Airflow DAG 目录
│   └── mysql_to_dolphindb.py   # 主DAG定义
├── plugins/                    # Airflow 自定义插件（预留）
├── etl/                        # ETL 核心代码
│   ├── __init__.py
│   ├── checkpoint.py           # 断点管理器（SQLite）
│   ├── readers/
│   │   ├── __init__.py
│   │   ├── base.py             # Reader 抽象基类
│   │   └── mysql_chunk_reader.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── base.py             # 通用转换器
│   ├── writers/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── dolphindb_writer.py
│   │   └── strategies/
│   │       ├── __init__.py
│   │       └── window_overwrite.py
│   └── pipeline.py             # 管道组装器
├── tests/                      # 单元测试
├── scripts/                    # 初始化脚本
│   └── init_dolphindb.dos      # DolphinDB 库表创建脚本
├── Dockerfile
├── requirements.txt
└── README.md
```

## 三、配置文件定义 (config/config.yaml)

```yaml
mysql:
  host: 10.0.0.1
  port: 3306
  user: reader
  password: "pass"
  database: source_db

dolphindb:
  host: 10.0.0.2
  port: 8848
  user: admin
  password: "pass"
  db_path: "dfs://target_db"

checkpoint:
  db_path: "/opt/airflow/checkpoint.db"   # 断点 SQLite 文件

jobs:
  - name: order_sync
    mysql_table: orders
    dolphindb_table: orders
    incremental: true
    time_column: last_modified
    writer_strategy: window_overwrite
    parallel_chunks: 4
    chunk_size: 100000
    field_mapping:
      - mysql: order_id
        dolphindb: orderId
        type: SYMBOL
      - mysql: customer
        dolphindb: customer
        type: STRING
      - mysql: amount
        dolphindb: amt
        type: DOUBLE
      - mysql: last_modified
        dolphindb: updateTime
        type: TIMESTAMP
```

## 四、核心模块详解

### 1. 断点管理器 (etl/checkpoint.py)

```python
import sqlite3
import json
from datetime import datetime

class CheckpointManager:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_table()

    def _init_table(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS checkpoints (
                job_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                updated_at TIMESTAMP
            )
        ''')
        self.conn.commit()

    def load(self, job_name: str) -> dict:
        cur = self.conn.execute(
            "SELECT checkpoint_data FROM checkpoints WHERE job_name = ?", (job_name,))
        row = cur.fetchone()
        if row:
            return json.loads(row[0])
        return {}

    def save(self, job_name: str, data: dict):
        self.conn.execute(
            "REPLACE INTO checkpoints (job_name, checkpoint_data, updated_at) VALUES (?, ?, ?)",
            (job_name, json.dumps(data), datetime.now())
        )
        self.conn.commit()
```

### 2. MySQL 分块读取器 (etl/readers/mysql_chunk_reader.py)

```python
import pymysql
import pandas as pd
from typing import Iterator, Tuple

class MySQLChunkReader:
    def __init__(self, mysql_config, job_config, checkpoint: dict):
        self.conn = pymysql.connect(**mysql_config)
        self.table = job_config['mysql_table']
        self.chunk_size = job_config.get('chunk_size', 100000)
        self.order_col = job_config.get('incremental_col', 'id')  # 自增ID或时间戳
        self.last_val = checkpoint.get('last_val', 0)
        self.batch_seq = 0

    def read_batches(self) -> Iterator[Tuple[pd.DataFrame, str]]:
        while True:
            sql = (f"SELECT * FROM {self.table} "
                   f"WHERE {self.order_col} > '{self.last_val}' "  # 注意实际需处理类型引用
                   f"ORDER BY {self.order_col} LIMIT {self.chunk_size}")
            df = pd.read_sql(sql, self.conn)
            if df.empty:
                break
            self.last_val = df[self.order_col].max()
            self.batch_seq += 1
            batch_id = f"{self.table}_{self.batch_seq}_{self.last_val}"
            yield df, batch_id

    def get_checkpoint(self) -> dict:
        return {'last_val': self.last_val}

    def close(self):
        self.conn.close()
```

### 3. 通用转换器 (etl/transformers/base.py)

```python
import pandas as pd

class DataTransformer:
    def __init__(self, field_mapping):
        self.mapping = field_mapping

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # 字段重命名
        rename = {m['mysql']: m['dolphindb'] for m in self.mapping if m['mysql'] != m['dolphindb']}
        df = df.rename(columns=rename)

        # 类型转换
        for m in self.mapping:
            col = m['dolphindb']
            if col in df.columns:
                dtype = m.get('type')
                if dtype == 'DATE':
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif dtype == 'TIMESTAMP':
                    df[col] = pd.to_datetime(df[col])
                # 其他类型转换可按需添加
        return df
```

### 4. DolphinDB 写入器与幂等策略 (etl/writers/)

#### 写入器 (dolphindb_writer.py)

```python
import dolphindb as ddb
from dolphindb.table_appender import TableAppender

class DolphinDBWriter:
    def __init__(self, ddb_config, table_name, strategy):
        self.session = ddb.Session()
        self.session.connect(ddb_config['host'], ddb_config['port'],
                             ddb_config['user'], ddb_config['password'])
        self.db_path = ddb_config['db_path']
        self.table_name = table_name
        self.strategy = strategy

    def write_batch(self, df, batch_id: str):
        if self.strategy.is_duplicate(batch_id):
            return
        self.strategy.pre_write(df, batch_id, self.session)
        appender = TableAppender(self.table_name, self.db_path, self.session)
        appender.append(df)
        self.strategy.post_write(batch_id)

    def close(self):
        self.session.close()
```

#### 时间窗口覆盖策略 (strategies/window_overwrite.py)

```python
class WindowOverwriteStrategy:
    def __init__(self, time_col):
        self.time_col = time_col
        self.processed_batches = set()

    def is_duplicate(self, batch_id):
        return batch_id in self.processed_batches

    def pre_write(self, df, batch_id, session):
        ts_min = df[self.time_col].min()
        ts_max = df[self.time_col].max()
        table_path = f"loadTable('{self.db_path}','{self.table_name}')"
        script = f"""
            t = {table_path}
            delete from t where {self.time_col} between {ts_min} : {ts_max}
        """
        session.run(script)

    def post_write(self, batch_id):
        self.processed_batches.add(batch_id)
```

### 5. 管道调度器 (etl/pipeline.py)

```python
from etl.readers.mysql_chunk_reader import MySQLChunkReader
from etl.transformers.base import DataTransformer
from etl.writers.dolphindb_writer import DolphinDBWriter
from etl.writers.strategies.window_overwrite import WindowOverwriteStrategy

class ETLPipeline:
    def __init__(self, job_config, mysql_cfg, ddb_cfg, checkpoint_mgr):
        self.job = job_config
        self.mysql_cfg = mysql_cfg
        self.ddb_cfg = ddb_cfg
        self.checkpoint_mgr = checkpoint_mgr

    def run(self):
        cp = self.checkpoint_mgr.load(self.job['name'])
        reader = MySQLChunkReader(self.mysql_cfg, self.job, cp)
        transformer = DataTransformer(self.job['field_mapping'])
        strategy = WindowOverwriteStrategy(self.job.get('time_column', 'updateTime'))
        writer = DolphinDBWriter(self.ddb_cfg, self.job['dolphindb_table'], strategy)

        try:
            for df, batch_id in reader.read_batches():
                transformed = transformer.transform(df)
                writer.write_batch(transformed, batch_id)
                # 每处理完一块，立刻持久化断点
                self.checkpoint_mgr.save(self.job['name'], reader.get_checkpoint())
        finally:
            reader.close()
            writer.close()
```

## 五、Airflow DAG 编排

创建文件 `dags/mysql_to_dolphindb.py`：

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl.pipeline import ETLPipeline
from etl.checkpoint import CheckpointManager
import yaml

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'mysql_to_dolphindb_etl',
    default_args=default_args,
    description='MySQL to DolphinDB incremental sync',
    schedule_interval='@hourly',
    catchup=False
)

# 加载配置
with open('/opt/airflow/config/config.yaml') as f:
    config = yaml.safe_load(f)

checkpoint_mgr = CheckpointManager(config['checkpoint']['db_path'])

def create_etl_task(job_conf):
    def run_etl(**context):
        pipeline = ETLPipeline(job_conf, config['mysql'], config['dolphindb'], checkpoint_mgr)
        pipeline.run()
    return run_etl

# 为每个同步任务生成一个 Airflow Task
for job in config['jobs']:
    PythonOperator(
        task_id=f"etl_{job['name']}",
        python_callable=create_etl_task(job),
        dag=dag
    )
```

## 六、DolphinDB 表初始化 (scripts/init_dolphindb.dos)

```sql
// 创建复合分区数据库
db1 = database("", VALUE, date(2022.01.01)..date(2024.12.31))
db2 = database("", HASH, [SYMBOL, 20])
db = database("dfs://target_db", COMPO, [db1, db2])

// 定义表结构（示例）
schema = table(
    1:0,
    `orderId`customer`amt`updateTime,
    [SYMBOL, STRING, DOUBLE, TIMESTAMP]
)

// 创建分布式表
db.createPartitionedTable(schema, `orders, `updateTime`orderId)
```

## 七、部署与调度

### Docker 镜像构建 (Dockerfile)

```dockerfile
FROM apache/airflow:2.9.0-python3.10
USER root
RUN apt-get update && apt-get install -y gcc
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ /opt/airflow/config/
COPY etl/ /opt/airflow/etl/
COPY dags/ /opt/airflow/dags/
```

### 依赖清单 (requirements.txt)

```
pymysql==1.1.0
pandas==2.2.2
dolphindb==1.30.22.1
loguru==0.7.2
pyyaml==6.0
```

### 启动与挂载注意事项

- 将 `config/`、`etl/`、`dags/` 挂载到 Airflow 容器对应路径。
- 确保 SQLite 断点文件 `/opt/airflow/checkpoint.db` 所在目录具有持久化存储，避免容器重启丢失进度。
- 首次运行前，在 DolphinDB 中执行 `scripts/init_dolphindb.dos` 创建库表。

## 八、监控与日志

- **Airflow 自带监控**：通过 Flower 查看 Worker 状态，配置 StatsD 将指标接入 Prometheus + Grafana。
- **自定义日志**：在 ETL 管道中使用 loguru，日志输出到 stdout，由 Airflow 自动收集到 Task 日志中。
- **关键指标建议**：每批处理行数、处理耗时、失败批次ID，可扩展 pipeline 在捕获异常时发送告警。
