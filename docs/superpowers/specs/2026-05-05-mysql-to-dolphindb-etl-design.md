# MySQL to DolphinDB ETL System Spec

> **Status:** Approved
> **Created:** 2026-05-05
> **Priority target:** Data consistency — zero data loss, zero duplication

## 1. Problem Statement

将 MySQL 源库中的数据增量同步到 DolphinDB 分布式表中。系统需要处理以下约束：

- **大数据量**：单表可达千万级行，必须分批流式处理。
- **中断恢复**：Airflow Task 可能因 OOM、网络闪断、Worker 重启等原因中断。中断后必须从断点精确续跑，不丢数据也不产生重复。
- **幂等安全**：已写入的批次即使被重放也不能产生重复行——这是数据一致性的硬要求。
- **多表扩展**：配置驱动，新增同步任务只需加 YAML 配置，不改代码。

### Success Criteria

| Criteria | Measurement |
|----------|-------------|
| 零数据丢失 | 断点恢复后 MySQL 中所有增量数据均被写入 DolphinDB |
| 零重复 | 同一批数据重放后，DolphinDB 表行数与增量行数一致 |
| 断点续跑 | 模拟进程 kill 后重启，自动从最后一组已写入的 chunk 恢复 |
| 水平扩展 | 新增同步表只需在 config.yaml 中新增 job 定义 |

### Non-Goals

- 不负责全量历史数据的一次性迁移（只做增量同步）。
- 不做实时 CDC（不监听 binlog）。
- 不做双向同步或反向回流。
- 不做 DDL 变更的自动感知。

## 2. Architecture

```
graph TB
    subgraph "Airflow DAG — @hourly"
        A[CheckpointManager.load] --> B[MySQLChunkReader]
        B --> C[DataTransformer]
        C --> D[DolphinDBWriter]
        D --> E[CheckpointManager.save]
    end
    B --> F[(MySQL)]
    C --> G["field_mapping config"]
    D --> H[(DolphinDB)]
    E --> I[(SQLite)]
```

### Component Responsibility Matrix

| Component | Responsibility |
|-----------|---------------|
| `CheckpointManager` | 读写 SQLite 中的断点（job_name → last_val）。提供 `load()` 和 `save()`。 |
| `MySQLChunkReader` | 从断点位置开始，以 chunk_size 为单位流式读取 MySQL 增量行，yield (DataFrame, batch_id)，并持续追踪当前 last_val。 |
| `DataTransformer` | 按 field_mapping 做列重命名 + 类型转换（STRING → SYMBOL，DATETIME → TIMESTAMP 等），输出 DolphinDB 格式 DataFrame。 |
| `DolphinDBWriter` | 负责写入。委托 strategy 做 pre_write 幂等清理和 post_write 标记，然后通过 TableAppender 追加数据。 |
| `WriterStrategy` | 幂等策略接口。当前实现为 WindowOverwriteStrategy。策略拥有 `is_duplicate` / `pre_write` / `post_write` 三个方法。 |
| `ETLPipeline` | 组装以上组件，协调流式处理循环：读 → 转换 → 幂等写 → 立即存断点。 |
| Airflow DAG | 每小时触发一次，为每个 job 创建一个 PythonOperator。 |

### Key Design Decisions

1. **断点和写入不拆成两个 Task**：断点在同一个 PythonOperator 内保存，避免 Airflow 将 "写成功但断点未保存" 的中间状态当作可恢复点。DAG 结构足够简单——每个 job 一个 operator。

2. **Writer 持有 Strategy，不持有策略细节**：DolphinDBWriter 只调用 strategy 的三个方法，不知道 window_overwrite、batch_id_dedup 等策略细节。新增策略只需实现接口，不改 Writer。

3. **batch_id 作为内存去重 + 断点恢复的双重保险**：即使进程未重启，processed_batches set 阻止同批次重放；即使重启后 set 丢失，断点的 last_val 保证不会重读已写数据。

4. **时间窗口覆盖而非 batch_id 去重**：选 window_overwrite 而非单纯依赖 batch_id，原因是数据可能在两批次之间存在交叠时间范围，简单的 append + id 去重无法处理 "同一时间窗口内部分行已变更" 的场景。

## 3. Data Flow

### Normal Flow (一次 Airflow Trigger)

```
Airflow Scheduler fires DAG
  → PythonOperator.__call__()
    → ETLPipeline.run()
      → CheckpointManager.load("order_sync") → {"last_val": "2025-03-01 12:00:00"}
      → MySQLChunkReader.read_batches()
          Batch 1:  SELECT * FROM orders WHERE last_modified > '2025-03-01 12:00:00'
                    ORDER BY last_modified LIMIT 100000
                    → DataFrame(100000 rows), batch_id="orders_1_2025-03-01 13:00:00"
          Batch 2:  ...WHERE last_modified > '2025-03-01 13:00:00'...
                    → DataFrame(85000 rows), batch_id="orders_2_2025-03-01 14:30:00"
          Batch 3:  ...WHERE last_modified > '2025-03-01 14:30:00'...
                    → DataFrame(0 rows) → break
      → For each batch:
          transformed = DataTransformer.transform(df)
          DolphinDBWriter.write_batch(transformed, batch_id)
            → strategy.is_duplicate(batch_id)? No
            → strategy.pre_write: DELETE FROM t WHERE updateTime BETWEEN ts_min AND ts_max
            → TableAppender.append(df)
            → strategy.post_write: mark batch_id as processed
          CheckpointManager.save("order_sync", {"last_val": "2025-03-01 14:30:00"})
```

### Failure Recovery Flow (进程被 Kill 后重启)

```
Airflow retries the failed Task
  → CheckpointManager.load("order_sync") → {"last_val": "2025-03-01 13:00:00"}
  → MySQLChunkReader starts FROM last_val = '2025-03-01 13:00:00'
  → Re-reads batch starting after 13:00
  → WindowOverwriteStrategy: DELETE ... WHERE updateTime BETWEEN [13:00, 14:30]
  → Re-writes the same time window (幂等 — 覆盖旧数据)
  → Continues normally
```

注意：这里依靠的是 `WHERE incremental_col > last_val`（严格大于），不是 `>=`。因此 last_val 对应的记录已经完成写入，下一 batch 从严格大于它的位置开始，不会重复读。

### What Happens When There Are No Changes

```
MySQLChunkReader first query returns empty DataFrame
  → pipeline iterator exits immediately
  → no writes, no checkpoint update
  → Task succeeds (no-op run)
```

## 4. Component Specifications

### 4.1 CheckpointManager (`etl/checkpoint.py`)

**Contract:**
```
load(job_name: str) → dict
save(job_name: str, data: dict) → None
```

**Storage schema (SQLite):**
```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    job_name TEXT PRIMARY KEY,
    checkpoint_data TEXT,     -- JSON blob, e.g. {"last_val": "2025-03-01 13:00:00"}
    updated_at TIMESTAMP
);
```

**States:**
- 首次运行：`load()` 返回 `{}`。Reader 从 `order_col` 的最小合法值开始。
- 正常运行：`save()` 在每个 batch 写成功后立即调用。
- 异常中断：最后一次成功的 `save()` 对应的断点保留在 SQLite 中。

**Edge cases:**
- SQLite 文件不存在 → `sqlite3.connect` 自动创建，`_init_table` 建表。
- 并发写入同一 job_name → Airflow 不会为同一个 job 并行调度，不存在并发场景。
- 不同 jobs 共享同一 SQLite 文件 → 各自以 job_name 为 key，互不冲突。

### 4.2 MySQLChunkReader (`etl/readers/mysql_chunk_reader.py`)

**Contract:**
```
read_batches() → Iterator[Tuple[pd.DataFrame, str]]
get_checkpoint() → dict
close() → None
```

**SQL template (parameterization to be handled in implementation):**
```sql
SELECT * FROM {table}
WHERE {order_col} > {last_val}
ORDER BY {order_col}
LIMIT {chunk_size}
```

**Key invariant:** `last_val` 始终追踪已成功 yield 的最大 order_col 值。只有 DataFrame 非空时才更新 last_val。

**Edge cases:**
- `order_col` 在游标范围内有大量相同值（如时间戳精度不足） → `LIMIT` 保证每批大小可控，`>` 保证推进，重复时间戳行会拆分到不同 batch，靠 window_overwrite 策略的去重能力在写入侧处理。
- MySQL 连接超时 → pymysql 抛出 OperationalError，向上传播到 Airflow 重试层。
- DataFrame 为空 → 循环退出，不更新 last_val。

### 4.3 DataTransformer (`etl/transformers/base.py`)

**Contract:**
```
transform(df: pd.DataFrame) → pd.DataFrame
```

**Operations (in order):**
1. 列重命名：`{mysql_col: dolphindb_col}`，仅 rename 名称不同的列。
2. 类型转换：按 mapping 中的 `type` 字段处理。仅处理在 DataFrame 中存在的列（不在 source 中的列跳过）。

**Supported type conversions (V1):**

| Mapping Type | Conversion |
|-------------|------------|
| `DATE` | `pd.to_datetime(col).dt.date` |
| `TIMESTAMP` | `pd.to_datetime(col)` |
| 未指定 type | 保持原样透传 |

**Edge cases:**
- NULL 值 → pandas 原生处理，保持为 NaN/NaT。
- 转换失败（如非法日期字符串） → pandas 抛出异常，向上传播。

### 4.4 DolphinDBWriter & Write Strategies

#### Writer (`etl/writers/dolphindb_writer.py`)

**Contract:**
```
write_batch(df: pd.DataFrame, batch_id: str) → None
close() → None
```

**Flow for each batch:**
```
1. if strategy.is_duplicate(batch_id): return    # 内存级去重
2. strategy.pre_write(df, batch_id, session)      # 幂等前置清理
3. TableAppender.append(df)                       # 实际写入
4. strategy.post_write(batch_id)                  # 标记完成
```

#### WindowOverwriteStrategy (`etl/writers/strategies/window_overwrite.py`)

**State:**
- `time_col`: 用于窗口计算的时间列名
- `processed_batches: set[str]`: 当前进程内已完成的 batch_id

**Contract:**
```
is_duplicate(batch_id: str) → bool
pre_write(df, batch_id, session) → None    # DELETE rows in [min(time_col), max(time_col)]
post_write(batch_id) → None                # mark as processed
```

**pre_write semantics:**
```python
ts_min = df[time_col].min()
ts_max = df[time_col].max()
# Run on DolphinDB:
# DELETE FROM {table} WHERE {time_col} BETWEEN ts_min : ts_max
```

**幂等保证：**
- 同 batch 重放 → `is_duplicate` 在内存中拦截（进程内）。
- 进程重启后同时间窗口数据重放 → `pre_write` 的 DELETE 先清空窗口，再 append，保证覆盖不乱序。

**Edge cases:**
- `time_col` 在 DataFrame 中不存在 → AttributeError，向上传播。
- DELETE 覆盖范围可能影响恰好边界值的数据 → BETWEEN 是闭区间，这是预期行为。边界时间点若有其他写入也属于同一窗口，覆盖是正确的。
- 多个 batch 共享同一时间窗口 → 已按 order_col 有序 + LIMIT 拆分，不共享。

### 4.5 ETLPipeline (`etl/pipeline.py`)

**Lifecycle:**
```
__init__(job_config, mysql_cfg, ddb_cfg, checkpoint_mgr)
run() → None
```

`run()` 是唯一公开方法，内部完整管理 reader/transformer/writer 的创建与销毁。

**Checkpoint save timing:** 每 batch 写成功后立即 `save()`，使用 reader 的当前 last_val。这确保中断后断点正好指向最后一个完整写入的 batch。

## 5. Configuration Surface

### 5.1 Static Config (`config/config.yaml`)

```yaml
# MySQL source connection
mysql:
  host: 10.0.0.1
  port: 3306
  user: reader
  password: "pass"
  database: source_db

# DolphinDB target connection
dolphindb:
  host: 10.0.0.2
  port: 8848
  user: admin
  password: "pass"
  db_path: "dfs://target_db"

# Checkpoint persistence
checkpoint:
  db_path: "/opt/airflow/checkpoint.db"

# Sync job definitions (array: one entry per table)
jobs:
  - name: order_sync              # unique job identifier
    mysql_table: orders           # source table in MySQL
    dolphindb_table: orders       # target table in DolphinDB
    incremental: true             # must be true for chunk reader
    time_column: last_modified    # used by window_overwrite strategy
    writer_strategy: window_overwrite
    chunk_size: 100000            # rows per batch
    field_mapping:                # column-level mapping
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

### 5.2 Config Validation (to be implemented)

At startup, the DAG file or pipeline should validate:
- Required top-level keys: `mysql`, `dolphindb`, `checkpoint`, `jobs`.
- Each job has: `name`, `mysql_table`, `dolphindb_table`, `field_mapping`.
- `mysql_table` and `dolphindb_table` are non-empty strings.
- `field_mapping` is a non-empty array.
- `writer_strategy` maps to an available strategy.

Validation failures raise clear errors before any DB connection is established.

## 6. Error Handling Strategy

| Layer | Error | Behavior |
|-------|-------|----------|
| MySQL connection | `OperationalError` | Raised to Airflow → Task marked FAILED → retry (up to 2) |
| MySQL query timeout | `OperationalError` | Same as above |
| Transform failure | `ValueError` / `TypeError` | Not retried for bad data row — but our T is schema-mapped, so bad data means a source schema change, which should fail noisily. |
| DolphinDB write failure | `IOException` / connection error | Raised to Airflow → Task FAILED → retry. 断点未更新 → retry 时从上次成功断点续跑。 |
| DolphinDB DELETE (pre_write) failure | connection error | 同上 |
| SQLite checkpoint save failure | `sqlite3.Error` | 写入成功了但断点没存 → retry 时会重复写入同一窗口，window_overwrite 覆盖同时间窗口 → 幂等。 |

### Retry Behavior

Retry 等于重新执行 ETLPipeline.run()。流程如下：
1. load 断点 → 获得上次成功的 last_val。
2. Reader 从 last_val 之后开始读 → 重跑未持久化的区间。
3. Writer 的 window_overwrite 覆盖同窗口 → 幂等写入。

**唯一不安全的情况：** MySQL 读取成功、DolphinDB DELETE 成功、DolphinDB append 成功、但 SQLite save 断点失败且进程 crash。此时重试会再次 DELETE 同窗口，然后 append。数据不丢不重——这就是窗口覆盖策略的幂等保证。

## 7. Testing Strategy

### Unit Tests

| Module | What to Test |
|--------|-------------|
| `CheckpointManager` | load returns {} for unknown job; save + load round-trip; REPLACE overwrites existing |
| `DataTransformer` | rename only diff columns; TIMESTAMP conversion; DATE conversion; missing col skipped; untyped col passed through |
| `WindowOverwriteStrategy` | is_duplicate blocks known batch_id; post_write adds to set; pre_write generates correct DELETE script |
| `MySQLChunkReader` | (mock pymysql) yields correct num batches for given row count; advances last_val correctly; empty result stops iteration |

### Integration Tests

| Scenario | Validation |
|----------|-----------|
| Fresh sync (no checkpoint) | All source rows arrive in DolphinDB |
| Resume after kill mid-batch | Total row count matches; no duplicates |
| No changes since last sync | 0 rows written; checkpoint unchanged |
| Window overlap (same time_col values across batches) | Final DolphinDB row count equals distinct source rows |

### Test data fixture

Use a small SQLite :memory: database as a MySQL stand-in for unit tests. For integration tests, spin up a Docker DolphinDB or use a mock session.

## 8. Deployment

### Docker Image

```dockerfile
FROM apache/airflow:2.9.0-python3.10
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ /opt/airflow/config/
COPY etl/ /opt/airflow/etl/
COPY dags/ /opt/airflow/dags/
```

### Pre-flight Checklist

1. DolphinDB 中已执行 `scripts/init_dolphindb.dos` 创建分区库和分布式表。
2. SQLite 断点文件目录有持久卷挂载。
3. MySQL 账号有对应表的 SELECT 权限。
4. DolphinDB 账号有表的读写 + DELETE 权限。

## 9. Open Risks

1. **Schema drift** — MySQL 列被删除或重命名后，Reader 读取时会因 `SELECT *` 拿到新结构，Transform 因 field_mapping 中的旧列名不存在而静默跳过 rename、静默跳过类型转换，写入可能携带未映射的裸列。建议后续版本增加 schema 校验环节。
2. **Clock skew** — 若 MySQL `time_column` 来自应用服务器且不同机器时钟不同步，`>` 断点可能漏数据或陷入死循环。建议以 MySQL 服务器时间为准或使用自增 ID。
3. **Memory pressure** — chunk_size=100000 对宽表可能撑爆 Worker 内存。建议初期根据列宽调小或在 Reader 中显式 SELECT 需要的列而非 `*`。
