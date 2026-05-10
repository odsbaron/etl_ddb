import argparse
import csv
import sys
from pathlib import Path

import keyring
import pymysql
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.schema import build_field_mapping, ddb_schema_script, to_ddb_name, to_ddb_table_name


DEFAULT_INCREMENTAL_CANDIDATES = (
    "sjqy_update_time",
    "XGRQ",
    "UpdateTime",
    "ModifiedTime",
    "InfoPublDate",
    "TradingDay",
    "EndDate",
    "ListedDate",
)
DATE_PARTITION_CANDIDATES = (
    "TradingDay",
    "EndDate",
    "InfoPublDate",
    "ListedDate",
    "sjqy_update_time",
    "XGRQ",
)
KEY_CANDIDATES = ("ID", "InnerCode", "CompanyCode", "SecuCode", "JSID")


def get_password(config):
    if "password_keyring" not in config:
        return config.get("password", "")
    keyring_config = config["password_keyring"]
    return keyring.get_password(keyring_config["service"], keyring_config["username"])


def connect_mysql(mysql_config):
    config = mysql_config.copy()
    config.pop("password_keyring", None)
    config["password"] = get_password(mysql_config)
    return pymysql.connect(**config, charset="utf8mb4", connect_timeout=30, read_timeout=7200)


def choose_column(columns, candidates):
    names = {column["Field"] for column in columns}
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def choose_incremental_column(columns):
    return choose_column(columns, DEFAULT_INCREMENTAL_CANDIDATES)


def choose_key_columns(columns, indexes):
    primary = [index["Column_name"] for index in indexes if index.get("Key_name") == "PRIMARY"]
    if primary:
        return list(dict.fromkeys(primary))
    names = {column["Field"] for column in columns}
    for candidate in KEY_CANDIDATES:
        if candidate in names:
            return [candidate]
    return [columns[0]["Field"]]


def load_tables(mysql_config):
    conn = connect_mysql(mysql_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def quote_mysql_identifier(identifier):
    return "`" + str(identifier).replace("`", "``") + "`"


def load_columns(mysql_config, table_name):
    conn = connect_mysql(mysql_config)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {quote_mysql_identifier(table_name)}")
            return cursor.fetchall()
    finally:
        conn.close()


def load_indexes(mysql_config, table_name):
    conn = connect_mysql(mysql_config)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(f"SHOW INDEX FROM {quote_mysql_identifier(table_name)}")
            return cursor.fetchall()
    finally:
        conn.close()


def table_count(mysql_config, table_name):
    conn = connect_mysql(mysql_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {quote_mysql_identifier(table_name)}")
            return cursor.fetchone()[0]
    finally:
        conn.close()


def initial_cursor_value(column):
    mysql_type = str(column.get("Type", "")).lower()
    if any(token in mysql_type for token in ("int", "decimal", "float", "double")):
        return 0
    if any(token in mysql_type for token in ("date", "time", "year")):
        return "1900-01-01 00:00:00"
    return ""


def build_job(table_name, columns, indexes=None, max_batches=None):
    indexes = indexes or []
    field_mapping = build_field_mapping(columns)
    incremental_col = choose_incremental_column(columns)
    table_name_ddb = to_ddb_table_name(table_name)
    job = {
        "name": f"{table_name_ddb}_sync",
        "mysql_table": table_name,
        "dolphindb_table": table_name_ddb,
        "incremental": True,
        "chunk_size": 100000,
        "field_mapping": field_mapping,
    }
    if incremental_col:
        key_columns = [column for column in choose_key_columns(columns, indexes) if column != incremental_col]
        cursor_columns = [incremental_col, *key_columns]
        column_by_name = {column["Field"]: column for column in columns}
        initial_cursor = {incremental_col: "1900-01-01 00:00:00"}
        for key_column in key_columns:
            initial_cursor[key_column] = initial_cursor_value(column_by_name[key_column])
        job.update({
            "incremental_col": incremental_col,
            "cursor_columns": cursor_columns,
            "initial_cursor": initial_cursor,
            "time_column": to_ddb_name(incremental_col),
            "writer_strategy": "window_overwrite",
            "where": f"{incremental_col} IS NOT NULL",
        })
    else:
        key_columns = [to_ddb_name(column) for column in choose_key_columns(columns, indexes)]
        job.update({
            "incremental_col": columns[0]["Field"],
            "initial_last_val": 0,
            "time_column": to_ddb_name(columns[0]["Field"]),
            "writer_strategy": "key_delete_insert",
            "key_columns": key_columns,
        })
    if max_batches is not None:
        job["max_batches"] = max_batches
    return job


def inventory_row(table_name, columns, indexes, row_count=None):
    incremental_col = choose_incremental_column(columns)
    partition_col = to_ddb_name(incremental_col or columns[0]["Field"])
    hash_col = choose_column(columns, ("SecuCode", "InnerCode"))
    return {
        "mysql_table": table_name,
        "dolphindb_table": to_ddb_table_name(table_name),
        "row_count": row_count if row_count is not None else "",
        "column_count": len(columns),
        "incremental_col": incremental_col or "",
        "partition_col": partition_col,
        "hash_partition_col": to_ddb_name(hash_col) if hash_col else "",
        "writer_strategy": "window_overwrite" if incremental_col else "key_delete_insert",
        "key_columns": ",".join(to_ddb_name(column) for column in choose_key_columns(columns, indexes)),
    }


def generate_table(config, table_name, max_batches=None, include_count=False):
    columns = load_columns(config["mysql"], table_name)
    indexes = load_indexes(config["mysql"], table_name)
    job = build_job(table_name, columns, indexes, max_batches)
    row_count = table_count(config["mysql"], table_name) if include_count else None
    inventory = inventory_row(table_name, columns, indexes, row_count)
    ddl = ddb_schema_script(
        config["dolphindb"]["db_path"],
        job["dolphindb_table"],
        job["field_mapping"],
        job["time_column"],
        inventory["hash_partition_col"] or None,
    )
    return job, ddl, inventory


def write_outputs(output_dir, jobs, ddl_scripts, inventory):
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "jobs.yaml").write_text(
        yaml.safe_dump({"jobs": jobs}, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    (output_dir / "create_tables.dos").write_text("\n".join(ddl_scripts), encoding="utf-8")
    with (output_dir / "table_inventory.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(inventory[0].keys()))
        writer.writeheader()
        writer.writerows(inventory)


def main():
    parser = argparse.ArgumentParser(description="Generate ETL jobs and DolphinDB schemas from MySQL tables")
    parser.add_argument("table", nargs="?")
    parser.add_argument("-c", "--config", default="config/config.yaml")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--include-count", action="store_true")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    tables = load_tables(config["mysql"]) if args.all else [args.table]
    if not tables or not tables[0]:
        parser.error("provide a table name or --all")

    jobs = []
    ddl_scripts = []
    inventory = []
    for table_name in tables:
        job, ddl, row = generate_table(config, table_name, args.max_batches, args.include_count)
        jobs.append(job)
        ddl_scripts.append(ddl)
        inventory.append(row)

    if args.output_dir:
        write_outputs(Path(args.output_dir), jobs, ddl_scripts, inventory)
        print(f"generated {len(jobs)} table mappings in {args.output_dir}")
    else:
        print(yaml.safe_dump({"jobs": jobs}, sort_keys=False, allow_unicode=True))
        print("--- dolphinDB schema ---")
        print("\n".join(ddl_scripts))
        print("--- inventory ---")
        for row in inventory:
            print(row)


if __name__ == "__main__":
    main()
