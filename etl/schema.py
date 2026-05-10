MYSQL_TO_DDB_TYPES = {
    "tinyint": "INT",
    "smallint": "INT",
    "mediumint": "INT",
    "int": "INT",
    "integer": "INT",
    "bigint": "LONG",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DOUBLE",
    "numeric": "DOUBLE",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
    "char": "STRING",
    "varchar": "STRING",
    "text": "STRING",
    "tinytext": "STRING",
    "mediumtext": "STRING",
    "longtext": "STRING",
    "enum": "SYMBOL",
    "set": "STRING",
    "bit": "INT",
}


def normalize_mysql_type(column_type):
    return column_type.split("(", 1)[0].strip().lower()


def mysql_type_to_ddb(column_type, column_name=None):
    ddb_type = MYSQL_TO_DDB_TYPES.get(normalize_mysql_type(column_type), "STRING")
    if ddb_type == "STRING" and column_name and column_name.lower().endswith("code"):
        return "SYMBOL"
    return ddb_type


def to_ddb_name(name):
    if name.isupper():
        return name.lower()
    parts = name.replace("-", "_").split("_")
    if len(parts) > 1:
        return parts[0].lower() + "".join(part[:1].upper() + part[1:] for part in parts[1:])
    return name[:1].lower() + name[1:]


def to_ddb_table_name(name):
    return name.replace("-", "_").lower()


def build_field_mapping(columns):
    return [
        {
            "mysql": column["Field"],
            "dolphindb": to_ddb_name(column["Field"]),
            "type": mysql_type_to_ddb(column["Type"], column["Field"]),
        }
        for column in columns
    ]


def resolve_ddb_type(mapping, partition_column=None, hash_partition_column=None):
    column_type = mapping.get("type")
    if column_type:
        return column_type
    mysql_name = (mapping.get("mysql") or "").lower()
    ddb_name = (mapping.get("dolphindb") or "").lower()
    names = [mysql_name, ddb_name]
    if hash_partition_column and mapping.get("dolphindb") == hash_partition_column:
        return "SYMBOL"
    if partition_column and mapping.get("dolphindb") == partition_column:
        if any("date" in name and "time" not in name for name in names):
            return "DATE"
        return "TIMESTAMP"
    if any(name.endswith("code") for name in names):
        return "SYMBOL"
    if any("date" in name and "time" not in name for name in names):
        return "DATE"
    if any("time" in name or name.endswith("ts") or "timestamp" in name for name in names):
        return "TIMESTAMP"
    return "STRING"


def default_partition_columns(field_mapping, partition_column=None, hash_partition_column=None):
    time_column = partition_column or field_mapping[0]["dolphindb"]
    hash_column = hash_partition_column
    if hash_column is None:
        for mapping in field_mapping:
            if resolve_ddb_type(mapping, time_column, hash_partition_column) == "SYMBOL" and mapping["dolphindb"] != time_column:
                hash_column = mapping["dolphindb"]
                break
    return time_column, hash_column


def ddb_schema_script(db_path, table_name, field_mapping, partition_column, hash_partition_column=None, hash_buckets=50):
    resolved_field_mapping = [
        {**mapping, "type": resolve_ddb_type(mapping, partition_column, hash_partition_column)}
        for mapping in field_mapping
    ]
    names = "`" + "`".join(m["dolphindb"] for m in resolved_field_mapping)
    types = ", ".join(m["type"] for m in resolved_field_mapping)
    if hash_partition_column:
        db_script = f"""if(!existsDatabase(dbPath)){{
    dbDate = database(\"\", VALUE, date(1900.01.01)..date(2035.12.31))
    dbHash = database(\"\", HASH, [SYMBOL, {hash_buckets}])
    db = database(dbPath, COMPO, [dbDate, dbHash])
}} else {{
    db = database(dbPath)
}}"""
        partition_cols = f"`{partition_column}`{hash_partition_column}"
    else:
        db_script = f"""if(!existsDatabase(dbPath)){{
    db = database(dbPath, VALUE, date(1900.01.01)..date(2035.12.31))
}} else {{
    db = database(dbPath)
}}"""
        partition_cols = f"`{partition_column}"
    return f"""dbPath = \"{db_path}\"
{db_script}
if(!existsTable(dbPath, `{table_name})){{
    schema = table(1:0, {names}, [{types}])
    db.createPartitionedTable(schema, `{table_name}, {partition_cols})
}}
"""


def build_bootstrap_script(ddb_config, job_config):
    partition_column, hash_partition_column = default_partition_columns(
        job_config["field_mapping"],
        job_config.get("partition_column") or job_config.get("time_column"),
        job_config.get("hash_partition_column"),
    )
    return ddb_schema_script(
        ddb_config["db_path"],
        job_config["dolphindb_table"],
        job_config["field_mapping"],
        partition_column,
        hash_partition_column,
        job_config.get("hash_buckets", 50),
    )
