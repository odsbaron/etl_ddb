from etl.schema import build_bootstrap_script, build_field_mapping, ddb_schema_script, default_partition_columns, mysql_type_to_ddb, resolve_ddb_type, to_ddb_name, to_ddb_table_name


def test_mysql_type_to_ddb_maps_common_types():
    assert mysql_type_to_ddb("bigint(20)") == "LONG"
    assert mysql_type_to_ddb("decimal(18,4)") == "DOUBLE"
    assert mysql_type_to_ddb("datetime") == "TIMESTAMP"
    assert mysql_type_to_ddb("varchar(30)") == "STRING"


def test_mysql_type_to_ddb_uses_symbol_for_code_columns():
    assert mysql_type_to_ddb("varchar(30)", "SecuCode") == "SYMBOL"


def test_to_ddb_name_normalizes_common_mysql_names():
    assert to_ddb_name("ID") == "id"
    assert to_ddb_name("InnerCode") == "innerCode"
    assert to_ddb_name("sjqy_update_time") == "sjqyUpdateTime"


def test_to_ddb_table_name_lowercases_table_names():
    assert to_ddb_table_name("SecuMain") == "secumain"


def test_build_field_mapping_maps_columns():
    columns = [
        {"Field": "ID", "Type": "bigint"},
        {"Field": "SecuCode", "Type": "varchar(30)"},
    ]
    assert build_field_mapping(columns) == [
        {"mysql": "ID", "dolphindb": "id", "type": "LONG"},
        {"mysql": "SecuCode", "dolphindb": "secuCode", "type": "SYMBOL"},
    ]


def test_ddb_schema_script_can_generate_composite_partition_schema():
    script = ddb_schema_script(
        "dfs://db",
        "quotes",
        [
            {"dolphindb": "tradeDate", "type": "DATE"},
            {"dolphindb": "secuCode", "type": "SYMBOL"},
        ],
        "tradeDate",
        "secuCode",
    )
    assert "COMPO" in script
    assert "`tradeDate`secuCode" in script


def test_default_partition_columns_prefers_first_symbol_hash_column():
    partition_column, hash_column = default_partition_columns([
        {"dolphindb": "updateTime", "type": "TIMESTAMP"},
        {"dolphindb": "secuCode", "type": "SYMBOL"},
        {"dolphindb": "market", "type": "INT"},
    ], "updateTime")
    assert partition_column == "updateTime"
    assert hash_column == "secuCode"


def test_resolve_ddb_type_infers_from_names_and_partitions():
    assert resolve_ddb_type({"mysql": "trade_date", "dolphindb": "tradeDate", "type": None}) == "DATE"
    assert resolve_ddb_type({"mysql": "update_time", "dolphindb": "updateTime", "type": None}) == "TIMESTAMP"
    assert resolve_ddb_type({"mysql": "secu_code", "dolphindb": "secuCode", "type": None}) == "SYMBOL"
    assert resolve_ddb_type(
        {"mysql": "id", "dolphindb": "id", "type": None},
        partition_column="id",
    ) == "TIMESTAMP"


def test_ddb_schema_script_defaults_missing_types():
    script = ddb_schema_script(
        "dfs://db",
        "quotes",
        [
            {"mysql": "id", "dolphindb": "id", "type": None},
            {"mysql": "secu_code", "dolphindb": "secuCode", "type": None},
            {"mysql": "trade_date", "dolphindb": "tradeDate", "type": None},
        ],
        "tradeDate",
        "secuCode",
    )
    assert "[STRING, SYMBOL, DATE]" in script


def test_build_bootstrap_script_uses_job_defaults():
    script = build_bootstrap_script(
        {"db_path": "dfs://db"},
        {
            "dolphindb_table": "quotes",
            "time_column": "tradeTime",
            "field_mapping": [
                {"mysql": "trade_time", "dolphindb": "tradeTime", "type": "TIMESTAMP"},
                {"mysql": "secu_code", "dolphindb": "secuCode", "type": "SYMBOL"},
            ],
        },
    )
    assert "dfs://db" in script
    assert "`quotes" in script
    assert "`tradeTime`secuCode" in script


def test_build_bootstrap_script_handles_missing_field_types():
    script = build_bootstrap_script(
        {"db_path": "dfs://db"},
        {
            "dolphindb_table": "orders",
            "time_column": "updateTime",
            "field_mapping": [
                {"mysql": "id", "dolphindb": "id", "type": None},
                {"mysql": "update_time", "dolphindb": "updateTime", "type": None},
                {"mysql": "secu_code", "dolphindb": "secuCode", "type": None},
            ],
        },
    )
    assert "[STRING, TIMESTAMP, SYMBOL]" in script
