from scripts.generate_table_mapping import build_job, quote_mysql_identifier


def test_quote_mysql_identifier_escapes_backticks():
    assert quote_mysql_identifier("bad`name;drop") == "`bad``name;drop`"


def test_build_job_adds_compound_cursor_for_incremental_tables():
    columns = [
        {"Field": "ID", "Type": "bigint(20)"},
        {"Field": "UpdateTime", "Type": "datetime"},
        {"Field": "SecuCode", "Type": "varchar(30)"},
    ]
    indexes = [{"Key_name": "PRIMARY", "Column_name": "ID"}]

    job = build_job("AShareDescription", columns, indexes, max_batches=1)

    assert job["incremental_col"] == "UpdateTime"
    assert job["cursor_columns"] == ["UpdateTime", "ID"]
    assert job["initial_cursor"] == {
        "UpdateTime": "1900-01-01 00:00:00",
        "ID": 0,
    }
    assert "initial_last_val" not in job
    assert job["max_batches"] == 1


def test_build_job_uses_string_initial_cursor_for_string_tie_breaker():
    columns = [
        {"Field": "SecuCode", "Type": "varchar(30)"},
        {"Field": "UpdateTime", "Type": "datetime"},
    ]
    indexes = [{"Key_name": "PRIMARY", "Column_name": "SecuCode"}]

    job = build_job("CodeTable", columns, indexes)

    assert job["cursor_columns"] == ["UpdateTime", "SecuCode"]
    assert job["initial_cursor"] == {
        "UpdateTime": "1900-01-01 00:00:00",
        "SecuCode": "",
    }
