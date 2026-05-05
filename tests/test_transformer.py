import pandas as pd
import pytest
from etl.transformers.base import DataTransformer


MAPPING = [
    {"mysql": "order_id", "dolphindb": "orderId", "type": "SYMBOL"},
    {"mysql": "customer", "dolphindb": "customer", "type": "STRING"},
    {"mysql": "amount", "dolphindb": "amt", "type": "DOUBLE"},
    {"mysql": "created_at", "dolphindb": "createTime", "type": "TIMESTAMP"},
    {"mysql": "order_date", "dolphindb": "orderDate", "type": "DATE"},
    {"mysql": "raw_col", "dolphindb": "raw_col", "type": None},
]


class TestDataTransformer:
    @pytest.fixture
    def transformer(self):
        return DataTransformer(MAPPING)

    def test_renames_columns_where_names_differ(self, transformer):
        df = pd.DataFrame({"order_id": [1], "customer": ["a"], "amount": [10.0],
                           "created_at": ["2025-01-01 12:00:00"],
                           "order_date": ["2025-01-01"],
                           "raw_col": ["keep"]})
        result = transformer.transform(df)
        assert "orderId" in result.columns
        assert "amt" in result.columns
        assert "createTime" in result.columns
        assert "orderDate" in result.columns
        assert "order_id" not in result.columns
        assert "amount" not in result.columns
        assert "created_at" not in result.columns
        assert "order_date" not in result.columns

    def test_keeps_same_name_columns_unchanged(self, transformer):
        df = pd.DataFrame({"customer": ["a"], "raw_col": ["keep"]})
        result = transformer.transform(df)
        assert "customer" in result.columns
        assert "raw_col" in result.columns

    def test_converts_timestamp_column(self, transformer):
        df = pd.DataFrame({"created_at": ["2025-06-15 08:30:00"]})
        result = transformer.transform(df)
        assert pd.api.types.is_datetime64_any_dtype(result["createTime"])

    def test_converts_date_column(self, transformer):
        df = pd.DataFrame({"order_date": ["2025-12-25"]})
        result = transformer.transform(df)
        assert result["orderDate"].iloc[0].__class__.__name__ == "date"

    def test_untyped_column_passes_through(self, transformer):
        df = pd.DataFrame({"raw_col": [42]})
        result = transformer.transform(df)
        assert result["raw_col"].iloc[0] == 42

    def test_skips_mapped_column_not_present_in_df(self, transformer):
        df = pd.DataFrame({"customer": ["a"]})
        result = transformer.transform(df)
        assert "orderId" not in result.columns
        assert "customer" in result.columns
