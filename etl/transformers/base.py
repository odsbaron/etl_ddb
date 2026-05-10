import pandas as pd


class DataTransformer:
    def __init__(self, field_mapping):
        self.mapping = field_mapping

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        rename = {
            m["mysql"]: m["dolphindb"]
            for m in self.mapping
            if m["mysql"] != m["dolphindb"]
        }
        df = df.rename(columns=rename)

        for m in self.mapping:
            col = m["dolphindb"]
            if col not in df.columns:
                continue
            dtype = m.get("type")
            if dtype in {"INT", "LONG", "SHORT"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype in {"DOUBLE", "FLOAT"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype in {"STRING", "SYMBOL"}:
                df[col] = df[col].astype("string")
            elif dtype == "BOOL":
                df[col] = df[col].astype("boolean")
            elif dtype == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df
