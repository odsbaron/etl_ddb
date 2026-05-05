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
            if dtype == "DATE":
                df[col] = pd.to_datetime(df[col]).dt.date
            elif dtype == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col])

        return df
