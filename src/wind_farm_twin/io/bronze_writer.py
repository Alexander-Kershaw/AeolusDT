from __future__ import annotations

from pathlib import Path
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from wind_farm_twin.io.schemas import telemetry_arrow_schema


class BronzeWriter:
    def __init__(self, base_path: Path | str = "data_lake/bronze"):
        self.base_path = Path(base_path)

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            raise ValueError("BronzeWriter received empty dataframe")

        required = {"event_time", "farm_id", "sim_run_id", "ingest_time"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        df = df.copy()
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True).dt.floor("ms")
        df["ingest_time"] = pd.to_datetime(df["ingest_time"], utc=True).dt.floor("ms")

        df["date"] = df["event_time"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["event_time"].dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00Z")

        schema = telemetry_arrow_schema()
        telemetry_cols = schema.names
        table = pa.Table.from_pandas(df[telemetry_cols], schema=schema, preserve_index=False, safe=False)

        # Append partition cols (not in schema contract)
        table = table.append_column("date", pa.array(df["date"], type=pa.string()))
        table = table.append_column("hour", pa.array(df["hour"], type=pa.string()))

        self.base_path.mkdir(parents=True, exist_ok=True)

        # Unique filename per write = no accidental overwrites
        unique_name = f"part-{uuid.uuid4().hex}.parquet"

        pq.write_to_dataset(
            table=table,
            root_path=str(self.base_path),
            partition_cols=["sim_run_id", "farm_id", "date", "hour"],
            basename_template=unique_name.replace(".parquet", "-{i}.parquet"),
            existing_data_behavior="overwrite_or_ignore",
        )

    def write_hourly_chunks(self, df: pd.DataFrame) -> None:
        if df.empty:
            raise ValueError("Empty dataframe")

        df = df.copy()
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True).dt.floor("ms")
        df["hour"] = df["event_time"].dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00Z")

        for (_, _hour), g in df.groupby(["farm_id", "hour"], sort=True):
            self.write(g.drop(columns=["hour"]))
