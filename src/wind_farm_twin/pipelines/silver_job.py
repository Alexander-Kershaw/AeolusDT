from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from wind_farm_twin.io.schemas import telemetry_arrow_schema
from wind_farm_twin.io.silver_schema import TelemetryConstraints, silver_arrow_schema
from wind_farm_twin.io.state import StateStore


@dataclass
class SilverJob:
    bronze_path: Path = Path("data_lake/bronze")
    silver_path: Path = Path("data_lake/silver")
    constraints: TelemetryConstraints = TelemetryConstraints()
    state: StateStore = field(default_factory=StateStore)
    state_name: str = "silver_processed_hours"


    def _add_range_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.constraints
        df = df.copy()
        df["has_range_violation"] = False
        df["range_violation_fields"] = ""

        def flag(col: str, lo: float, hi: float):
            nonlocal df
            if col not in df.columns:
                return
            bad = df[col].isna() | (df[col] < lo) | (df[col] > hi)
            if bad.any():
                df.loc[bad, "has_range_violation"] = True
                existing = df.loc[bad, "range_violation_fields"]
                df.loc[bad, "range_violation_fields"] = existing.where(existing == "", existing + ",") + col

        flag("wind_speed_mps", *c.wind_speed_range)
        flag("wind_dir_deg", *c.wind_dir_range)
        flag("rotor_speed_rpm", *c.rotor_rpm_range)
        flag("yaw_deg", *c.yaw_deg_range)
        flag("power_kw", *c.power_kw_range)

        return df

    def run(self) -> Path:
        if not self.bronze_path.exists():
            raise FileNotFoundError(f"Bronze path not found: {self.bronze_path}")

        dataset = ds.dataset(str(self.bronze_path),format="parquet",partitioning="hive")

        table = dataset.to_table()  # local read-all for now
        df = table.to_pandas()

        if df.empty:
            raise ValueError("No data found in bronze.")

        # Normalize timestamps
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True).dt.floor("ms")
        df["ingest_time"] = pd.to_datetime(df["ingest_time"], utc=True).dt.floor("ms")

        # Determine hour key
        df["hour_key"] = df["event_time"].dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00Z")
        df["process_key"] = (df["sim_run_id"].astype(str) + "|" + df["farm_id"].astype(str) + "|" + df["hour_key"])

        processed = self.state.load_set(self.state_name)
        new_mask = ~df["process_key"].isin(processed)
        df_new = df.loc[new_mask].copy()

        if df_new.empty:
            print("SilverJob: no new hours to process")
            return self.silver_path 
        
        
        df = df_new

        # Ensure flag columns always exist (even if no duplicates / no violations)
        df["is_duplicate"] = False
        df["has_range_violation"] = False
        df["range_violation_fields"] = ""


        # Partition key
        df["date"] = df["event_time"].dt.strftime("%Y-%m-%d")

        # Deduplicate per unique telemetry key
        key_cols = ["event_time", "farm_id", "turbine_id"]
        df = df.sort_values(key_cols)
        df["is_duplicate"] = df.duplicated(subset=key_cols, keep="first")
        df = df.drop_duplicates(subset=key_cols, keep="first")

        # Range flags
        df = self._add_range_flags(df)

        # Write to Silver: keep contract columns + flags
        schema = silver_arrow_schema()
        silver_cols = schema.names

        # Create Arrow table from expected columns
        out = pa.Table.from_pandas(df[silver_cols], schema=schema, preserve_index=False, safe=False)

        # Append partition column 'date' for dataset partitioning (like we did in bronze)
        out = out.append_column("date", pa.array(df["date"], type=pa.string()))

        self.silver_path.mkdir(parents=True, exist_ok=True)

        pq.write_to_dataset(
            table=out,
            root_path=self.silver_path,
            partition_cols=["sim_run_id", "farm_id", "date"],
            basename_template="part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        processed.update(df["process_key"].unique().tolist())
        self.state.save_set(self.state_name, processed)


        return self.silver_path
