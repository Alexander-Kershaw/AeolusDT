from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from wind_farm_twin.io.state import StateStore


@dataclass
class GoldJob:
    silver_path: Path = Path("data_lake/silver")
    gold_path: Path = Path("data_lake/gold")
    state: StateStore = field(default_factory=StateStore)
    state_name: str = "gold_processed_hours"

    def run_hourly_energy(self, dt_seconds: float) -> Path:
        
        if not self.silver_path.exists():
            raise FileNotFoundError(f"Silver path not found: {self.silver_path}")
        
        # Output directory for hourly energy data defined early for safe return on an early exit
        out_dir = self.gold_path / "hourly_energy"
        out_dir.mkdir(parents=True, exist_ok=True)

        dataset = ds.dataset(
            str(self.silver_path),
            format="parquet",
            partitioning="hive",
        )
        df = dataset.to_table().to_pandas()

        if df.empty:
            print("GoldJob: no data found in silver.")
            return out_dir

        # Normalize
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

        # Hour bucket
        df["hour"] = df["event_time"].dt.floor("h")

        # partition key for gold output
        df["date"] = df["hour"].dt.strftime("%Y-%m-%d") 

        # Energy per row: kW * hours
        dt_hours = dt_seconds / 3600.0
        df["energy_kwh"] = df["power_kw"] * dt_hours

        # Downtime rows: status != RUNNING
        df["is_running"] = (df["status"] == "RUNNING")

        agg = (
            df.groupby(["sim_run_id", "farm_id", "turbine_id", "hour"], as_index=False)
              .agg(
                  wind_speed_mps_avg=("wind_speed_mps", "mean"),
                  power_kw_avg=("power_kw", "mean"),
                  energy_kwh=("energy_kwh", "sum"),
                  downtime_minutes=("is_running", lambda s: float((~s).sum()) * (dt_seconds / 60.0)),
                  bad_rows=("has_range_violation", "sum"),
                  rows=("event_time", "count"),
              )
        )

        # Recompute date from hour
        agg["date"] = agg["hour"].dt.strftime("%Y-%m-%d")

        # Incremental keys: farm_id|hour_key
        agg["hour_key"] = agg["hour"].dt.strftime("%Y-%m-%dT%H:00:00Z")
        agg["process_key"] = agg["sim_run_id"].astype(str) + "|" + agg["farm_id"].astype(str) + "|" + agg["hour_key"]

        processed = self.state.load_set(self.state_name)
        new_mask = ~agg["process_key"].isin(processed)
        agg_new = agg.loc[new_mask].copy()

        if agg_new.empty:
            print("GoldJob: no new hours to process")
            return out_dir
        

        out_table = pa.Table.from_pandas(agg_new, preserve_index=False)

        pq.write_to_dataset(
            table=out_table,
            root_path=str(out_dir),
            partition_cols=["sim_run_id", "farm_id", "date"],
            basename_template="part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        processed.update(agg_new["process_key"].unique().tolist())
        self.state.save_set(self.state_name, processed)


        return out_dir

    

    def run_farm_kpis(self, dt_seconds: float, rated_power_kw: float = 2000.0) -> Path:

        if not self.silver_path.exists():
            raise FileNotFoundError(f"Silver path not found: {self.silver_path}")

        out_dir = self.gold_path / "farm_kpis"
        out_dir.mkdir(parents=True, exist_ok=True)

        dataset = ds.dataset(str(self.silver_path), format="parquet", partitioning="hive")
        df = dataset.to_table().to_pandas()

        if df.empty:
            print("GoldJob(farm_kpis): no data found in silver")
            return out_dir

        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df["hour"] = df["event_time"].dt.floor("h")
        df["date"] = df["hour"].dt.strftime("%Y-%m-%d")

        dt_hours = dt_seconds / 3600.0
        df["energy_kwh"] = df["power_kw"] * dt_hours
        df["is_running"] = (df["status"] == "RUNNING")

        # Farm rollup per hour
        agg = (
            df.groupby(["sim_run_id", "farm_id", "hour"], as_index=False)
              .agg(
                  farm_power_kw_avg=("power_kw", "mean"),
                  farm_energy_kwh=("energy_kwh", "sum"),
                  running_rows=("is_running", "sum"),
                  total_rows=("is_running", "count"),
                  bad_rows=("has_range_violation", "sum"),
                  avg_free_wind=("wind_speed_free_mps", "mean"),
                  avg_effective_wind=("wind_speed_mps", "mean"),
              )
        )

        agg["date"] = agg["hour"].dt.strftime("%Y-%m-%d")
        agg["availability"] = agg["running_rows"] / agg["total_rows"]
        agg["avg_wake_loss_mps"] = agg["avg_free_wind"] - agg["avg_effective_wind"]
        agg["bad_row_rate"] = agg["bad_rows"] / agg["total_rows"]

        # Estimate number of turbines from rows per hour:
        # total_rows ≈ n_turbines * (3600/dt_seconds)
        rows_per_turbine_per_hour = int(3600 / dt_seconds)
        agg["n_turbines_est"] = (agg["total_rows"] / rows_per_turbine_per_hour).round().astype(int)

        # Capacity factor per hour (energy / (max possible energy))
        # Max energy in 1 hour = n_turbines * rated_power_kw * 1 hour
        agg["capacity_factor"] = agg["farm_energy_kwh"] / (agg["n_turbines_est"] * rated_power_kw * 1.0)

        # Incremental key (separate state name for this dataset)
        agg["hour_key"] = agg["hour"].dt.strftime("%Y-%m-%dT%H:00:00Z")
        agg["process_key"] = (
            agg["sim_run_id"].astype(str) + "|"
            + agg["farm_id"].astype(str) + "|"
            + agg["hour_key"]
        )

        state_name = "gold_farm_kpis_processed_hours"
        processed = self.state.load_set(state_name)
        new_mask = ~agg["process_key"].isin(processed)
        agg_new = agg.loc[new_mask].copy()

        if agg_new.empty:
            print("GoldJob(farm_kpis): no new hours to process")
            return out_dir

        out_table = pa.Table.from_pandas(agg_new, preserve_index=False)

        pq.write_to_dataset(
            table=out_table,
            root_path=str(out_dir),
            partition_cols=["sim_run_id", "farm_id", "date"],
            basename_template="part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        processed.update(agg_new["process_key"].unique().tolist())
        self.state.save_set(state_name, processed)

        return out_dir
