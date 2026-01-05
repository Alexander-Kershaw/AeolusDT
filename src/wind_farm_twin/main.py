from __future__ import annotations

from datetime import datetime, timezone
import duckdb

from wind_farm_twin.config import ScenarioConfig
from wind_farm_twin.io.bronze_writer import BronzeWriter
from wind_farm_twin.pipelines.silver_job import SilverJob
from wind_farm_twin.pipelines.gold_job import GoldJob

from wind_farm_twin.models.farm_layout import make_grid_farm
from wind_farm_twin.models.wind_field import WindField
from wind_farm_twin.models.turbine_powercurve import PowerCurve
from wind_farm_twin.models.wake_model import WakeModel
from wind_farm_twin.sim.telemetry_generator import TelemetryGenerator


def run_scenario(cfg: ScenarioConfig):
    farm = make_grid_farm(farm_id="F001", n_rows=3, n_cols=4, spacing_m=600.0, rated_power_kw=2000.0)

    wind_field = WindField(
        base_speed_mps=cfg.base_speed_mps,
        daily_variation_mps=cfg.daily_variation_mps,
        noise_std_mps=cfg.noise_std_mps,
        seed=cfg.wind_seed,
    )
    wake = WakeModel(
        wake_strength=cfg.wake_strength,
        decay_length_m=cfg.decay_length_m,
        crosswind_sigma_m=cfg.crosswind_sigma_m,
    )
    curve = PowerCurve()

    gen = TelemetryGenerator(
        farm=farm,
        wind_field=wind_field,
        power_curve=curve,
        wake_model=wake,
        sim_run_id=cfg.sim_run_id,
        seed=cfg.telemetry_seed,
    )

    start = datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc)
    df = gen.generate(start_time_utc=start, duration_hours=6, dt_seconds=60, inject_issues=True)

    BronzeWriter().write_hourly_chunks(df)
    print(f"[{cfg.sim_run_id}] Bronze hourly chunks written")


def assert_runs_exist_in_bronze(expected=("SIM-A", "SIM-B")):
    con = duckdb.connect()
    df = con.execute("""
    SELECT DISTINCT sim_run_id
    FROM read_parquet('data_lake/bronze/**/*.parquet', hive_partitioning=1)
    ORDER BY sim_run_id
    """).df()
    found = set(df["sim_run_id"].tolist())
    missing = [r for r in expected if r not in found]
    if missing:
        raise RuntimeError(f"Missing sim_run_id(s) in BRONZE: {missing}. Found: {sorted(found)}")


def assert_runs_exist_in_gold(expected=("SIM-A", "SIM-B")):
    con = duckdb.connect()
    df = con.execute("""
    SELECT DISTINCT sim_run_id
    FROM read_parquet('data_lake/gold/hourly_energy/**/*.parquet', hive_partitioning=1)
    ORDER BY sim_run_id
    """).df()
    found = set(df["sim_run_id"].tolist())
    missing = [r for r in expected if r not in found]
    if missing:
        raise RuntimeError(f"Missing sim_run_id(s) in GOLD: {missing}. Found: {sorted(found)}")


def main():
    scenarios = [
        ScenarioConfig(sim_run_id="SIM-A", wake_strength=0.12),
        ScenarioConfig(sim_run_id="SIM-B", wake_strength=0.25),
    ]

    for cfg in scenarios:
        run_scenario(cfg)

    assert_runs_exist_in_bronze(("SIM-A", "SIM-B"))

    SilverJob().run()
    print("Silver job successful")

    GoldJob().run_hourly_energy(dt_seconds=60)
    print("Gold job successful")

    GoldJob().run_farm_kpis(dt_seconds=60, rated_power_kw=2000.0)
    print("Farm KPI gold job successful")

    assert_runs_exist_in_gold(("SIM-A", "SIM-B"))
    print("Both SIM-A and SIM-B are present in Gold")


if __name__ == "__main__":
    main()
