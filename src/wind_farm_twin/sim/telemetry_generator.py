from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

# Telemetry generator for simulating wind farm telemetry data
# Generates realistic telemetry data for turbines in a wind farm
# Uses wind field and power curve models to compute measurements

from wind_farm_twin.models.farm_layout import WindFarm
from wind_farm_twin.models.wind_field import WindField
from wind_farm_twin.models.turbine_powercurve import PowerCurve
from wind_farm_twin.models.wake_model import WakeModel


# Data class for telemetry generation

@dataclass
class TelemetryGenerator:
    farm: WindFarm #Wind farm layout
    wind_field: WindField # Wind field model
    power_curve: PowerCurve # Turbine power curve model
    wake_model: WakeModel | None = None # Optional wake model integration
    sim_run_id: str = "SIM-001" # Simulation run identifier
    source: str = "simulator" # Data source identifier
    seed: int = 123 # Seed for random number generator

    def __post_init__(self):
        self.rng = np.random.default_rng(self.seed) # Random number generator for reproducibility

    def generate(
        self,
        start_time_utc: datetime, # Start time of the simulation in UTC
        duration_hours: float, # Duration of the simulation in hours
        dt_seconds: int, # Time step between measurements in seconds
        inject_issues: bool = True, # Whether to inject data quality issues
    ) -> pd.DataFrame:

        n_steps = int(duration_hours * 3600 / dt_seconds) # Total number of time steps
        rows = []

        for step in range(n_steps):
            t = start_time_utc + timedelta(seconds=step * dt_seconds)
            t_seconds = step * dt_seconds # Time in seconds since start

            for turb in self.farm.turbines:

                free_speed, wind_dir = self.wind_field.get_wind_at_turbine(t_seconds, turb.x_m, turb.y_m) # Get free stream wind speed and direction

                if self.wake_model is not None:
                    wind_speed = self.wake_model.effective_speed(
                    free_speed_mps=free_speed,
                    wind_dir_deg=wind_dir,
                    target=turb,
                    turbines=self.farm.turbines,
    )
                else:
                    wind_speed = free_speed # No wake effects

                power_kw, status = self.power_curve.power_kw(wind_speed, turb.rated_power_kw) # Get power output and status
                
                # simple rotor speed 
                rotor_rpm = 0.0 if status != "RUNNING" else 6.0 + (min(wind_speed, 12.0) - 3.0) * (12.0 / 9.0)
                yaw_deg = wind_dir  # pretend yaw follows wind for now

                # Inject realistic data problems
                if inject_issues:
                    r = self.rng.random()

                    # 0.2% chance of missing measurement -> NaN
                    if r < 0.002:
                        wind_speed = float("nan")

                    # 0.2% chance of impossible negative wind
                    if 0.002 <= r < 0.004:
                        wind_speed = -5.0

                    # 0.15% chance of power spike
                    if 0.004 <= r < 0.0055:
                        power_kw = 99999.0

                    # 0.2% chance of duplicate event


                rows.append({
                    "event_time": t,
                    "farm_id": self.farm.farm_id,
                    "turbine_id": turb.turbine_id,
                    "wind_speed_free_mps": float(free_speed),
                    "wind_speed_mps": float(wind_speed) if wind_speed == wind_speed else wind_speed,
                    "wind_dir_deg": float(wind_dir),
                    "power_kw": float(power_kw),
                    "rotor_speed_rpm": float(rotor_rpm),
                    "yaw_deg": float(yaw_deg),
                    "status": status,
                    "sim_run_id": self.sim_run_id,
                    "ingest_time": datetime.now(timezone.utc),
                    "source": self.source,
                })

        df = pd.DataFrame(rows) # Create DataFrame from generated rows

        # Inject a few duplicates deliberately 0.2% of the time
        if inject_issues and len(df) > 0:
            dup_count = max(1, int(0.002 * len(df)))
            dup_idx = self.rng.choice(df.index, size=dup_count, replace=False)
            df = pd.concat([df, df.loc[dup_idx]], ignore_index=True)

        return df
