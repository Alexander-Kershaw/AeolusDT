from __future__ import annotations

from dataclasses import dataclass

# Data class representing the power curve of a wind turbine
# Defines how power output varies with wind speed

@dataclass(frozen=True)
class PowerCurve:
    cut_in_mps: float = 3.0 # Cut-in wind speed in meters per second
    rated_mps: float = 12.0 # Rated wind speed in meters per second
    cut_out_mps: float = 25.0 # Cut-out wind speed in meters per second

    # Method to calculate power output and status based on wind speed

    def power_kw(self, wind_speed_mps: float, rated_power_kw: float) -> tuple[float, str]:
        v = wind_speed_mps

        if v >= self.cut_out_mps:
            return 0.0, "CUT_OUT" # Turbine stops to prevent damage
        if v < self.cut_in_mps:
            return 0.0, "STOPPED" # Turbine not generating power
        if v < self.rated_mps:
            x = (v - self.cut_in_mps) / (self.rated_mps - self.cut_in_mps) # Normalized wind speed
            return float(rated_power_kw * (x ** 3)), "RUNNING" # Cubic relation for power output
        return float(rated_power_kw), "RUNNING"
