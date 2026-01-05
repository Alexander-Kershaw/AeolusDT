from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pyarrow as pa
from wind_farm_twin.io.schemas import telemetry_arrow_schema


# Define constraints for telemetry data validation

@dataclass(frozen=True)
class TelemetryConstraints:
    wind_speed_range: Tuple[float, float] = (0.0, 60.0)      # m/s
    wind_dir_range: Tuple[float, float] = (0.0, 360.0)       # degrees
    rotor_rpm_range: Tuple[float, float] = (0.0, 30.0)       # rpm
    yaw_deg_range: Tuple[float, float] = (0.0, 360.0)        # degrees
    power_kw_range: Tuple[float, float] = (0.0, 10_000.0)    # kW (10 MW max sanity)

# Create an instance with default constraints

def silver_arrow_schema() -> pa.Schema:

    base = telemetry_arrow_schema() # get base telemetry schema
    # Define additional fields for silver schema
    extra = pa.schema([ 
        ("is_duplicate", pa.bool_()),
        ("has_range_violation", pa.bool_()),
        ("range_violation_fields", pa.string()), 
    ])
    return pa.schema(list(base) + list(extra)) # combine schemas
