from __future__ import annotations
import pyarrow as pa

# Define the schema for wind turbine telemetry data
# This is a bronze-level schema capturing raw telemetry information

def telemetry_arrow_schema() -> pa.Schema:
    return pa.schema([
        # time + identity
        ("event_time", pa.timestamp("ms", tz="UTC")),
        ("farm_id", pa.string()),
        ("turbine_id", pa.string()),

        # wind measurements
        ("wind_speed_free_mps", pa.float32()),
        ("wind_speed_mps", pa.float32()),
        ("wind_dir_deg", pa.float32()),

        # turbine outputs
        ("power_kw", pa.float32()),
        ("rotor_speed_rpm", pa.float32()),
        ("yaw_deg", pa.float32()),

        # operational state
        ("status", pa.string()),  # RUNNING / STOPPED / CUT_OUT
        ("sim_run_id", pa.string()),

        # ingestion metadata
        ("ingest_time", pa.timestamp("ms", tz="UTC")),
        ("source", pa.string()),
    ])
