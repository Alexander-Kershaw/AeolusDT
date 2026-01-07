***
***
# AeolusDT — Wind Farm Digital Twin (Python + Lakehouse)
***
***

AeolusDT is an end-to-end wind farm digital twin implemented in Python, combining
physics-integrated wind field simulation with a modern data engineering (Bronze / Silver / Gold) lakehouse architecture.

The project simulates wind farm telemetry, processes it incrementally, and produces
analytics-ready KPIs and visualizations that enable scenario comparison, such as evaluating the impact
of different wake model assumptions on energy production.

***
***
## Key Features
***
***

### Digital Twin
***
- Wind farm layout with multiple turbines
- Time-varying wind field
- Wake interaction model
- Turbine power curve
- Scenario-based simulation (`sim_run_id`)
***
### Data Engineering Architecture
***
- **Bronze / Silver / Gold** layers
- Hive-style Parquet partitioning
- Incremental processing with persisted state
- Idempotent batch jobs
- DuckDB analytics on Parquet
***
### Analytics Outputs
***
- Turbine-level hourly energy
- Farm-level KPIs:
  - Energy production
  - Capacity factor
  - Availability
  - Wake loss
  - Data quality metrics
- Scenario comparison (e.g. SIM-A vs SIM-B)


***
***
## Architecture Overview
***
***

Simulation
|
v
Bronze (raw telemetry)

partitioned by sim_run_id / farm_id / date / hour
|
v
Silver (cleaned telemetry)

deduplicated

quality flags
|
v
Gold
├── hourly_energy (per turbine)
└── farm_kpis (per farm)

***

State for incremental processing is stored in data_lake/_state/

***
***
## Project Structure
***
***

```text
AeolusDT/
├── src/
│   └── wind_farm_twin/
│       ├── sim/            # Telemetry simulation
│       ├── models/         # Wind, wake, turbine models
│       ├── io/             # Schemas, writers, state handling
│       ├── pipelines/      # Silver / Gold data jobs
│       └── main.py
│
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── _state/
│
├── environment.yml
└── README.md
```


***
***
## Tech Stack
***
***

- Python 3.11
- pandas
- pyarrow / parquet
- DuckDB
- NumPy
- Conda

***
***


# UPDATE README AT PROJECT COMPLETION
