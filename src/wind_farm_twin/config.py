from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScenarioConfig:
    sim_run_id: str
    wind_seed: int = 42
    telemetry_seed: int = 123

    # Wind field params
    base_speed_mps: float = 8.0
    daily_variation_mps: float = 2.0
    noise_std_mps: float = 0.6

    # Wake params
    wake_strength: float = 0.18
    decay_length_m: float = 900.0
    crosswind_sigma_m: float = 250.0
