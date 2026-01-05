from __future__ import annotations
from dataclasses import dataclass
import numpy as np

# A simple wind field model for the wind farm
# Wind speed varies sinusoidally with position
# Each turbine experiences wind speed based on its coordinates
# Incorporates random turbulence (noise) into the wind speed calculation 

@dataclass
class WindField:
# Parameters for the wind field model
    base_speed_mps: float = 8.0
    daily_variation_mps: float = 4.0
    spatial_variation_mps: float = 0.8
    noise_std_mps: float = 1.6
    base_dir_deg: float = 220.0
    dir_noise_std_deg: float = 8.0
    seed: int = 42

# Initialize the random number generator
    def __post_init__(self):
        self.rng = np.random.default_rng(self.seed) # Random number generator for reproducibility

    def get_wind_at_turbine(self, t_seconds: float, x_m: float, y_m: float) -> tuple[float, float]:
        
        # Daily wind cycle varying sinusoidally over a 24-hour period
        daily_cycle = np.sin(2 * np.pi * (t_seconds / 3600.0) / 24.0)

        # Calculate base wind speed with daily variation
        base = self.base_speed_mps + self.daily_variation_mps * daily_cycle

        # Spatial variation bias based on turbine position
        spacial_bias = self.spatial_variation_mps * (0.0005 * x_m - 0.0003 * y_m)

        # Random turbulence noise
        speed = base + spacial_bias + self.rng.normal(0.0, self.noise_std_mps) # Add noise to wind speed
        speed = max(0.0, speed) # Ensure non-negative wind speed

        # Wind direction with some random noise
        direction = self.base_dir_deg + self.rng.normal(0.0, self.dir_noise_std_deg)
        
        # Normalize direction to [0, 360) for full circle representation
        direction = float(direction % 360.0)

        return speed, direction


        