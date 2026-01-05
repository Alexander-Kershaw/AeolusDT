from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

from wind_farm_twin.models.farm_layout import Turbine

# Simple wake model to compute effective wind speed at turbines considering wakes from other turbines
# Uses an exponential decay model for wake effects
# Assumes wakes reduce wind speed based on distance and crosswind offset

def _unit_vector_from_direction_deg(direction_deg: float) -> tuple[float, float]:

    theta = math.radians(direction_deg) # Convert degrees to radians
    return math.cos(theta), math.sin(theta) # Return unit vector components

# Data class for the simple wake model

@dataclass(frozen=True)
class WakeModel:
    wake_strength: float = 0.15     # how much speed is lost directly downwind
    decay_length_m: float = 800.0   # how quickly the wake recovers with distance
    crosswind_sigma_m: float = 200.0  # how "wide" a wake is (bigger = affects more turbines)

# Method to compute effective wind speed at a target turbine considering wakes from other turbines

    def effective_speed(
        self,
        free_speed_mps: float, # Free stream wind speed in meters per second
        wind_dir_deg: float, # Wind direction in degrees
        target: Turbine, # Target turbine for which to compute effective wind speed
        turbines: Iterable[Turbine], # All turbines in the wind farm
    ) -> float:
        
        # Wind flow direction unit vector
        ux, uy = _unit_vector_from_direction_deg(wind_dir_deg)

        v = free_speed_mps # Start with free stream wind speed
        total_factor = 1.0 # Start with no speed reduction

        # Loop over all turbines to compute wake effects
        for src in turbines:
            if src.turbine_id == target.turbine_id: 
                continue

            dx = target.x_m - src.x_m 
            dy = target.y_m - src.y_m 

            # Project displacement into wind coordinates:
            # downwind distance = projection along wind flow
            downwind = dx * ux + dy * uy
            # crosswind distance = perpendicular component magnitude
            crosswind = abs(-dx * uy + dy * ux)

            # src affects target only if src is upwind (target is downwind of src)
            if downwind <= 0:
                continue

            # Wake influence decreases with crosswind offset (Gaussian-ish) and distance
            cross_factor = math.exp(-(crosswind ** 2) / (2 * (self.crosswind_sigma_m ** 2)))
            dist_factor = math.exp(-downwind / self.decay_length_m)

            loss = self.wake_strength * cross_factor * dist_factor # Speed loss factor
            total_factor *= (1.0 - loss) # Combine multiplicatively

        v_eff = max(0.0, v * total_factor) # Effective wind speed cannot be negative
        return v_eff
