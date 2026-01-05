from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple

# Data class representing a wind turbine in the farm
@dataclass(frozen=True) 
class Turbine:
    turbine_id: str # Unique identifier for the turbine
    x_m: float # X coordinate in meters
    y_m: float # Y coordinate in meters
    rated_power_kw: float = 2000.0 # Default rated power of 2000 kW

# Data class representing the wind farm layout
@dataclass(frozen=True)
class WindFarm:
    farm_id: str # Unique identifier for the wind farm
    turbines: List[Turbine] # List of turbines in the wind farm


# Function to create a grid layout of turbines in the wind farm
def make_grid_farm(
    farm_id: str = "F001", # Default farm ID
    n_rows: int = 3, # Number of rows in the grid
    n_cols: int = 4, # Number of columns in the grid
    spacing_m: float = 600.0, # Spacing between turbines in meters
    rated_power_kw: float = 2000.0, # Rated power of each turbine in kW
) -> WindFarm:
    turbines: List[Turbine] = []
    k = 0
# Create turbines in a grid layout
    for r in range(n_rows):
        for c in range(n_cols):
            k += 1
            turbines.append(
                Turbine(
                    turbine_id=f"T{k:03d}",
                    x_m=c * spacing_m,
                    y_m=r * spacing_m,
                    rated_power_kw=rated_power_kw,
                )
            )
    return WindFarm(farm_id=farm_id, turbines=turbines)