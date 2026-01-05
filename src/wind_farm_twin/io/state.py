from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Set

# Simple state store to save and load sets of strings as JSON files
# Used for tracking processed items or similar stateful data
# Store hour keys like: F001|2026-01-05T03:00:00Z

@dataclass
class StateStore:
    base_path: Path = Path("data_lake/_state") # Base directory for state files

    def _path(self, name: str) -> Path:
        self.base_path.mkdir(parents=True, exist_ok=True)
        return self.base_path / f"{name}.json"

    # Load a set of strings from a JSON file    
    def load_set(self, name: str) -> Set[str]:
        p = self._path(name)
        if not p.exists():
            return set()
        with p.open("r") as f:
            data = json.load(f)
        return set(data.get("items", []))

    # Save a set of strings to a JSON file
    def save_set(self, name: str, items: Set[str]) -> None:
        p = self._path(name)
        with p.open("w") as f:
            json.dump({"items": sorted(items)}, f, indent=2)
