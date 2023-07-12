""" Module for fasthep-carpenter functions and settings """
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Settings:
    """Settings for fasthep-carpenter"""

    ncores: int = 1
    outdir: str = "output"
    quiet: bool = False
    mode: str = "multiprocessing"
    nblocks_per_dataset: int = 1
    nblocks_per_sample: int = -1
    blocksize: int = 1_000_000
    profile: bool = False
    plugins: dict[str, Any] = field(default_factory=dict)


    @classmethod
    def from_yaml(cls, filename: str) -> Settings:
        """Create settings from YAML file"""
        import yaml

        with open(filename, "r") as file:
            settings = yaml.safe_load(file)
        return cls(**settings)

    @classmethod
    def from_dict(cls, settings: dict[str, Any]) -> Settings:
        """Create settings from dictionary"""
        return cls(**settings)
