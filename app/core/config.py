"""Configuration settings for the application (minimal).

This module provides a small Settings container used by other
components. Kept intentionally minimal for the assigned task.
"""
from dataclasses import dataclass


@dataclass
class Settings:
    DB_PATH: str = "data/app.db"
    STORAGE_PATH: str = "storage"


settings = Settings()
