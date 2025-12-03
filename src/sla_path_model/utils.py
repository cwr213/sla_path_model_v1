"""Shared utility functions for SLA path model."""
import logging
import sys
from datetime import time
from typing import Optional

import pandas as pd


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return logger that writes to stdout."""
    logger = logging.getLogger("sla_path")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger


def parse_time_value(val) -> Optional[time]:
    """Parse a time value from Excel into a time object."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, time):
        return val

    if isinstance(val, (int, float)) and not hasattr(val, 'time'):
        if isinstance(val, float) and 0 <= val < 1:
            total_minutes = val * 24 * 60
            hour = int(total_minutes // 60)
            minute = int(total_minutes % 60)
            return time(hour, minute)

        val_int = int(val)
        if 0 <= val_int <= 24:
            return time(val_int if val_int < 24 else 0, 0)
        elif 100 <= val_int <= 2400:
            hour = val_int // 100
            minute = val_int % 100
            if hour >= 24:
                hour = 0
            return time(hour, minute)
        else:
            raise ValueError(f"Cannot parse integer time value: {val}")

    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            parts = val.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            second = int(parts[2]) if len(parts) > 2 else 0
            return time(hour, minute, second)
        except (ValueError, IndexError):
            raise ValueError(f"Cannot parse time value: {val}")

    if hasattr(val, 'time'):
        return val.time()

    raise ValueError(f"Unexpected time value type: {type(val)} = {val}")


def parse_days_of_week(val: str) -> list[str]:
    """Parse days of week from comma-separated string."""
    if not val or pd.isna(val):
        return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    valid_days = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
    days = [d.strip() for d in str(val).split(",")]

    for day in days:
        if day not in valid_days:
            raise ValueError(f"Invalid day of week: {day}. Must be one of {valid_days}")

    return days


def format_path_nodes(nodes: list[str]) -> str:
    """Format path nodes as arrow-separated string: PHL->ONT->LAX1"""
    return "->".join(nodes)


def parse_path_nodes(path_str: str) -> list[str]:
    """Parse arrow-separated path string back to list."""
    return path_str.split("->")