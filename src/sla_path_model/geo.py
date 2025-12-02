"""
Geographic calculations: haversine distance, transit time, zone determination.
"""
import math
from typing import Optional

from .config import EARTH_RADIUS_MILES, MileageBand, MINUTES_PER_HOUR


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance between two points using Haversine formula.

    Args:
        lat1, lon1: Origin coordinates in degrees
        lat2, lon2: Destination coordinates in degrees

    Returns:
        Distance in miles
    """
    # Convert to radians
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_MILES * c


def get_zone_for_distance(distance_miles: float, mileage_bands: list[MileageBand]) -> Optional[MileageBand]:
    """
    Determine the zone (mileage band) for a given distance.

    Args:
        distance_miles: Distance in miles
        mileage_bands: List of MileageBand objects sorted by mileage_band_min

    Returns:
        Matching MileageBand or None if no match
    """
    for band in mileage_bands:
        if band.mileage_band_min <= distance_miles <= band.mileage_band_max:
            return band

    # If distance exceeds all bands, return the highest zone
    if mileage_bands and distance_miles > mileage_bands[-1].mileage_band_max:
        return mileage_bands[-1]

    return None


def calculate_transit_time_minutes(
    distance_miles: float,
    circuity_factor: float,
    mph: float
) -> float:
    """
    Calculate transit time in minutes.

    Args:
        distance_miles: Direct (haversine) distance
        circuity_factor: Multiplier for road distance (typically 1.2-1.4)
        mph: Average speed in miles per hour

    Returns:
        Transit time in minutes
    """
    if mph <= 0:
        raise ValueError(f"Speed must be positive, got {mph}")

    road_miles = distance_miles * circuity_factor
    hours = road_miles / mph
    return hours * MINUTES_PER_HOUR


def calculate_atw_factor(total_path_miles: float, direct_miles: float) -> float:
    """
    Calculate around-the-world factor.

    This ratio indicates how much longer the path is compared to direct routing.
    A factor of 1.0 means direct; higher values indicate indirect routing.

    Args:
        total_path_miles: Sum of all leg distances in path
        direct_miles: Direct haversine distance from origin to destination

    Returns:
        ATW factor (total_path_miles / direct_miles)
    """
    if direct_miles <= 0:
        # Same location (O=D) or very close
        return 1.0

    return total_path_miles / direct_miles


def calculate_path_distance(
    path_nodes: list[str],
    facilities: dict  # name -> Facility
) -> tuple[float, list[float]]:
    """
    Calculate total path distance and individual leg distances.

    Args:
        path_nodes: List of facility names in path order
        facilities: Dictionary of Facility objects by name

    Returns:
        (total_miles, leg_miles): Total distance and list of leg distances
    """
    if len(path_nodes) < 2:
        return 0.0, []

    leg_miles = []
    for i in range(len(path_nodes) - 1):
        origin = facilities[path_nodes[i]]
        dest = facilities[path_nodes[i + 1]]

        dist = haversine_miles(origin.lat, origin.lon, dest.lat, dest.lon)
        leg_miles.append(dist)

    return sum(leg_miles), leg_miles