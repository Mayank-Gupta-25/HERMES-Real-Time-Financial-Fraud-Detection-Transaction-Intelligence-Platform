"""
Geospatial Haversine Distance and Travel Velocity Operator (`MOD-02` / `MOD-03`).

Computes great-circle distance (`haversine_distance_km`) on Earth (R = 6371.0 km)
and implied travel speed (`compute_geo_velocity` in km/h) between a customer's
prior transaction coordinates and current transaction coordinates, with safe
denominator guards for zero/sub-second time deltas and first transactions.
"""
from __future__ import annotations

import math
from typing import Optional, Tuple

EARTH_RADIUS_KM: float = 6371.0


def haversine_distance_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Calculate the great-circle distance in kilometers between two points
    on Earth specified in decimal degrees.
    """
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    delta_phi = math.radians(float(lat2) - float(lat1))
    delta_lambda = math.radians(float(lon2) - float(lon1))

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
    )
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return EARTH_RADIUS_KM * c


def compute_geo_velocity(
    curr_lat: Optional[float],
    curr_lon: Optional[float],
    curr_time_epoch: Optional[float],
    prev_lat: Optional[float],
    prev_lon: Optional[float],
    prev_time_epoch: Optional[float],
) -> Tuple[float, float]:
    """
    Compute `(geo_distance_km, travel_speed_kmph)` between a prior transaction
    and the current transaction.

    - Returns `(0.0, 0.0)` when prior coordinates or timestamps are missing
      (e.g. a customer's very first transaction).
    - Clamps `delta_seconds = max(curr_time_epoch - prev_time_epoch, 1.0)` so
      simultaneous or out-of-order timestamps never cause division by zero.
    """
    if (
        curr_lat is None
        or curr_lon is None
        or curr_time_epoch is None
        or prev_lat is None
        or prev_lon is None
        or prev_time_epoch is None
        or float(prev_time_epoch) <= 0.0
    ):
        return 0.0, 0.0

    distance_km = haversine_distance_km(
        float(prev_lat),
        float(prev_lon),
        float(curr_lat),
        float(curr_lon),
    )
    if distance_km <= 1e-6:
        return 0.0, 0.0

    delta_seconds = max(float(curr_time_epoch) - float(prev_time_epoch), 1.0)
    hours = delta_seconds / 3600.0
    speed_kmph = distance_km / hours
    return round(distance_km, 4), round(speed_kmph, 4)
