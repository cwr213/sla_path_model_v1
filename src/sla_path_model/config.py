"""
Configuration: constants, enums, and dataclasses for SLA Path Model.
"""
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo


# =============================================================================
# CONSTANTS
# =============================================================================
EARTH_RADIUS_MILES = 3958.756
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
MINUTES_PER_DAY = 1440

# Default input/output paths
DEFAULT_INPUT_FILE = "data/input_sla_model_v1.xlsx"
DEFAULT_OUTPUT_FILE = "outputs/output_sla_model_v1.xlsx"


# =============================================================================
# ENUMS
# =============================================================================
class FacilityType(str, Enum):
    HUB = "hub"
    HYBRID = "hybrid"
    LAUNCH = "launch"


class SortLevel(str, Enum):
    REGION = "region"
    MARKET = "market"
    SORT_GROUP = "sort_group"


class PathType(str, Enum):
    DIRECT = "direct"
    ONE_TOUCH = "1_touch"
    TWO_TOUCH = "2_touch"
    THREE_TOUCH = "3_touch"


class FlowType(str, Enum):
    DIRECT_INJECTION = "direct_injection"
    MIDDLE_MILE = "middle_mile"


class ObjectiveType(str, Enum):
    MAXIMIZE_VOLUME_AT_SLA = "maximize_volume_at_sla"
    WEIGHTED_SLA = "weighted_sla"


class StepType(str, Enum):
    INDUCTION_SORT = "induction_sort"
    TRANSIT = "transit"
    CROSSDOCK = "crossdock"
    FULL_SORT = "full_sort"
    LAST_MILE_SORT = "last_mile_sort"


# =============================================================================
# DATACLASSES
# =============================================================================
@dataclass
class TimingParams:
    """Global timing parameters from timing_params sheet."""
    induction_sort_minutes: float
    middle_mile_crossdock_minutes: float
    middle_mile_sort_minutes: float
    last_mile_sort_minutes: float


@dataclass
class SortWindow:
    """Represents a sort window at a facility (can cross midnight)."""
    start_local: time
    end_local: time
    timezone: ZoneInfo

    def crosses_midnight(self) -> bool:
        """Check if window spans midnight (e.g., 22:00 to 06:00)."""
        return self.end_local < self.start_local

    def duration_minutes(self) -> float:
        """Calculate window duration in minutes, handling midnight crossing."""
        start_mins = self.start_local.hour * 60 + self.start_local.minute
        end_mins = self.end_local.hour * 60 + self.end_local.minute

        if self.crosses_midnight():
            # e.g., 22:00 to 06:00 = (24:00 - 22:00) + 06:00 = 8 hours
            return (MINUTES_PER_DAY - start_mins) + end_mins
        else:
            return end_mins - start_mins


@dataclass
class CPT:
    """Critical Pull Time - a departure event from a facility."""
    origin: str
    dest: str
    cpt_sequence: int
    cpt_local: time
    days_of_week: list[str]  # e.g., ["Mon", "Tue", "Wed", "Thu", "Fri"]
    timezone: ZoneInfo

    def cpt_utc_for_date(self, local_date: datetime) -> datetime:
        """Convert CPT to UTC datetime for a specific date."""
        local_dt = datetime.combine(local_date.date(), self.cpt_local)
        local_dt = local_dt.replace(tzinfo=self.timezone)
        return local_dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


@dataclass
class ServiceCommitment:
    """SLA commitment for an origin-destination pair or zone."""
    origin: str  # "*" means all
    dest: str    # "*" means all
    zone: Optional[int]  # None if origin/dest specific
    sla_days: int
    sla_buffer_days: float
    priority_weight: float

    def matches(self, origin: str, dest: str, zone: int) -> bool:
        """Check if this commitment applies to a given OD pair and zone."""
        origin_match = (self.origin == "*" or self.origin == origin)
        dest_match = (self.dest == "*" or self.dest == dest)
        zone_match = (self.zone is None or self.zone == zone)
        return origin_match and dest_match and zone_match

    def specificity_score(self) -> int:
        """
        Higher score = more specific commitment.
        Used to prioritize: specific OD > zone-based > default.
        """
        score = 0
        if self.origin != "*":
            score += 2
        if self.dest != "*":
            score += 2
        if self.zone is not None:
            score += 1
        return score


@dataclass
class MileageBand:
    """Zone definition with transit parameters."""
    zone: int
    mileage_band_min: float
    mileage_band_max: float
    circuity_factor: float
    mph: float


@dataclass
class Facility:
    """Facility with all operational parameters."""
    name: str
    facility_type: FacilityType
    lat: float
    lon: float
    timezone: ZoneInfo
    parent_hub_name: Optional[str]
    regional_sort_hub: Optional[str]
    is_injection_node: bool

    # Middle mile sort window
    mm_sort_start_local: Optional[time]
    mm_sort_end_local: Optional[time]

    # Last mile sort window (end = delivery cutoff)
    lm_sort_start_local: Optional[time]
    lm_sort_end_local: Optional[time]

    # Outbound window for CPT generation
    outbound_window_start_local: Optional[time]
    outbound_window_end_local: Optional[time]
    outbound_cpt_count: Optional[int]

    # Capacity constraints
    max_inbound_trucks_per_hour: Optional[float]
    max_outbound_trucks_per_hour: Optional[float]

    def get_mm_sort_window(self) -> Optional[SortWindow]:
        """Get middle mile sort window if defined."""
        if self.mm_sort_start_local and self.mm_sort_end_local:
            return SortWindow(
                start_local=self.mm_sort_start_local,
                end_local=self.mm_sort_end_local,
                timezone=self.timezone
            )
        return None

    def get_lm_sort_window(self) -> Optional[SortWindow]:
        """Get last mile sort window if defined."""
        if self.lm_sort_start_local and self.lm_sort_end_local:
            return SortWindow(
                start_local=self.lm_sort_start_local,
                end_local=self.lm_sort_end_local,
                timezone=self.timezone
            )
        return None

    def get_outbound_window(self) -> Optional[SortWindow]:
        """Get outbound window if defined."""
        if self.outbound_window_start_local and self.outbound_window_end_local:
            return SortWindow(
                start_local=self.outbound_window_start_local,
                end_local=self.outbound_window_end_local,
                timezone=self.timezone
            )
        return None


@dataclass
class PathCandidate:
    """A candidate path through the network."""
    origin: str
    dest: str
    path_nodes: list[str]  # e.g., ["PHL", "ONT", "LAX1"]
    path_type: PathType
    sort_level: SortLevel
    total_path_miles: float
    direct_miles: float
    atw_factor: float  # total_path_miles / direct_miles


@dataclass
class PathTimingResult:
    """Complete timing result for a path."""
    path: PathCandidate
    tit_hours: float
    sort_window_dwell_hours: float
    cpt_dwell_hours: float
    total_dwell_hours: float
    required_injection_utc: datetime
    delivery_datetime_utc: datetime
    sla_days: int
    sla_buffer_days: float
    sla_target_hours: float
    sla_met: bool
    sla_slack_hours: float
    priority_weight: float
    steps: list  # List of PathStep dataclasses


@dataclass
class PathStep:
    """Individual step in a path timing breakdown."""
    step_sequence: int
    step_type: StepType
    from_facility: Optional[str]
    to_facility: Optional[str]
    from_lat: Optional[float]
    from_lon: Optional[float]
    to_lat: Optional[float]
    to_lon: Optional[float]
    distance_miles: Optional[float]
    start_utc: datetime
    end_utc: datetime
    duration_minutes: float
    sort_window_dwell_minutes: float
    cpt_dwell_minutes: float
    total_dwell_minutes: float


@dataclass
class RunSettings:
    """Model run configuration from run_settings sheet."""
    objective_type: ObjectiveType
    max_path_touches: int  # 4 = direct + up to 3 intermediates
    max_path_atw_factor: float  # e.g., 1.5
    reference_injection_date: datetime


@dataclass
class ODDemand:
    """Demand for an origin-destination pair."""
    scenario_id: str
    origin: str
    dest: str
    pkgs_day: float
    zone: int
    flow_type: FlowType
    day_type: str  # "offpeak" or "peak"


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def time_to_minutes(t: time) -> float:
    """Convert time to minutes since midnight."""
    return t.hour * MINUTES_PER_HOUR + t.minute + t.second / 60


def minutes_to_time(minutes: float) -> time:
    """Convert minutes since midnight to time (wraps at 24 hours)."""
    minutes = minutes % MINUTES_PER_DAY
    hours = int(minutes // MINUTES_PER_HOUR)
    mins = int(minutes % MINUTES_PER_HOUR)
    secs = int((minutes % 1) * 60)
    return time(hour=hours, minute=mins, second=secs)


def parse_days_of_week(days_str: Optional[str]) -> list[str]:
    """
    Parse comma-separated day names into validated list.
    Empty/None = all days (represented as empty list).

    Examples:
        "Mon,Wed,Fri" -> ["Mon", "Wed", "Fri"]
        "" or None -> []  (means all days)
    """
    valid_days = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

    if not days_str or not days_str.strip():
        return []

    result = []
    for part in days_str.split(","):
        day = part.strip()
        if day not in valid_days:
            raise ValueError(f"Invalid day of week: '{day}'. Expected one of {sorted(valid_days)}")
        result.append(day)

    return result


def get_day_name(dt: datetime) -> str:
    """Get day name (Mon, Tue, etc.) from datetime."""
    return dt.strftime("%a")