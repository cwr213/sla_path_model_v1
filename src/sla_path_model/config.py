"""Configuration: constants, enums, and dataclasses for SLA Path Model."""
from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo


EARTH_RADIUS_MILES = 3958.756
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
MINUTES_PER_DAY = 1440

DEFAULT_INPUT_FILE = "data/input_sla_model_v1.xlsx"
DEFAULT_OUTPUT_FILE = "outputs/output_sla_model_v1.xlsx"


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


@dataclass
class TimingParams:
    induction_sort_minutes: float
    middle_mile_crossdock_minutes: float
    middle_mile_sort_minutes: float
    last_mile_sort_minutes: float


@dataclass
class SortWindow:
    start_local: time
    end_local: time
    timezone: ZoneInfo

    def crosses_midnight(self) -> bool:
        return self.end_local < self.start_local

    def duration_minutes(self) -> float:
        start_mins = self.start_local.hour * 60 + self.start_local.minute
        end_mins = self.end_local.hour * 60 + self.end_local.minute

        if self.crosses_midnight():
            return (MINUTES_PER_DAY - start_mins) + end_mins
        else:
            return end_mins - start_mins


@dataclass
class CPT:
    origin: str
    dest: str
    cpt_sequence: int
    cpt_local: time
    days_of_week: list[str]
    timezone: ZoneInfo
    is_active: bool

    def cpt_utc_for_date(self, local_date: datetime) -> datetime:
        local_dt = datetime.combine(local_date.date(), self.cpt_local)
        local_dt = local_dt.replace(tzinfo=self.timezone)
        return local_dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


@dataclass
class ServiceCommitment:
    origin: str
    dest: str
    zone: Optional[int]
    sla_days: int
    sla_buffer_days: float
    priority_weight: float

    def matches(self, origin: str, dest: str, zone: int) -> bool:
        origin_match = (self.origin == "*" or self.origin == origin)
        dest_match = (self.dest == "*" or self.dest == dest)
        zone_match = (self.zone is None or self.zone == zone)
        return origin_match and dest_match and zone_match


@dataclass
class MileageBand:
    zone: int
    mileage_band_min: float
    mileage_band_max: float
    circuity_factor: float
    mph: float


@dataclass
class Facility:
    name: str
    facility_type: FacilityType
    lat: float
    lon: float
    timezone: ZoneInfo
    parent_hub_name: Optional[str]
    regional_sort_hub: Optional[str]
    is_injection_node: bool

    mm_sort_start_local: Optional[time]
    mm_sort_end_local: Optional[time]

    lm_sort_start_local: Optional[time]
    lm_sort_end_local: Optional[time]

    outbound_window_start_local: Optional[time]
    outbound_window_end_local: Optional[time]
    outbound_cpt_count: Optional[int]

    max_inbound_trucks_per_hour: Optional[float]
    max_outbound_trucks_per_hour: Optional[float]

    def get_mm_sort_window(self) -> Optional[SortWindow]:
        if self.mm_sort_start_local and self.mm_sort_end_local:
            return SortWindow(
                start_local=self.mm_sort_start_local,
                end_local=self.mm_sort_end_local,
                timezone=self.timezone
            )
        return None

    def get_lm_sort_window(self) -> Optional[SortWindow]:
        if self.lm_sort_start_local and self.lm_sort_end_local:
            return SortWindow(
                start_local=self.lm_sort_start_local,
                end_local=self.lm_sort_end_local,
                timezone=self.timezone
            )
        return None

    def get_outbound_window(self) -> Optional[SortWindow]:
        if self.outbound_window_start_local and self.outbound_window_end_local:
            return SortWindow(
                start_local=self.outbound_window_start_local,
                end_local=self.outbound_window_end_local,
                timezone=self.timezone
            )
        return None


@dataclass
class PathCandidate:
    origin: str
    dest: str
    path_nodes: list[str]
    path_type: PathType
    sort_level: SortLevel           # Sort level at origin
    dest_sort_level: SortLevel      # Sort level arriving at destination
    total_path_miles: float
    direct_miles: float
    atw_factor: float


@dataclass
class PathTimingResult:
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
    steps: list
    uses_only_active_arcs: bool


@dataclass
class PathStep:
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
    objective_type: ObjectiveType
    max_path_touches: int
    max_path_atw_factor: float
    reference_injection_date: datetime
    reference_injection_time: time


@dataclass
class ODDemand:
    scenario_id: str
    origin: str
    dest: str
    pkgs_day: float
    zone: int
    flow_type: FlowType
    day_type: str


def time_to_minutes(t: time) -> float:
    return t.hour * MINUTES_PER_HOUR + t.minute + t.second / 60


def minutes_to_time(minutes: float) -> time:
    minutes = minutes % MINUTES_PER_DAY
    hours = int(minutes // MINUTES_PER_HOUR)
    mins = int(minutes % MINUTES_PER_HOUR)
    secs = int((minutes % 1) * 60)
    return time(hour=hours, minute=mins, second=secs)


def parse_days_of_week(days_str: Optional[str]) -> list[str]:
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
    return dt.strftime("%a")