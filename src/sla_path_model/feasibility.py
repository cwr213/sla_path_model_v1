"""
Feasibility: SLA checking and path filtering.
"""
from typing import Optional

from .config import (
    PathTimingResult, ServiceCommitment, ODDemand, FlowType,
    HOURS_PER_DAY
)
from .utils import setup_logging

logger = setup_logging()


class FeasibilityChecker:
    """Check path feasibility against SLA commitments."""

    def __init__(self, service_commitments: list[ServiceCommitment]):
        self.service_commitments = service_commitments

        # Index commitments for fast lookup
        self._build_commitment_index()

    def _build_commitment_index(self):
        """Build index for efficient commitment lookup."""
        # Priority order:
        # 1. Specific OD pair
        # 2. Origin-specific (dest = *)
        # 3. Dest-specific (origin = *)
        # 4. Zone-based
        # 5. Default (* -> *)

        self.od_commitments = {}  # (origin, dest) -> ServiceCommitment
        self.origin_commitments = {}  # origin -> ServiceCommitment
        self.dest_commitments = {}  # dest -> ServiceCommitment
        self.zone_commitments = {}  # zone -> ServiceCommitment
        self.default_commitment = None

        for sc in self.service_commitments:
            if sc.origin != "*" and sc.dest != "*":
                # Specific OD pair
                self.od_commitments[(sc.origin, sc.dest)] = sc
            elif sc.origin != "*" and sc.dest == "*":
                # Origin-specific
                self.origin_commitments[sc.origin] = sc
            elif sc.origin == "*" and sc.dest != "*":
                # Dest-specific
                self.dest_commitments[sc.dest] = sc
            elif sc.zone is not None:
                # Zone-based
                self.zone_commitments[sc.zone] = sc
            else:
                # Default
                self.default_commitment = sc

    def get_commitment(
            self,
            origin: str,
            dest: str,
            zone: int
    ) -> Optional[ServiceCommitment]:
        """
        Get the applicable service commitment for an OD pair.

        Priority:
        1. Specific OD pair
        2. Origin-specific
        3. Dest-specific
        4. Zone-based
        5. Default
        """
        # Check specific OD
        if (origin, dest) in self.od_commitments:
            return self.od_commitments[(origin, dest)]

        # Check origin-specific
        if origin in self.origin_commitments:
            return self.origin_commitments[origin]

        # Check dest-specific
        if dest in self.dest_commitments:
            return self.dest_commitments[dest]

        # Check zone-based
        if zone in self.zone_commitments:
            return self.zone_commitments[zone]

        # Return default
        return self.default_commitment

    def check_feasibility(
            self,
            timing: PathTimingResult,
            zone: int
    ) -> PathTimingResult:
        """
        Check if a path meets SLA and update timing result with SLA info.

        Args:
            timing: PathTimingResult from timing engine
            zone: Zone for this OD pair

        Returns:
            Updated PathTimingResult with SLA fields populated
        """
        commitment = self.get_commitment(
            timing.path.origin,
            timing.path.dest,
            zone
        )

        if commitment is None:
            # No commitment defined - assume met
            timing.sla_days = 0
            timing.sla_buffer_days = 0
            timing.sla_target_hours = float('inf')
            timing.sla_met = True
            timing.sla_slack_hours = float('inf')
            timing.priority_weight = 1.0
            return timing

        # Calculate SLA target in hours
        sla_target_hours = (commitment.sla_days + commitment.sla_buffer_days) * HOURS_PER_DAY

        # Check if path meets SLA
        sla_met = timing.tit_hours <= sla_target_hours
        sla_slack_hours = sla_target_hours - timing.tit_hours

        # Update timing result
        timing.sla_days = commitment.sla_days
        timing.sla_buffer_days = commitment.sla_buffer_days
        timing.sla_target_hours = sla_target_hours
        timing.sla_met = sla_met
        timing.sla_slack_hours = sla_slack_hours
        timing.priority_weight = commitment.priority_weight

        return timing


def check_all_feasibility(
        od_timings: dict[tuple[str, str], list[PathTimingResult]],
        od_demands: list[ODDemand],
        service_commitments: list[ServiceCommitment]
) -> dict[tuple[str, str], list[PathTimingResult]]:
    """
    Check feasibility for all path timings.

    Args:
        od_timings: Dictionary mapping (origin, dest) to list of PathTimingResult
        od_demands: List of ODDemand objects (for zone lookup)
        service_commitments: List of ServiceCommitment objects

    Returns:
        Updated od_timings with SLA info populated
    """
    checker = FeasibilityChecker(service_commitments)

    # Build zone lookup from demands
    od_zones = {}
    for demand in od_demands:
        key = (demand.origin, demand.dest)
        od_zones[key] = demand.zone

    # Check each path
    total_paths = sum(len(timings) for timings in od_timings.values())
    paths_met = 0
    paths_missed = 0

    for (origin, dest), timings in od_timings.items():
        zone = od_zones.get((origin, dest), 1)  # Default to zone 1

        for timing in timings:
            checker.check_feasibility(timing, zone)

            if timing.sla_met:
                paths_met += 1
            else:
                paths_missed += 1

    logger.info(
        f"Feasibility check complete: {paths_met}/{total_paths} paths meet SLA "
        f"({100 * paths_met / total_paths:.1f}%)"
    )

    return od_timings


def filter_feasible_paths(
        od_timings: dict[tuple[str, str], list[PathTimingResult]],
        include_all: bool = True
) -> dict[tuple[str, str], list[PathTimingResult]]:
    """
    Filter paths to only feasible ones (or keep all with sla_met flag).

    Args:
        od_timings: Dictionary mapping (origin, dest) to list of PathTimingResult
        include_all: If True, keep all paths with sla_met flag; if False, only keep feasible

    Returns:
        Filtered od_timings
    """
    if include_all:
        return od_timings

    filtered = {}
    for (origin, dest), timings in od_timings.items():
        feasible = [t for t in timings if t.sla_met]
        if feasible:
            filtered[(origin, dest)] = feasible

    return filtered