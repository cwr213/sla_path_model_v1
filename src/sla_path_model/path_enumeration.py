"""
Path enumeration: generate all candidate paths through the network.
"""
from typing import Optional

from .config import (
    Facility, FacilityType, PathCandidate, PathType, SortLevel, RunSettings
)
from .geo import haversine_miles, calculate_atw_factor, calculate_path_distance
from .utils import setup_logging

logger = setup_logging()


class PathEnumerator:
    """Generate candidate paths through the network."""

    def __init__(
            self,
            facilities: dict[str, Facility],
            run_settings: RunSettings
    ):
        self.facilities = facilities
        self.max_path_touches = run_settings.max_path_touches
        self.max_atw_factor = run_settings.max_path_atw_factor

        # Build facility type lookups
        self._build_facility_lookups()

    def _build_facility_lookups(self):
        """Build lookups for facility types and relationships."""
        self.hubs = {
            name: fac for name, fac in self.facilities.items()
            if fac.facility_type == FacilityType.HUB
        }
        self.hybrids = {
            name: fac for name, fac in self.facilities.items()
            if fac.facility_type == FacilityType.HYBRID
        }
        self.launches = {
            name: fac for name, fac in self.facilities.items()
            if fac.facility_type == FacilityType.LAUNCH
        }

        # Hub and hybrid facilities (can do sorting)
        self.sorting_facilities = {**self.hubs, **self.hybrids}

        # Build parent hub mapping
        self.parent_hub = {}
        for name, fac in self.facilities.items():
            if fac.parent_hub_name:
                self.parent_hub[name] = fac.parent_hub_name

        # Build regional sort hub mapping
        self.regional_hub = {}
        for name, fac in self.facilities.items():
            if fac.regional_sort_hub:
                self.regional_hub[name] = fac.regional_sort_hub

        logger.info(
            f"Path enumeration: {len(self.hubs)} hubs, {len(self.hybrids)} hybrids, "
            f"{len(self.launches)} launches"
        )

    def enumerate_paths_for_od(
            self,
            origin: str,
            dest: str
    ) -> list[PathCandidate]:
        """
        Enumerate all valid paths from origin to destination.

        Considers hub hierarchy rules and max_path_touches.

        Args:
            origin: Origin facility name (injection point)
            dest: Destination facility name (delivery facility)

        Returns:
            List of PathCandidate objects, one per valid (path, sort_level) combination
        """
        if origin not in self.facilities:
            raise ValueError(f"Unknown origin facility: {origin}")
        if dest not in self.facilities:
            raise ValueError(f"Unknown destination facility: {dest}")

        origin_fac = self.facilities[origin]
        dest_fac = self.facilities[dest]

        # Calculate direct distance for ATW filtering
        direct_miles = haversine_miles(
            origin_fac.lat, origin_fac.lon,
            dest_fac.lat, dest_fac.lon
        )

        # Generate raw paths (without sort level consideration)
        raw_paths = self._enumerate_raw_paths(origin, dest)

        # Expand each raw path into multiple candidates (one per valid sort level)
        candidates = []
        for path_nodes in raw_paths:
            path_candidates = self._expand_path_to_candidates(
                path_nodes, origin, dest, direct_miles
            )
            candidates.extend(path_candidates)

        # Filter by ATW factor
        valid_candidates = [
            c for c in candidates
            if c.atw_factor <= self.max_atw_factor
        ]

        logger.debug(
            f"OD {origin}->{dest}: {len(raw_paths)} raw paths, "
            f"{len(candidates)} candidates, {len(valid_candidates)} after ATW filter"
        )

        return valid_candidates

    def _enumerate_raw_paths(self, origin: str, dest: str) -> list[list[str]]:
        """
        Enumerate raw paths (node sequences) from origin to destination.

        Generates:
        - Direct paths (origin -> dest)
        - 1-touch paths (origin -> intermediate -> dest)
        - 2-touch paths (origin -> hub1 -> hub2 -> dest)
        - 3-touch paths (origin -> hub1 -> hub2 -> hub3 -> dest)
        """
        paths = []

        # Direct path (always valid)
        paths.append([origin, dest])

        # 1-touch paths
        if self.max_path_touches >= 2:
            for hub_name in self.sorting_facilities:
                if hub_name != origin and hub_name != dest:
                    path = [origin, hub_name, dest]
                    if self._is_valid_path_structure(path):
                        paths.append(path)

        # 2-touch paths
        if self.max_path_touches >= 3:
            for hub1 in self.sorting_facilities:
                if hub1 == origin or hub1 == dest:
                    continue
                for hub2 in self.sorting_facilities:
                    if hub2 == origin or hub2 == dest or hub2 == hub1:
                        continue
                    path = [origin, hub1, hub2, dest]
                    if self._is_valid_path_structure(path):
                        paths.append(path)

        # 3-touch paths
        if self.max_path_touches >= 4:
            for hub1 in self.sorting_facilities:
                if hub1 == origin or hub1 == dest:
                    continue
                for hub2 in self.sorting_facilities:
                    if hub2 == origin or hub2 == dest or hub2 == hub1:
                        continue
                    for hub3 in self.sorting_facilities:
                        if hub3 == origin or hub3 == dest or hub3 == hub1 or hub3 == hub2:
                            continue
                        path = [origin, hub1, hub2, hub3, dest]
                        if self._is_valid_path_structure(path):
                            paths.append(path)

        return paths

    def _is_valid_path_structure(self, path: list[str]) -> bool:
        """
        Check if path adheres to hub hierarchy rules.

        Rules:
        1. Origin must be hub or hybrid (injection node)
        2. Destination must be launch or hybrid (delivery facility)
        3. Intermediate nodes must be hub or hybrid (sorting facilities)
        4. Parent hub rule: if dest has a parent hub, path should go through it
           (unless coming from same parent hub region)
        """
        if len(path) < 2:
            return False

        origin = path[0]
        dest = path[-1]

        # Check origin is injection capable
        origin_fac = self.facilities[origin]
        if origin_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
            return False

        # Check dest is delivery capable
        dest_fac = self.facilities[dest]
        if dest_fac.facility_type not in (FacilityType.LAUNCH, FacilityType.HYBRID):
            return False

        # Check intermediates are sorting facilities
        for node in path[1:-1]:
            node_fac = self.facilities[node]
            if node_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                return False

        # Parent hub rule: if dest has parent hub, path should include it
        # (unless origin is the parent hub or shares same parent)
        if dest in self.parent_hub:
            parent = self.parent_hub[dest]
            origin_parent = self.parent_hub.get(origin)

            # Skip rule if origin shares same parent or IS the parent
            if origin != parent and origin_parent != parent:
                if parent not in path:
                    # Path doesn't go through dest's parent hub - invalid
                    return False

        return True

    def _expand_path_to_candidates(
            self,
            path_nodes: list[str],
            origin: str,
            dest: str,
            direct_miles: float
    ) -> list[PathCandidate]:
        """
        Expand a raw path into PathCandidate objects for each valid sort level.

        Sort levels determine processing at intermediate hubs:
        - region: crossdock at intermediates (fastest)
        - market: partial sort at intermediates
        - sort_group: full sort at intermediates (most efficient for consolidation)
        """
        # Calculate path metrics
        total_miles, leg_miles = calculate_path_distance(path_nodes, self.facilities)
        atw_factor = calculate_atw_factor(total_miles, direct_miles)

        # Determine path type
        num_touches = len(path_nodes) - 1  # Number of facility touches excluding origin
        path_type = {
            1: PathType.DIRECT,
            2: PathType.ONE_TOUCH,
            3: PathType.TWO_TOUCH,
            4: PathType.THREE_TOUCH
        }.get(num_touches, PathType.THREE_TOUCH)

        # Generate candidates for each valid sort level
        candidates = []

        for sort_level in SortLevel:
            # Validate sort level is achievable for this path
            if self._is_sort_level_valid(path_nodes, sort_level):
                candidate = PathCandidate(
                    origin=origin,
                    dest=dest,
                    path_nodes=path_nodes,
                    path_type=path_type,
                    sort_level=sort_level,
                    total_path_miles=total_miles,
                    direct_miles=direct_miles,
                    atw_factor=atw_factor
                )
                candidates.append(candidate)

        return candidates

    def _is_sort_level_valid(self, path_nodes: list[str], sort_level: SortLevel) -> bool:
        """
        Check if a sort level is achievable for a given path.

        For now, all sort levels are considered valid for all paths.
        Future enhancement: check if destination supports the sort level.
        """
        # All sort levels valid for all paths in v1
        return True


def enumerate_all_paths(
        data: dict,
        od_demands: list  # List of ODDemand
) -> dict[tuple[str, str], list[PathCandidate]]:
    """
    Enumerate paths for all unique OD pairs in demand.

    Args:
        data: Dictionary from InputLoader.load_all()
        od_demands: List of ODDemand objects

    Returns:
        Dictionary mapping (origin, dest) to list of PathCandidate objects
    """
    enumerator = PathEnumerator(
        facilities=data["facilities"],
        run_settings=data["run_settings"]
    )

    # Get unique OD pairs (excluding direct injection zone 0)
    od_pairs = set()
    for od in od_demands:
        if od.zone > 0:  # Middle mile only (direct injection doesn't need paths)
            od_pairs.add((od.origin, od.dest))

    logger.info(f"Enumerating paths for {len(od_pairs)} unique OD pairs")

    # Enumerate paths for each OD pair
    od_paths = {}
    for origin, dest in od_pairs:
        candidates = enumerator.enumerate_paths_for_od(origin, dest)
        od_paths[(origin, dest)] = candidates

    total_paths = sum(len(p) for p in od_paths.values())
    logger.info(f"Generated {total_paths} total path candidates across {len(od_pairs)} OD pairs")

    return od_paths