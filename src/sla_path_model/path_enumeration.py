"""Generate all candidate paths through the network."""
from .config import (
    Facility, FacilityType, PathCandidate, PathType, SortLevel, RunSettings
)
from .geo import haversine_miles, calculate_atw_factor, calculate_path_distance
from .utils import setup_logging

logger = setup_logging()


class PathEnumerator:

    def __init__(self, facilities: dict[str, Facility], run_settings: RunSettings):
        self.facilities = facilities
        self.max_path_touches = run_settings.max_path_touches
        self.max_atw_factor = run_settings.max_path_atw_factor

        self._build_facility_lookups()

    def _build_facility_lookups(self):
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

        self.sorting_facilities = {**self.hubs, **self.hybrids}

        self.parent_hub = {}
        for name, fac in self.facilities.items():
            if fac.parent_hub_name:
                self.parent_hub[name] = fac.parent_hub_name

        self.regional_hub = {}
        for name, fac in self.facilities.items():
            if fac.regional_sort_hub:
                self.regional_hub[name] = fac.regional_sort_hub

        logger.info(
            f"Path enumeration: {len(self.hubs)} hubs, {len(self.hybrids)} hybrids, "
            f"{len(self.launches)} launches"
        )

    def enumerate_paths_for_od(self, origin: str, dest: str) -> list[PathCandidate]:
        if origin not in self.facilities:
            raise ValueError(f"Unknown origin facility: {origin}")
        if dest not in self.facilities:
            raise ValueError(f"Unknown destination facility: {dest}")

        origin_fac = self.facilities[origin]
        dest_fac = self.facilities[dest]

        direct_miles = haversine_miles(
            origin_fac.lat, origin_fac.lon,
            dest_fac.lat, dest_fac.lon
        )

        # O=D: Return single hardcoded path, no enumeration needed
        if origin == dest:
            return self._create_od_equal_path(origin, dest)

        raw_paths = self._enumerate_raw_paths(origin, dest)

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

    def _create_od_equal_path(self, origin: str, dest: str) -> list[PathCandidate]:
        """Create the single valid path for O=D scenarios."""
        # O=D is always sort_group level, direct path, 0 miles
        return [PathCandidate(
            origin=origin,
            dest=dest,
            path_nodes=[origin, dest],
            path_type=PathType.DIRECT,
            sort_level=SortLevel.SORT_GROUP,
            dest_sort_level=SortLevel.SORT_GROUP,
            total_path_miles=0.0,
            direct_miles=0.0,
            atw_factor=1.0
        )]

    def _enumerate_raw_paths(self, origin: str, dest: str) -> list[list[str]]:
        """Enumerate raw paths. For non-direct, regional_sort_hub must be 2nd-to-last."""
        paths = []
        dest_regional_hub = self.regional_hub.get(dest)

        # Direct path (always valid)
        paths.append([origin, dest])

        # 1-touch paths: O → H → D (H must be regional_sort_hub for REGION sort)
        if self.max_path_touches >= 2:
            for hub_name in self.sorting_facilities:
                if hub_name != origin and hub_name != dest:
                    path = [origin, hub_name, dest]
                    if self._is_valid_path_structure(path):
                        paths.append(path)

        # 2-touch paths: O → X → H → D (H must be regional_sort_hub for REGION sort)
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

        # 3-touch paths: O → X → Y → H → D
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
        if len(path) < 2:
            return False

        origin = path[0]
        dest = path[-1]

        origin_fac = self.facilities[origin]
        if origin_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
            return False

        dest_fac = self.facilities[dest]
        if dest_fac.facility_type not in (FacilityType.LAUNCH, FacilityType.HYBRID):
            return False

        for node in path[1:-1]:
            node_fac = self.facilities[node]
            if node_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                return False

        if dest in self.parent_hub:
            parent = self.parent_hub[dest]
            origin_parent = self.parent_hub.get(origin)

            if origin != parent and origin_parent != parent:
                if parent not in path:
                    return False

        return True

    def _expand_path_to_candidates(
            self,
            path_nodes: list[str],
            origin: str,
            dest: str,
            direct_miles: float
    ) -> list[PathCandidate]:
        total_miles, leg_miles = calculate_path_distance(path_nodes, self.facilities)
        atw_factor = calculate_atw_factor(total_miles, direct_miles)

        num_touches = len(path_nodes) - 1
        path_type = {
            1: PathType.DIRECT,
            2: PathType.ONE_TOUCH,
            3: PathType.TWO_TOUCH,
            4: PathType.THREE_TOUCH
        }.get(num_touches, PathType.THREE_TOUCH)

        is_direct = (num_touches == 1)
        dest_regional_hub = self.regional_hub.get(dest)
        second_to_last = path_nodes[-2] if len(path_nodes) >= 2 else None

        candidates = []

        # SORT_GROUP: Valid for any path
        # dest_sort_level = SORT_GROUP (no LM sort needed)
        candidates.append(PathCandidate(
            origin=origin,
            dest=dest,
            path_nodes=path_nodes,
            path_type=path_type,
            sort_level=SortLevel.SORT_GROUP,
            dest_sort_level=SortLevel.SORT_GROUP,
            total_path_miles=total_miles,
            direct_miles=direct_miles,
            atw_factor=atw_factor
        ))

        # MARKET: Valid for any path
        # dest_sort_level = MARKET (LM sort needed)
        candidates.append(PathCandidate(
            origin=origin,
            dest=dest,
            path_nodes=path_nodes,
            path_type=path_type,
            sort_level=SortLevel.MARKET,
            dest_sort_level=SortLevel.MARKET,
            total_path_miles=total_miles,
            direct_miles=direct_miles,
            atw_factor=atw_factor
        ))

        # REGION: Only valid for non-direct paths where 2nd-to-last is regional_sort_hub
        if not is_direct and dest_regional_hub and second_to_last == dest_regional_hub:
            # Enumerate two variants: hub sorts to MARKET or SORT_GROUP
            candidates.append(PathCandidate(
                origin=origin,
                dest=dest,
                path_nodes=path_nodes,
                path_type=path_type,
                sort_level=SortLevel.REGION,
                dest_sort_level=SortLevel.MARKET,  # Hub sorts to market, dest does LM sort
                total_path_miles=total_miles,
                direct_miles=direct_miles,
                atw_factor=atw_factor
            ))
            candidates.append(PathCandidate(
                origin=origin,
                dest=dest,
                path_nodes=path_nodes,
                path_type=path_type,
                sort_level=SortLevel.REGION,
                dest_sort_level=SortLevel.SORT_GROUP,  # Hub sorts to sort_group, no LM sort
                total_path_miles=total_miles,
                direct_miles=direct_miles,
                atw_factor=atw_factor
            ))

        return candidates


def enumerate_all_paths(
        data: dict,
        od_demands: list
) -> dict[tuple[str, str], list[PathCandidate]]:
    enumerator = PathEnumerator(
        facilities=data["facilities"],
        run_settings=data["run_settings"]
    )

    # Get unique OD pairs (excluding direct injection zone 0)
    od_pairs = set()
    for od in od_demands:
        if od.zone > 0:
            od_pairs.add((od.origin, od.dest))

    logger.info(f"Enumerating paths for {len(od_pairs)} unique OD pairs")

    od_paths = {}
    for origin, dest in od_pairs:
        candidates = enumerator.enumerate_paths_for_od(origin, dest)
        od_paths[(origin, dest)] = candidates

    total_paths = sum(len(p) for p in od_paths.values())
    logger.info(f"Generated {total_paths} total path candidates across {len(od_pairs)} OD pairs")

    return od_paths