"""Generate all candidate paths through the network."""
from .config import (
    Facility, FacilityType, PathCandidate, PathType, SortLevel, RunSettings,
    ALL_SORT_LEVELS
)
from .geo import haversine_miles, calculate_atw_factor, calculate_path_distance
from .utils import setup_logging

logger = setup_logging()


class PathEnumerator:

    def __init__(self, facilities: dict[str, Facility], run_settings: RunSettings,
                 injection_df, enabled_sort_levels: frozenset = None):
        self.facilities = facilities
        self.max_path_touches = run_settings.max_path_touches
        self.max_atw_factor = run_settings.max_path_atw_factor
        self.enabled_sort_levels = enabled_sort_levels if enabled_sort_levels is not None else ALL_SORT_LEVELS

        self._build_injection_facilities(injection_df)
        self._build_facility_lookups()

    def _build_injection_facilities(self, injection_df):
        """Build set of facilities that receive injection volume."""
        share_cols = [c for c in injection_df.columns if c.startswith('share_')]
        if not share_cols and 'absolute_share' in injection_df.columns:
            share_cols = ['absolute_share']

        self.injection_facilities = set()
        for _, row in injection_df.iterrows():
            fac_name = str(row["facility_name"]).strip()
            # If facility has non-zero share in ANY year, it's an injection facility
            if any(row[col] > 0 for col in share_cols if col in row):
                self.injection_facilities.add(fac_name)

        logger.info(f"Identified {len(self.injection_facilities)} injection facilities")

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

        # Build regional_sort_hub mappings
        self.regional_hub = {}
        self.regional_hub_to_facilities = {}  # Reverse mapping: RSH -> list of facilities it serves
        for name, fac in self.facilities.items():
            if fac.regional_sort_hub:
                self.regional_hub[name] = fac.regional_sort_hub
                # Build reverse mapping
                if fac.regional_sort_hub not in self.regional_hub_to_facilities:
                    self.regional_hub_to_facilities[fac.regional_sort_hub] = []
                self.regional_hub_to_facilities[fac.regional_sort_hub].append(name)

        logger.info(
            f"Path enumeration: {len(self.hubs)} hubs, {len(self.hybrids)} hybrids, "
            f"{len(self.launches)} launches, {len(self.regional_hub_to_facilities)} regional sort hubs"
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
        """
        Create path for O=D scenarios where origin equals destination.

        Used for zone skip and middle mile flows where O=D (zone 1+).
        Direct injection (zone 0) is handled separately in reporting.
        """
        # O=D middle mile - uses od_mm path type
        # sort_level and dest_sort_level are n/a conceptually but we use SORT_GROUP
        # since the timing logic uses path_type to determine processing
        return [PathCandidate(
            origin=origin,
            dest=dest,
            path_nodes=[origin],  # Single node for O=D
            path_type=PathType.OD_MM,
            sort_level=SortLevel.SORT_GROUP,
            dest_sort_level=SortLevel.SORT_GROUP,
            total_path_miles=0.0,
            direct_miles=0.0,
            atw_factor=1.0
        )]

    def _enumerate_raw_paths(self, origin: str, dest: str) -> list[list[str]]:
        """
        Enumerate raw paths based on max_path_touches.

        Touch count = number of facilities in path:
        - 2-touch: O -> D (2 nodes)
        - 3-touch: O -> H -> D (3 nodes)
        - 4-touch: O -> H1 -> H2 -> D (4 nodes)
        - 5-touch: O -> H1 -> H2 -> H3 -> D (5 nodes)
        """
        paths = []

        # 2-touch: Direct path (always valid if max_path_touches >= 2)
        if self.max_path_touches >= 2:
            paths.append([origin, dest])

        # 3-touch: O -> H -> D
        if self.max_path_touches >= 3:
            for hub_name in self.sorting_facilities:
                if hub_name != origin and hub_name != dest:
                    path = [origin, hub_name, dest]
                    if self._is_valid_path_structure(path):
                        paths.append(path)

        # 4-touch: O -> H1 -> H2 -> D
        if self.max_path_touches >= 4:
            for hub1 in self.sorting_facilities:
                if hub1 == origin or hub1 == dest:
                    continue
                for hub2 in self.sorting_facilities:
                    if hub2 == origin or hub2 == dest or hub2 == hub1:
                        continue
                    path = [origin, hub1, hub2, dest]
                    if self._is_valid_path_structure(path):
                        paths.append(path)

        # 5-touch: O -> H1 -> H2 -> H3 -> D
        if self.max_path_touches >= 5:
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
        Validate path structure with hierarchy enforcement.

        Rules:
        1. Origin must be hub or hybrid
        2. Destination must be launch or hybrid
        3. All intermediates must be hub or hybrid
        4. Non-injection intermediates can only route to their children (via regional_sort_hub)
        5. If destination has regional_sort_hub (other than itself), RSH must be in path
        """
        if len(path) < 2:
            return False

        origin = path[0]
        dest = path[-1]

        # Rule 1: Origin must be hub or hybrid
        origin_fac = self.facilities[origin]
        if origin_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
            return False

        # Rule 2: Destination must be launch or hybrid
        dest_fac = self.facilities[dest]
        if dest_fac.facility_type not in (FacilityType.LAUNCH, FacilityType.HYBRID):
            return False

        # Rule 3 & 4: Validate intermediates with hierarchy enforcement
        for i in range(1, len(path) - 1):
            node = path[i]
            node_fac = self.facilities[node]

            # All intermediates must be hub or hybrid
            if node_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                return False

            # HIERARCHY ENFORCEMENT: Non-injection intermediates can only route to children
            if node not in self.injection_facilities:
                next_node = path[i + 1]
                next_fac = self.facilities[next_node]

                # Check if next_node is a child of current node (via regional_sort_hub)
                is_child = (next_fac.regional_sort_hub == node)

                if not is_child:
                    # Non-injection intermediate routing to non-child: invalid path
                    return False

        # Rule 5: If destination has regional_sort_hub (not self-referencing), RSH must be in path
        # This ensures proper last mile hierarchy
        if dest in self.regional_hub:
            rsh = self.regional_hub[dest]
            # Only enforce if RSH is not the destination itself
            if rsh != dest:
                # Check if origin is the RSH or if RSH appears in path
                origin_rsh = self.regional_hub.get(origin)

                if origin != rsh and origin_rsh != rsh:
                    if rsh not in path:
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

        num_touches = len(path_nodes)
        path_type = {
            2: PathType.TWO_TOUCH,
            3: PathType.THREE_TOUCH,
            4: PathType.FOUR_TOUCH,
            5: PathType.FIVE_TOUCH
        }.get(num_touches, PathType.FIVE_TOUCH)

        is_direct = (num_touches == 2)
        dest_regional_hub = self.regional_hub.get(dest)
        second_to_last = path_nodes[-2] if len(path_nodes) >= 2 else None

        candidates = []

        # SORT_GROUP: Valid for any path
        # dest_sort_level = SORT_GROUP (no LM sort needed)
        if SortLevel.SORT_GROUP in self.enabled_sort_levels:
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
        if SortLevel.MARKET in self.enabled_sort_levels:
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

        # REGION: Valid when 2nd-to-last facility is destination's RSH,
        # OR when destination itself IS an RSH
        # NOT valid for direct paths from RSH to child (that's just MARKET/SORT_GROUP)
        if SortLevel.REGION in self.enabled_sort_levels:

            # Case 1: Multi-hop path where 2nd-to-last IS the destination's RSH
            # Example: ATL02 → PHL01 → ABE01 (where ABE01.regional_sort_hub = PHL01)
            # PHL01 does region-level sort, then ABE01 receives presorted freight
            if dest_regional_hub and not is_direct and second_to_last == dest_regional_hub:
                # Two variants based on how much sorting destination does:
                # 1a. RSH sorts to market level, destination does sort_group→route (full LM sort)
                candidates.append(PathCandidate(
                    origin=origin,
                    dest=dest,
                    path_nodes=path_nodes,
                    path_type=path_type,
                    sort_level=SortLevel.REGION,
                    dest_sort_level=SortLevel.MARKET,
                    total_path_miles=total_miles,
                    direct_miles=direct_miles,
                    atw_factor=atw_factor
                ))
                # 1b. RSH sorts to sort_group level, destination only does route sort (minimal LM sort)
                # Only create this variant if SORT_GROUP is also enabled
                if SortLevel.SORT_GROUP in self.enabled_sort_levels:
                    candidates.append(PathCandidate(
                        origin=origin,
                        dest=dest,
                        path_nodes=path_nodes,
                        path_type=path_type,
                        sort_level=SortLevel.REGION,
                        dest_sort_level=SortLevel.SORT_GROUP,
                        total_path_miles=total_miles,
                        direct_miles=direct_miles,
                        atw_factor=atw_factor
                    ))

            # Case 2: Destination IS an RSH (self-referencing or serves others)
            # Example: ATL02 → PHL01 (where PHL01.regional_sort_hub = PHL01)
            # PHL01 can handle region-level breakdown for itself
            dest_is_rsh = dest in self.regional_hub_to_facilities

            if dest_is_rsh:
                # Destination can handle region-level breakdown for itself
                if is_direct:
                    # Direct path to RSH: dest does region→sort_group + last mile
                    candidates.append(PathCandidate(
                        origin=origin,
                        dest=dest,
                        path_nodes=path_nodes,
                        path_type=path_type,
                        sort_level=SortLevel.REGION,
                        dest_sort_level=SortLevel.MARKET,
                        total_path_miles=total_miles,
                        direct_miles=direct_miles,
                        atw_factor=atw_factor
                    ))
                else:
                    # Multi-hop to RSH destination: dest does full sort + last mile
                    candidates.append(PathCandidate(
                        origin=origin,
                        dest=dest,
                        path_nodes=path_nodes,
                        path_type=path_type,
                        sort_level=SortLevel.REGION,
                        dest_sort_level=SortLevel.MARKET,
                        total_path_miles=total_miles,
                        direct_miles=direct_miles,
                        atw_factor=atw_factor
                    ))

        return candidates


def enumerate_all_paths(
        data: dict,
        od_demands: list,
        enabled_sort_levels: frozenset = None
) -> dict[tuple[str, str], list[PathCandidate]]:
    enumerator = PathEnumerator(
        facilities=data["facilities"],
        run_settings=data["run_settings"],
        injection_df=data["injection_distribution"],
        enabled_sort_levels=enabled_sort_levels
    )

    # Separate DI (zone 0) from networked flows (zone 1+)
    di_od_pairs = set()
    networked_od_pairs = set()

    for od in od_demands:
        if od.zone == 0:
            di_od_pairs.add((od.origin, od.dest))
        else:
            networked_od_pairs.add((od.origin, od.dest))

    logger.info(f"Enumerating paths for {len(networked_od_pairs)} networked OD pairs, {len(di_od_pairs)} DI OD pairs")

    od_paths = {}

    # Enumerate networked paths (zone 1+)
    for origin, dest in networked_od_pairs:
        candidates = enumerator.enumerate_paths_for_od(origin, dest)
        od_paths[(origin, dest)] = candidates

    # Create DI paths (zone 0, always O=D)
    for origin, dest in di_od_pairs:
        if origin != dest:
            logger.warning(f"DI demand has origin != dest: {origin} -> {dest}, skipping")
            continue

        # DI path - single node
        di_path = PathCandidate(
            origin=origin,
            dest=dest,
            path_nodes=[origin],
            path_type=PathType.DIRECT_INJECTION,
            sort_level=SortLevel.SORT_GROUP,  # n/a conceptually
            dest_sort_level=SortLevel.SORT_GROUP,  # n/a conceptually
            total_path_miles=0.0,
            direct_miles=0.0,
            atw_factor=1.0
        )

        # Add to existing paths or create new entry
        if (origin, dest) in od_paths:
            od_paths[(origin, dest)].append(di_path)
        else:
            od_paths[(origin, dest)] = [di_path]

    total_paths = sum(len(p) for p in od_paths.values())
    logger.info(f"Generated {total_paths} total path candidates across {len(od_paths)} OD pairs")

    return od_paths