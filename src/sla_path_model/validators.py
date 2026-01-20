"""
Input validation functions.
"""
from collections import defaultdict
from typing import Optional

from .config import Facility, FacilityType, MileageBand, ServiceCommitment, TimingParams, DemandSource
from .utils import setup_logging

logger = setup_logging()


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class InputValidator:
    """Validate loaded input data for consistency and completeness."""

    def __init__(self, data: dict):
        self.data = data
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def validate_all(self) -> tuple[list[str], list[str]]:
        """Run all validations and return (errors, warnings)."""
        self.validate_facilities()
        self.validate_facility_references()
        self.validate_regional_sort_hub_types()
        self.validate_non_injection_hub_hierarchy()
        self.validate_injection_nodes()
        self.validate_mileage_bands()
        self.validate_timing_params()
        self.validate_scenarios()
        self.validate_service_commitments()
        self.validate_zips_facility_references()
        self.validate_facility_markets()  # NEW
        self.validate_market_demand()      # NEW

        return self.errors, self.warnings

    def validate_facilities(self):
        """Validate facility data."""
        facilities: dict[str, Facility] = self.data["facilities"]

        for name, fac in facilities.items():
            # Check required coordinates
            if fac.lat is None or fac.lon is None:
                self.errors.append(f"Facility {name} missing coordinates")

            # Check coordinate ranges
            if fac.lat is not None and (fac.lat < -90 or fac.lat > 90):
                self.errors.append(f"Facility {name} has invalid latitude: {fac.lat}")
            if fac.lon is not None and (fac.lon < -180 or fac.lon > 180):
                self.errors.append(f"Facility {name} has invalid longitude: {fac.lon}")

            # Hub/Hybrid should have middle mile sort windows
            if fac.facility_type in (FacilityType.HUB, FacilityType.HYBRID):
                if fac.mm_sort_start_local is None or fac.mm_sort_end_local is None:
                    self.warnings.append(f"Facility {name} ({fac.facility_type.value}) missing MM sort window")

            # Launch/Hybrid should have last mile sort windows
            if fac.facility_type in (FacilityType.LAUNCH, FacilityType.HYBRID):
                if fac.lm_sort_start_local is None or fac.lm_sort_end_local is None:
                    self.warnings.append(f"Facility {name} ({fac.facility_type.value}) missing LM sort window")

            # Hubs/Hybrids should have outbound windows (for CPT generation)
            if fac.facility_type in (FacilityType.HUB, FacilityType.HYBRID):
                if fac.outbound_window_start_local is None or fac.outbound_window_end_local is None:
                    self.warnings.append(f"Hub/Hybrid {name} missing outbound window")
                if fac.outbound_cpt_count is None or fac.outbound_cpt_count < 1:
                    self.warnings.append(f"Hub/Hybrid {name} missing outbound_cpt_count")

    def validate_facility_references(self):
        """Validate that regional_sort_hub references exist and are valid types."""
        facilities: dict[str, Facility] = self.data["facilities"]

        for name, fac in facilities.items():
            if fac.regional_sort_hub:
                if fac.regional_sort_hub not in facilities:
                    self.errors.append(f"Facility {name} references unknown regional_sort_hub: {fac.regional_sort_hub}")
                else:
                    # Regional sort hub must be hub or hybrid (not launch)
                    regional_hub_fac = facilities[fac.regional_sort_hub]
                    if regional_hub_fac.facility_type == FacilityType.LAUNCH:
                        self.errors.append(
                            f"Facility {name} has regional_sort_hub={fac.regional_sort_hub} which is a launch facility. "
                            f"Regional sort hubs must be hub or hybrid."
                        )

    def validate_regional_sort_hub_types(self):
        """
        Validate that any facility serving as a regional_sort_hub is hub or hybrid.

        RULE: If a facility is designated as an RSH (by any other facility),
        it must be type 'hub' or 'hybrid', not 'launch'.
        """
        facilities: dict[str, Facility] = self.data["facilities"]

        # Build set of all facilities serving as RSH for someone
        facilities_serving_as_rsh = set()
        for fac in facilities.values():
            if fac.regional_sort_hub:
                facilities_serving_as_rsh.add(fac.regional_sort_hub)

        # Validate each RSH is hub or hybrid
        for rsh_name in facilities_serving_as_rsh:
            rsh_fac = facilities[rsh_name]
            if rsh_fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                # Find which facilities reference this invalid RSH
                referencing_facilities = [
                    name for name, fac in facilities.items()
                    if fac.regional_sort_hub == rsh_name
                ]
                self.errors.append(
                    f"Facility {rsh_name} is designated as regional_sort_hub for {len(referencing_facilities)} "
                    f"facilities {referencing_facilities[:3]}{'...' if len(referencing_facilities) > 3 else ''} "
                    f"but has type '{rsh_fac.facility_type.value}'. RSHs must be 'hub' or 'hybrid'."
                )

    def validate_non_injection_hub_hierarchy(self):
        """
        Validate non-injection hubs have proper child relationships.

        RULE: Non-injection hub/hybrid facilities are regional facilities designed
        to handle regional traffic, not national intermediate traffic. They should
        have children facilities defined via regional_sort_hub.

        This validation warns if a non-injection hub has no children, suggesting
        it may be misconfigured (should either be an injection node or have children).
        """
        facilities: dict[str, Facility] = self.data["facilities"]
        injection_dist = self.data["injection_distribution"]

        # Build set of facilities that receive injection volume
        share_cols = [c for c in injection_dist.columns if c.startswith('share_')]
        if not share_cols and 'absolute_share' in injection_dist.columns:
            share_cols = ['absolute_share']

        injection_facilities = set()
        for _, row in injection_dist.iterrows():
            fac_name = str(row["facility_name"]).strip()
            # If facility has non-zero share in ANY year, it's an injection facility
            if any(row[col] > 0 for col in share_cols if col in row):
                injection_facilities.add(fac_name)

        # Build children mapping (facilities that designate this facility as their RSH)
        children_map = defaultdict(set)
        for name, fac in facilities.items():
            if fac.regional_sort_hub and fac.regional_sort_hub != name:
                children_map[fac.regional_sort_hub].add(name)

        # Check each non-injection hub/hybrid
        for name, fac in facilities.items():
            is_injection_facility = name in injection_facilities
            if fac.facility_type in (FacilityType.HUB, FacilityType.HYBRID) and not is_injection_facility:
                if name not in children_map or len(children_map[name]) == 0:
                    self.warnings.append(
                        f"Facility {name} is a non-injection {fac.facility_type.value} with no children facilities. "
                        f"Non-injection hubs are regional facilities designed for regional traffic. "
                        f"Consider either: (1) adding it to injection_distribution, or (2) assigning it children "
                        f"via regional_sort_hub."
                    )
                else:
                    children_list = sorted(list(children_map[name]))
                    logger.debug(
                        f"Non-injection hub {name} serves {len(children_list)} children: "
                        f"{children_list[:5]}{'...' if len(children_list) > 5 else ''}"
                    )

    def validate_injection_nodes(self):
        """Validate injection distribution references valid facilities."""
        facilities: dict[str, Facility] = self.data["facilities"]
        injection_dist = self.data["injection_distribution"]

        # Find share columns (year-based or legacy)
        share_cols = [c for c in injection_dist.columns if c.startswith('share_')]
        if not share_cols and 'absolute_share' in injection_dist.columns:
            share_cols = ['absolute_share']

        for _, row in injection_dist.iterrows():
            fac_name = str(row["facility_name"]).strip()

            if fac_name not in facilities:
                self.errors.append(f"Injection distribution references unknown facility: {fac_name}")
                continue

            fac = facilities[fac_name]

            # Check if facility receives any injection volume
            has_injection = any(row[col] > 0 for col in share_cols if col in row)

            if has_injection:
                # Warn if launch facility receives injection (typically only hubs/hybrids)
                if fac.facility_type == FacilityType.LAUNCH:
                    self.warnings.append(
                        f"Launch facility {fac_name} receives injection volume. "
                        f"Verify this is intentional (typically only hubs/hybrids accept injection)."
                    )

    def validate_mileage_bands(self):
        """Validate mileage bands are contiguous and non-overlapping."""
        bands: list[MileageBand] = self.data["mileage_bands"]

        if not bands:
            self.errors.append("No mileage bands defined")
            return

        # Check zones are sequential (but can start from any number)
        zones = [b.zone for b in bands]
        if zones != sorted(zones):
            self.errors.append(f"Mileage band zones must be in ascending order: got {zones}")

        # Check for gaps in zone sequence
        for i in range(len(zones) - 1):
            if zones[i + 1] - zones[i] != 1:
                self.warnings.append(
                    f"Gap in mileage band zones: {zones[i]} to {zones[i + 1]}"
                )

        # Check bands are contiguous in mileage
        for i in range(len(bands) - 1):
            current = bands[i]
            next_band = bands[i + 1]

            if current.mileage_band_max > next_band.mileage_band_min:
                self.errors.append(
                    f"Mileage bands overlap: zone {current.zone} max ({current.mileage_band_max}) > "
                    f"zone {next_band.zone} min ({next_band.mileage_band_min})"
                )
            elif current.mileage_band_max < next_band.mileage_band_min:
                self.warnings.append(
                    f"Gap in mileage bands between zone {current.zone} and {next_band.zone}: "
                    f"{current.mileage_band_max} to {next_band.mileage_band_min}"
                )

        # Check reasonable values
        for band in bands:
            if band.circuity_factor < 1.0:
                self.warnings.append(f"Zone {band.zone} has circuity_factor < 1.0: {band.circuity_factor}")
            if band.mph <= 0:
                self.errors.append(f"Zone {band.zone} has non-positive mph: {band.mph}")

    def validate_timing_params(self):
        """Validate timing parameters are positive."""
        timing: TimingParams = self.data["timing_params"]

        if timing.induction_sort_minutes < 0:
            self.errors.append(f"induction_sort_minutes must be non-negative: {timing.induction_sort_minutes}")
        if timing.middle_mile_crossdock_minutes < 0:
            self.errors.append(
                f"middle_mile_crossdock_minutes must be non-negative: {timing.middle_mile_crossdock_minutes}")
        if timing.middle_mile_sort_minutes < 0:
            self.errors.append(f"middle_mile_sort_minutes must be non-negative: {timing.middle_mile_sort_minutes}")
        if timing.sort_group_sort_minutes < 0:
            self.errors.append(f"sort_group_sort_minutes must be non-negative: {timing.sort_group_sort_minutes}")
        if timing.route_sort_minutes < 0:
            self.errors.append(f"route_sort_minutes must be non-negative: {timing.route_sort_minutes}")

    def validate_scenarios(self):
        """Validate scenarios reference valid years and day types."""
        scenarios = self.data["scenarios"]
        demand = self.data["demand"]
        zips_df = self.data["zips"]

        valid_years = set(demand["year"].unique())
        valid_day_types = {"offpeak", "peak"}

        # Get available facility_YYYY years from zips
        facility_years = set()
        for col in zips_df.columns:
            if col.startswith('facility_'):
                try:
                    year = int(col.replace('facility_', ''))
                    facility_years.add(year)
                except ValueError:
                    continue

        for _, row in scenarios.iterrows():
            scenario_id = row.get("scenario_id", "unknown")
            year = row["year"]
            day_type = str(row["day_type"]).lower().strip()
            demand_source = str(row.get("demand_source", "population")).lower().strip()

            if day_type not in valid_day_types:
                self.errors.append(
                    f"Scenario '{scenario_id}' has invalid day_type: {day_type}. "
                    f"Must be one of {valid_day_types}"
                )

            # Validation depends on demand source
            if demand_source == DemandSource.POPULATION.value:
                # Population-based scenarios need demand sheet and facility_YYYY column
                if year not in valid_years:
                    self.errors.append(
                        f"Scenario '{scenario_id}' (demand_source=population) references year {year} "
                        f"not in demand sheet. Valid years: {sorted(valid_years)}"
                    )

                if year not in facility_years:
                    self.errors.append(
                        f"Scenario '{scenario_id}' (demand_source=population) uses year {year} "
                        f"but no facility_{year} column in zips. Available years: {sorted(facility_years)}"
                    )

            elif demand_source == DemandSource.MARKET.value:
                # Market-based scenarios need market_demand sheet with matching year/day_type
                # (validated in validate_market_demand)
                pass

    def validate_service_commitments(self):
        """Validate service commitments have valid structure."""
        commitments: list[ServiceCommitment] = self.data["service_commitments"]
        facilities: dict[str, Facility] = self.data["facilities"]
        bands: list[MileageBand] = self.data["mileage_bands"]

        valid_zones = {b.zone for b in bands}
        valid_zones.add(0)  # Zone 0 for direct injection

        for sc in commitments:
            # Check origin reference (unless wildcard)
            if sc.origin != "*" and sc.origin not in facilities:
                self.errors.append(f"Service commitment references unknown origin: {sc.origin}")

            # Check dest reference (unless wildcard)
            if sc.dest != "*" and sc.dest not in facilities:
                self.errors.append(f"Service commitment references unknown dest: {sc.dest}")

            # Check zone reference (if zone-based)
            if sc.zone is not None and sc.zone not in valid_zones:
                self.warnings.append(f"Service commitment references zone {sc.zone} not in mileage_bands")

            # Check positive values
            if sc.sla_days < 1:
                self.errors.append(f"Service commitment sla_days must be >= 1: {sc.sla_days}")
            if sc.sla_buffer_days < 0:
                self.warnings.append(f"Service commitment has negative sla_buffer_days: {sc.sla_buffer_days}")
            if sc.priority_weight <= 0:
                self.errors.append(f"Service commitment priority_weight must be positive: {sc.priority_weight}")

    def validate_zips_facility_references(self):
        """Validate that facility names in zips sheet exist in facilities."""
        facilities: dict[str, Facility] = self.data["facilities"]
        zips_df = self.data["zips"]

        # Get all facility_YYYY columns
        facility_cols = [c for c in zips_df.columns if c.startswith('facility_')]

        unknown_facilities = set()
        for col in facility_cols:
            # Get unique non-null facility names in this column
            fac_names = zips_df[col].dropna().unique()
            for fac_name in fac_names:
                fac_name_str = str(fac_name).strip()
                if fac_name_str and fac_name_str not in facilities:
                    unknown_facilities.add((col, fac_name_str))

        for col, fac_name in unknown_facilities:
            self.errors.append(f"Zips sheet {col} references unknown facility: {fac_name}")

    def validate_facility_markets(self):
        """
        Validate facility market assignments.

        Rules:
        1. Each market should be assigned to exactly one facility (1:1 relationship)
        2. If market_demand sheet exists, all facilities should have market assignments
        """
        facilities: dict[str, Facility] = self.data["facilities"]
        market_demand = self.data.get("market_demand")

        # Build market -> facilities mapping to detect duplicates
        market_to_facilities = defaultdict(list)
        facilities_without_market = []

        for name, fac in facilities.items():
            if fac.market:
                market_to_facilities[fac.market].append(name)
            else:
                facilities_without_market.append(name)

        # Check for duplicate market assignments (same market used by multiple facilities)
        for market, fac_list in market_to_facilities.items():
            if len(fac_list) > 1:
                self.errors.append(
                    f"Market '{market}' is assigned to multiple facilities: {fac_list}. "
                    f"Each market must be assigned to exactly one facility."
                )

        # If market_demand exists, check that facilities have markets
        if market_demand is not None:
            if facilities_without_market:
                self.warnings.append(
                    f"{len(facilities_without_market)} facilities missing market assignment "
                    f"(needed for market_demand lookups): "
                    f"{facilities_without_market[:5]}{'...' if len(facilities_without_market) > 5 else ''}"
                )

    def validate_market_demand(self):
        """
        Validate market_demand sheet if present.

        Rules:
        1. All markets in market_demand must map to facilities
        2. No duplicate entries (same origin_market/dest_market/year/day_type)
        3. Scenarios using demand_source='market' must have data in market_demand
        """
        market_demand = self.data.get("market_demand")
        if market_demand is None:
            # Check if any scenarios require market_demand
            scenarios = self.data["scenarios"]
            market_scenarios = scenarios[scenarios['demand_source'] == DemandSource.MARKET.value]
            if not market_scenarios.empty:
                scenario_ids = market_scenarios['scenario_id'].tolist()
                self.errors.append(
                    f"Scenarios {scenario_ids} use demand_source='market' but no market_demand sheet found"
                )
            return

        facilities: dict[str, Facility] = self.data["facilities"]

        # Build market -> facility mapping
        market_to_fac = {fac.market: name for name, fac in facilities.items() if fac.market}

        # Check all markets in market_demand are mapped to facilities
        all_origin_markets = set(market_demand['origin_market'].unique())
        all_dest_markets = set(market_demand['dest_market'].unique())
        all_markets = all_origin_markets | all_dest_markets

        unmapped_markets = all_markets - set(market_to_fac.keys())
        if unmapped_markets:
            self.errors.append(
                f"market_demand contains {len(unmapped_markets)} markets not mapped to facilities: "
                f"{sorted(list(unmapped_markets))[:10]}{'...' if len(unmapped_markets) > 10 else ''}"
            )

        # Check for duplicate entries
        dupes = market_demand.groupby(
            ['origin_market', 'dest_market', 'year', 'day_type']
        ).size()
        dupes = dupes[dupes > 1]
        if len(dupes) > 0:
            self.errors.append(
                f"market_demand has {len(dupes)} duplicate entries "
                f"(same origin_market/dest_market/year/day_type)"
            )

        # Check scenarios using demand_source='market' have data
        scenarios = self.data["scenarios"]
        market_scenarios = scenarios[scenarios['demand_source'] == DemandSource.MARKET.value]

        for _, scenario in market_scenarios.iterrows():
            scenario_id = scenario['scenario_id']
            year = scenario['year']
            day_type = str(scenario['day_type']).lower().strip()

            mask = (
                (market_demand['year'] == year) &
                (market_demand['day_type'].str.lower() == day_type)
            )
            if not mask.any():
                self.errors.append(
                    f"Scenario '{scenario_id}' uses demand_source='market' "
                    f"but no market_demand data for year={year}, day_type={day_type}"
                )


def validate_inputs(data: dict) -> None:
    """
    Validate all inputs and raise ValidationError if critical errors found.

    Warnings are logged but don't stop execution.
    """
    validator = InputValidator(data)
    errors, warnings = validator.validate_all()

    for warning in warnings:
        logger.warning(f"Validation warning: {warning}")

    if errors:
        for error in errors:
            logger.error(f"Validation error: {error}")
        raise ValidationError(f"Input validation failed with {len(errors)} error(s). See log for details.")

    logger.info("Input validation passed")