"""
Input validation functions.
"""
from typing import Optional

from .config import Facility, FacilityType, MileageBand, ServiceCommitment, TimingParams
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
        self.validate_injection_nodes()
        self.validate_mileage_bands()
        self.validate_timing_params()
        self.validate_scenarios()
        self.validate_service_commitments()

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

            # Injection nodes should have outbound windows
            if fac.is_injection_node:
                if fac.outbound_window_start_local is None or fac.outbound_window_end_local is None:
                    self.errors.append(f"Injection facility {name} missing outbound window")
                if fac.outbound_cpt_count is None or fac.outbound_cpt_count < 1:
                    self.errors.append(f"Injection facility {name} must have outbound_cpt_count >= 1")

    def validate_facility_references(self):
        """Validate that parent_hub_name and regional_sort_hub references exist."""
        facilities: dict[str, Facility] = self.data["facilities"]

        for name, fac in facilities.items():
            if fac.parent_hub_name and fac.parent_hub_name not in facilities:
                self.errors.append(f"Facility {name} references unknown parent_hub_name: {fac.parent_hub_name}")

            if fac.regional_sort_hub and fac.regional_sort_hub not in facilities:
                self.errors.append(f"Facility {name} references unknown regional_sort_hub: {fac.regional_sort_hub}")

    def validate_injection_nodes(self):
        """Validate injection distribution references valid facilities."""
        facilities: dict[str, Facility] = self.data["facilities"]
        injection_dist = self.data["injection_distribution"]

        for _, row in injection_dist.iterrows():
            fac_name = str(row["facility_name"]).strip()

            if fac_name not in facilities:
                self.errors.append(f"Injection distribution references unknown facility: {fac_name}")
                continue

            fac = facilities[fac_name]
            if not fac.is_injection_node:
                self.warnings.append(f"Facility {fac_name} in injection distribution but is_injection_node=False")

            if fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                self.errors.append(
                    f"Injection facility {fac_name} must be hub or hybrid, got {fac.facility_type.value}")

        # Check all injection nodes are in distribution
        injection_facs = set(injection_dist["facility_name"].astype(str).str.strip())
        for name, fac in facilities.items():
            if fac.is_injection_node and name not in injection_facs:
                self.warnings.append(f"Facility {name} has is_injection_node=True but not in injection_distribution")

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
        if timing.last_mile_sort_minutes < 0:
            self.errors.append(f"last_mile_sort_minutes must be non-negative: {timing.last_mile_sort_minutes}")

    def validate_scenarios(self):
        """Validate scenarios reference valid years and day types."""
        scenarios = self.data["scenarios"]
        demand = self.data["demand"]

        valid_years = set(demand["year"].unique())
        valid_day_types = {"offpeak", "peak"}

        for _, row in scenarios.iterrows():
            year = row["year"]
            day_type = str(row["day_type"]).lower().strip()

            if year not in valid_years:
                self.errors.append(f"Scenario references unknown year: {year}. Valid years: {valid_years}")

            if day_type not in valid_day_types:
                self.errors.append(f"Scenario has invalid day_type: {day_type}. Must be one of {valid_day_types}")

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