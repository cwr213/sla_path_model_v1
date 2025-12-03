"""
Diagnostic tool for analyzing path timing calculations.
"""

# =============================================================================
# CONFIGURATION - EDIT THESE VALUES
# =============================================================================
INPUT_FILE = r"C:\Users\cwr21\OneDrive\Documents\python_data\python_projects\sla_path_model_v1\data\input_sla_model_v1.xlsx"
ORIGIN = "BNA01"
DEST = "BOS01"
SCENARIO_ID = None  # None = use first scenario, or specify e.g. "2026_peak_paths_v1"
# =============================================================================

from datetime import datetime
from typing import Optional

from sla_path_model.io_loader import InputLoader
from sla_path_model.validators import InputValidator
from sla_path_model.demand_builder import DemandBuilder
from sla_path_model.path_enumeration import PathEnumerator
from sla_path_model.cpt_generator import CPTGenerator
from sla_path_model.timing_engine import TimingEngine
from sla_path_model.feasibility import FeasibilityChecker
from sla_path_model.geo import haversine_miles
from sla_path_model.time_utils import utc_to_local
from sla_path_model.config import FlowType, FacilityType, ODDemand

# Suppress verbose logging from other modules
import logging

logging.getLogger("sla_path_model").setLevel(logging.WARNING)


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'=' * 70}")
    print(f" {title}")
    print('=' * 70)


def print_facility_info(fac, label: str):
    """Print facility details."""
    print(f"\n{label}:")
    print(f"  Name: {fac.name}")
    print(f"  Type: {fac.facility_type.value}")
    print(f"  Timezone: {fac.timezone}")
    print(f"  Location: ({fac.lat:.4f}, {fac.lon:.4f})")

    # MM sort window (used for outbound processing at origin/intermediate)
    mm_window = fac.get_mm_sort_window()
    if mm_window:
        print(f"  MM Sort Window: {mm_window.start_local} - {mm_window.end_local}")

    # LM sort window (used at destination)
    lm_window = fac.get_lm_sort_window()
    if lm_window:
        print(f"  LM Sort Window: {lm_window.start_local} - {lm_window.end_local}")

    if fac.lm_sort_end_local:
        print(f"  LM Sort End (delivery deadline): {fac.lm_sort_end_local}")


def print_timing_params(timing):
    """Print timing parameters."""
    print(f"\nTiming Parameters:")
    print(f"  Induction Sort: {timing.induction_sort_minutes} min")
    print(f"  Middle Mile Crossdock: {timing.middle_mile_crossdock_minutes} min")
    print(f"  Middle Mile Sort: {timing.middle_mile_sort_minutes} min")
    print(f"  Last Mile Sort: {timing.last_mile_sort_minutes} min")


def print_step_detail(step, facilities: dict):
    """Print detailed info about a single path step."""
    from_fac = facilities.get(step.from_facility)
    to_fac = facilities.get(step.to_facility)

    print(f"\n  Step {step.step_sequence}: {step.step_type.value.upper()}")
    print(f"    From: {step.from_facility} → To: {step.to_facility}")

    if step.distance_miles and step.distance_miles > 0:
        print(f"    Distance: {step.distance_miles:.1f} mi")

    # Show times in local timezone of the 'from' facility
    if from_fac:
        start_local = utc_to_local(step.start_utc, from_fac.timezone)
        print(
            f"    Start: {step.start_utc.strftime('%Y-%m-%d %H:%M')} UTC = {start_local.strftime('%Y-%m-%d %H:%M')} local")
    else:
        print(f"    Start: {step.start_utc.strftime('%Y-%m-%d %H:%M')} UTC")

    # Show end time in local timezone of the 'to' facility
    if to_fac:
        end_local = utc_to_local(step.end_utc, to_fac.timezone)
        print(
            f"    End:   {step.end_utc.strftime('%Y-%m-%d %H:%M')} UTC = {end_local.strftime('%Y-%m-%d %H:%M')} local")
    else:
        print(f"    End:   {step.end_utc.strftime('%Y-%m-%d %H:%M')} UTC")

    print(f"    Duration: {step.duration_minutes:.0f} min")

    if step.sort_window_dwell_minutes > 0:
        print(f"    Sort Window Dwell: {step.sort_window_dwell_minutes:.0f} min (waited for window to open)")
    if step.cpt_dwell_minutes > 0:
        print(f"    CPT Dwell: {step.cpt_dwell_minutes:.0f} min (waited for trailer departure)")
    if step.total_dwell_minutes > 0:
        print(f"    Total Dwell: {step.total_dwell_minutes:.0f} min")


def diagnose_od_pair(
        input_file: str,
        origin_code: str,
        dest_code: str,
        scenario_id: Optional[str] = None
):
    """
    Diagnose timing calculations for a specific OD pair.
    Shows all paths and detailed timing breakdown.
    """
    print_section(f"DIAGNOSTIC: {origin_code} → {dest_code}")

    # Load data
    print("\nLoading data...")
    loader = InputLoader(input_file)
    data = loader.load_all()

    # Validate
    validator = InputValidator(data)
    errors, warnings = validator.validate_all()
    if errors:
        print("Validation errors:")
        for err in errors:
            print(f"  - {err}")
        return
    if warnings:
        print("Validation warnings:")
        for warn in warnings:
            print(f"  - {warn}")

    # Get facilities (already a dict keyed by name)
    facilities = data["facilities"]

    if origin_code not in facilities:
        print(f"ERROR: Origin '{origin_code}' not found in facilities")
        print(f"Available facilities: {list(facilities.keys())[:10]}...")
        return
    if dest_code not in facilities:
        print(f"ERROR: Destination '{dest_code}' not found in facilities")
        return

    origin_fac = facilities[origin_code]
    dest_fac = facilities[dest_code]

    print_facility_info(origin_fac, "ORIGIN FACILITY")
    print_facility_info(dest_fac, "DESTINATION FACILITY")

    # Distance
    direct_miles = haversine_miles(origin_fac.lat, origin_fac.lon, dest_fac.lat, dest_fac.lon)
    print(f"\nDirect Distance: {direct_miles:.1f} miles")

    # Get zone from mileage bands
    mileage_bands = data["mileage_bands"]
    zone = None
    matched_band = None
    for band in mileage_bands:
        if band.mileage_band_min <= direct_miles < band.mileage_band_max:
            zone = band.zone
            matched_band = band
            print(
                f"Zone: {zone} (band {band.mileage_band_min}-{band.mileage_band_max} mi, {band.mph} mph, circuity={band.circuity_factor})")
            break

    if zone is None:
        print("WARNING: No mileage band matched!")

    # Timing params
    timing_params = data["timing_params"]
    print_timing_params(timing_params)

    # Service commitment
    print_section("SERVICE COMMITMENT LOOKUP")
    feasibility_checker = FeasibilityChecker(data["service_commitments"])
    commitment = feasibility_checker.get_commitment(origin_code, dest_code, zone or 0)
    if commitment:
        print(f"Matched Rule: origin={commitment.origin}, dest={commitment.dest}, zone={commitment.zone}")
        print(f"SLA Days: {commitment.sla_days}")
        print(f"Buffer Days: {commitment.sla_buffer_days}")
        total_allowed = commitment.sla_days + commitment.sla_buffer_days
        print(f"Total Allowed: {total_allowed} days = {total_allowed * 24} hours")
    else:
        print("WARNING: No service commitment found!")

    # Get scenario
    scenarios_df = data["scenarios"]
    if scenario_id:
        scenario_match = scenarios_df[scenarios_df["scenario_id"] == scenario_id]
        if scenario_match.empty:
            print(f"ERROR: Scenario '{scenario_id}' not found")
            return
        scenario = scenario_match.iloc[0]
    else:
        scenario = scenarios_df.iloc[0]
        scenario_id = scenario["scenario_id"]
        print(f"\nUsing first scenario: {scenario_id}")

    # Get reference date from run_settings
    run_settings = data["run_settings"]
    reference_date = run_settings.reference_injection_date
    if hasattr(reference_date, 'date'):
        reference_datetime = reference_date
    else:
        reference_datetime = datetime.combine(reference_date, datetime.min.time())
    print(f"Reference Date: {reference_datetime.date()}")

    # Generate CPTs
    print_section("CPT GENERATION")
    cpt_gen = CPTGenerator(
        facilities=facilities,
        arc_cpts=data.get("arc_cpts", [])
    )

    # Show relevant CPTs for origin
    origin_cpts = cpt_gen.get_cpts_for_arc(origin_code, dest_code)
    print(f"\nCPTs for {origin_code} → {dest_code}: {len(origin_cpts)} found")
    for cpt in sorted(origin_cpts, key=lambda x: x.cpt_local)[:8]:
        print(f"  {cpt.cpt_local} local (seq={cpt.cpt_sequence}, days={','.join(cpt.days_of_week)})")
    if len(origin_cpts) > 8:
        print(f"  ... and {len(origin_cpts) - 8} more")

    # Build demand to check for OD record
    print_section("DEMAND LOOKUP")
    demand_builder = DemandBuilder(
        facilities=facilities,
        zips_df=data["zips"],
        demand_df=data["demand"],
        injection_df=data["injection_distribution"],
        scenarios_df=scenarios_df,
        mileage_bands=mileage_bands
    )
    all_demands = demand_builder.build_demands()

    # Filter to this OD pair
    od_demands = [d for d in all_demands
                  if d.scenario_id == scenario_id
                  and d.origin == origin_code
                  and d.dest == dest_code]

    if not od_demands:
        print(f"No demand records found for {origin_code} → {dest_code}")
        print("This could mean:")
        print("  - Origin doesn't inject to this destination")
        print("  - Volume below threshold (0.01 pkgs)")

        # Check if O=D
        if origin_code == dest_code:
            print(f"\nNote: This is an O=D pair.")
            print(f"  Origin type: {origin_fac.facility_type.value}")
            if origin_fac.facility_type == FacilityType.HYBRID:
                print("  HYBRID facilities CAN have O=D middle mile demand")
            else:
                print("  Only HYBRID facilities can have O=D middle mile demand")

        # Create a synthetic demand record for diagnosis
        print("\nCreating synthetic demand record for diagnosis...")
        od_demands = [ODDemand(
            scenario_id=scenario_id,
            origin=origin_code,
            dest=dest_code,
            pkgs_day=1.0,
            zone=zone or 2,
            flow_type=FlowType.MIDDLE_MILE,
            day_type="peak"
        )]

    for od in od_demands:
        print(f"\nDemand Record:")
        print(f"  Flow Type: {od.flow_type.value}")
        print(f"  Packages/Day: {od.pkgs_day:,.2f}")
        print(f"  Zone: {od.zone}")

    # Enumerate paths
    print_section("PATH ENUMERATION & TIMING")
    path_enum = PathEnumerator(
        facilities=facilities,
        run_settings=run_settings
    )

    timing_engine = TimingEngine(
        facilities=facilities,
        mileage_bands=mileage_bands,
        timing_params=timing_params,
        cpt_generator=cpt_gen,
        reference_date=reference_datetime,
        reference_injection_time=run_settings.reference_injection_time
    )

    # Collect all paths with their timing results
    all_results = []  # list of (od, path, timing_result)

    for od in od_demands:
        paths = path_enum.enumerate_paths_for_od(od.origin, od.dest)
        print(f"\nPaths enumerated for {od.flow_type.value} flow: {len(paths)}")

        for path in paths:
            result = timing_engine.calculate_path_timing(path)
            num_touches = len(path.path_nodes) - 1  # hops = nodes - 1
            all_results.append((od, path, result, num_touches))

    if not all_results:
        print("\nNo paths enumerated! Check path enumeration logic.")
        return

    # Group by path_type and find optimal path per category
    # Ranking: TIT (ascending), then touches (ascending), then miles (ascending)
    from collections import defaultdict
    paths_by_type = defaultdict(list)

    for od, path, result, num_touches in all_results:
        paths_by_type[path.path_type.value].append((od, path, result, num_touches))

    # Sort each group and get the best path
    optimal_paths = []
    print(f"\nPaths by category:")
    for path_type, paths_list in sorted(paths_by_type.items()):
        # Sort by: TIT, then touches, then total miles
        paths_list.sort(key=lambda x: (x[2].tit_hours, x[3], x[1].total_path_miles))
        best = paths_list[0]
        optimal_paths.append(best)

        od, path, result, num_touches = best
        print(f"  {path_type}: {len(paths_list)} paths → Best: {' → '.join(path.path_nodes)} "
              f"(TIT={result.tit_hours:.1f}h, {num_touches} touch{'es' if num_touches != 1 else ''}, "
              f"{path.total_path_miles:.0f}mi)")

    # Show detailed breakdown for optimal paths only
    print_section("OPTIMAL PATH DETAILS (by category)")

    for od, path, result, num_touches in optimal_paths:
        print(f"\n{'─' * 70}")
        print(f"PATH TYPE: {path.path_type.value.upper()}")
        print(f"PATH: {' → '.join(path.path_nodes)}")
        print(f"Sort Level: {path.sort_level.value} → Dest Sort Level: {path.dest_sort_level.value}")
        print(f"Touches: {num_touches} | Miles: {path.total_path_miles:.1f} | ATW: {path.atw_factor:.2f}")
        print('─' * 70)

        # Print each step in chronological order
        print("\nSTEP-BY-STEP BREAKDOWN (chronological):")
        for step in result.steps:
            print_step_detail(step, facilities)

        # Summary
        print(f"\n{'─' * 40}")
        print("TIMING SUMMARY:")
        print('─' * 40)

        inj_local = utc_to_local(result.required_injection_utc, origin_fac.timezone)
        arr_local = utc_to_local(result.delivery_datetime_utc, dest_fac.timezone)

        print(f"  Injection Time:  {result.required_injection_utc.strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"                   {inj_local.strftime('%Y-%m-%d %H:%M')} local")
        print(f"  Arrival Time:    {result.delivery_datetime_utc.strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"                   {arr_local.strftime('%Y-%m-%d %H:%M')} local")
        print(f"\n  Total TIT: {result.tit_hours:.2f} hours ({result.tit_hours / 24:.2f} days)")
        print(f"  Sort Window Dwell: {result.sort_window_dwell_hours:.2f} hours")
        print(f"  CPT Dwell: {result.cpt_dwell_hours:.2f} hours")
        print(f"  Total Dwell: {result.total_dwell_hours:.2f} hours")
        print(f"  Active Arcs Only: {'Yes' if result.uses_only_active_arcs else 'No'}")

        # SLA check
        if commitment:
            sla_hours = (commitment.sla_days + commitment.sla_buffer_days) * 24
            meets_sla = result.tit_hours <= sla_hours
            slack = sla_hours - result.tit_hours

            print(f"\n  SLA Target: {sla_hours:.0f} hours ({commitment.sla_days}+{commitment.sla_buffer_days} days)")
            if meets_sla:
                print(f"  ✓ MEETS SLA (slack: {slack:.2f} hours)")
            else:
                print(f"  ✗ MISSES SLA (over by: {-slack:.2f} hours)")


def main():
    diagnose_od_pair(
        INPUT_FILE,
        ORIGIN.upper(),
        DEST.upper(),
        SCENARIO_ID
    )


if __name__ == "__main__":
    main()