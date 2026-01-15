#!/usr/bin/env python3
"""
Diagnostic tool for analyzing specific OD pair paths with detailed timing breakdown.

Configure the parameters below and run in PyCharm or command line.
"""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# ============== CONFIGURATION ==============
ORIGIN = "ATL02"
DEST = "ATL02"
PATH_TYPE = None              # None = all, or "direct_injection", "od_mm", "2_touch", "3_touch", "4_touch"
SORT_LEVEL = None             # None = all, or "region", "market", "sort_group"
DEST_SORT_LEVEL = None        # None = all, or "region", "market", "sort_group"
SHOW_DETAIL_FOR = None        # None = summary only, or path number (1, 2, 3...) for detailed breakdown
SHOW_TOP_N_DETAILS = 0        # Show detailed breakdown for top N paths (0 = summary only, ignored if SHOW_DETAIL_FOR is set)
SHOW_ALL_DETAILS = False      # True = show detailed breakdown for ALL paths (overrides above)
INPUT_FILE = r"C:\Users\ChrisRallis\Desktop\python_projects\sla_path_model_v1\data\input_sla_model_v1.xlsx"
# ===========================================

from datetime import datetime, timedelta
from typing import Optional

from sla_path_model.config import (
    PathCandidate, PathTimingResult, PathType, SortLevel, StepType,
    FacilityType, FlowType, Facility
)
from sla_path_model.io_loader import InputLoader
from sla_path_model.path_enumeration import PathEnumerator
from sla_path_model.timing_engine import TimingEngine
from sla_path_model.cpt_generator import CPTGenerator
from sla_path_model.feasibility import FeasibilityChecker
from sla_path_model.geo import haversine_miles, get_zone_for_distance
from sla_path_model.time_utils import utc_to_local


def get_tz_abbrev(facility: Facility, dt_utc: datetime) -> str:
    """Get timezone abbreviation (EST/EDT, PST/PDT, etc.) for a given UTC datetime."""
    local_dt = dt_utc.replace(tzinfo=None)
    local_dt = utc_to_local(dt_utc, facility.timezone)
    # Get the timezone abbreviation
    return local_dt.strftime("%Z") if hasattr(local_dt, 'strftime') else str(facility.timezone)


def format_local_time(dt_utc: datetime, facility: Facility) -> str:
    """Format UTC datetime as local time with day and timezone."""
    local_dt = utc_to_local(dt_utc, facility.timezone)
    tz_abbrev = local_dt.strftime("%Z")
    return f"{local_dt.strftime('%a %H:%M')} {tz_abbrev}"


def format_window(start_local, end_local) -> str:
    """Format a time window as HH:MM-HH:MM."""
    if start_local is None or end_local is None:
        return "N/A"
    return f"{start_local.strftime('%H:%M')}-{end_local.strftime('%H:%M')}"


def print_summary_table(timings: list[PathTimingResult], facilities: dict[str, Facility]):
    """Print compact summary table of all paths."""
    print()
    print(f"  #  Path Type         Sort Lvl    Dest Sort   Nodes                                TIT(hrs)  SLA     Slack")
    print(f"  -  ---------         --------    ---------   -----                                --------  ---     -----")

    for i, timing in enumerate(timings, 1):
        path = timing.path
        nodes_str = "→".join(path.path_nodes)
        if len(nodes_str) > 35:
            nodes_str = nodes_str[:32] + "..."

        sla_status = "MET" if timing.sla_met else "MISS"
        slack_sign = "+" if timing.sla_slack_hours >= 0 else ""

        print(f"  {i:<2} {path.path_type.value:<17} {path.sort_level.value:<11} {path.dest_sort_level.value:<11} "
              f"{nodes_str:<36} {timing.tit_hours:>6.1f}    {sla_status:<6}  {slack_sign}{timing.sla_slack_hours:.1f}")


def print_detailed_breakdown(timing: PathTimingResult, facilities: dict[str, Facility], path_num: int):
    """Print detailed step-by-step breakdown for a single path."""
    path = timing.path
    origin_fac = facilities[path.origin]
    dest_fac = facilities[path.dest]

    print()
    print(f"{'='*90}")
    print(f"=== DETAILED BREAKDOWN: Path #{path_num} ({path.path_type.value}, {path.sort_level.value}) ===")
    print(f"{'='*90}")
    print()
    print(f"Route: {' → '.join(path.path_nodes)}")
    print(f"Origin: {path.origin} ({origin_fac.facility_type.value}) - {origin_fac.timezone}")
    print(f"Dest:   {path.dest} ({dest_fac.facility_type.value}) - {dest_fac.timezone}")
    print(f"Direct miles: {path.direct_miles:.1f}, Path miles: {path.total_path_miles:.1f}, ATW: {path.atw_factor:.2f}")
    print()

    # Header
    print(f"Step  Type                Facility        Start (local)       End (local)         Dur(min)  Dwell(min)  Notes")
    print(f"────  ────                ────────        ─────────────       ───────────         ────────  ──────────  ─────")

    # Track sort/transit/dwell totals
    total_sort_mins = 0
    total_transit_mins = 0
    total_crossdock_mins = 0
    total_window_dwell = 0
    total_cpt_dwell = 0

    for step in timing.steps:
        step_fac = facilities.get(step.from_facility) or facilities.get(step.to_facility)

        # Determine which facility to use for timezone
        if step.step_type == StepType.TRANSIT:
            from_fac = facilities[step.from_facility]
            to_fac = facilities[step.to_facility]
            facility_str = f"{step.from_facility}→{step.to_facility}"
            start_str = format_local_time(step.start_utc, from_fac)
            end_str = format_local_time(step.end_utc, to_fac)

            # Calculate speed for notes
            if step.distance_miles and step.duration_minutes > 0:
                speed = step.distance_miles / (step.duration_minutes / 60)
                notes = f"{step.distance_miles:.0f}mi @ {speed:.0f}mph"
                if step.cpt_dwell_minutes > 0:
                    notes += f", CPT wait"
            else:
                notes = ""

            total_transit_mins += step.duration_minutes
            total_cpt_dwell += step.cpt_dwell_minutes

        else:
            facility_str = step.from_facility
            start_str = format_local_time(step.start_utc, step_fac)
            end_str = format_local_time(step.end_utc, step_fac)

            # Build notes based on step type
            notes = ""
            if step.step_type == StepType.INDUCTION_SORT:
                mm_window = step_fac.get_mm_sort_window()
                if mm_window:
                    notes = f"MM window: {format_window(mm_window.start_local, mm_window.end_local)}"
                total_sort_mins += step.duration_minutes

            elif step.step_type == StepType.SORT_GROUP_SORT:
                mm_window = step_fac.get_mm_sort_window()
                if mm_window:
                    notes = f"MM window: {format_window(mm_window.start_local, mm_window.end_local)}"
                total_sort_mins += step.duration_minutes

            elif step.step_type == StepType.ROUTE_SORT:
                lm_window = step_fac.get_lm_sort_window()
                if lm_window:
                    notes = f"LM window: {format_window(lm_window.start_local, lm_window.end_local)}"
                total_sort_mins += step.duration_minutes

            elif step.step_type == StepType.FULL_SORT:
                mm_window = step_fac.get_mm_sort_window()
                if mm_window:
                    notes = f"MM window: {format_window(mm_window.start_local, mm_window.end_local)}"
                total_sort_mins += step.duration_minutes

            elif step.step_type == StepType.CROSSDOCK:
                mm_window = step_fac.get_mm_sort_window()
                if mm_window:
                    notes = f"MM window: {format_window(mm_window.start_local, mm_window.end_local)}"
                total_crossdock_mins += step.duration_minutes

            total_window_dwell += step.sort_window_dwell_minutes

        # Dwell display
        dwell_str = ""
        if step.sort_window_dwell_minutes > 0:
            dwell_str = f"{step.sort_window_dwell_minutes:.0f} (win)"
        elif step.cpt_dwell_minutes > 0:
            dwell_str = f"{step.cpt_dwell_minutes:.0f} (cpt)"
        else:
            dwell_str = "0"

        print(f"  {step.step_sequence:<3} {step.step_type.value:<19} {facility_str:<15} "
              f"{start_str:<19} {end_str:<19} {step.duration_minutes:>6.0f}    {dwell_str:<10}  {notes}")

    # Summary line
    print()
    sort_hrs = total_sort_mins / 60
    transit_hrs = total_transit_mins / 60
    crossdock_hrs = total_crossdock_mins / 60
    window_dwell_hrs = total_window_dwell / 60
    cpt_dwell_hrs = total_cpt_dwell / 60
    total_dwell_hrs = window_dwell_hrs + cpt_dwell_hrs

    print(f"Summary: Sort={sort_hrs:.1f}h, Crossdock={crossdock_hrs:.1f}h, Transit={transit_hrs:.1f}h, "
          f"Dwell={total_dwell_hrs:.1f}h (win:{window_dwell_hrs:.1f}, cpt:{cpt_dwell_hrs:.1f}) → TIT={timing.tit_hours:.1f}h")

    sla_status = "MET" if timing.sla_met else "MISS"
    slack_sign = "+" if timing.sla_slack_hours >= 0 else ""
    print(f"SLA: {timing.sla_days} day(s) = {timing.sla_target_hours:.1f}h target → {sla_status} ({slack_sign}{timing.sla_slack_hours:.1f}h slack)")
    print(f"Uses only active arcs: {timing.uses_only_active_arcs}")


def main():
    print(f"{'='*90}")
    print(f"SLA Path Model - Diagnostic Tool")
    print(f"{'='*90}")
    print(f"Origin: {ORIGIN}, Dest: {DEST}")
    print(f"Filters: path_type={PATH_TYPE}, sort_level={SORT_LEVEL}, dest_sort_level={DEST_SORT_LEVEL}")
    print()

    # Load data
    print("Loading inputs...")
    loader = InputLoader(INPUT_FILE)
    data = loader.load_all()
    facilities = data["facilities"]

    # Validate OD
    if ORIGIN not in facilities:
        print(f"ERROR: Origin facility '{ORIGIN}' not found")
        return 1
    if DEST not in facilities:
        print(f"ERROR: Destination facility '{DEST}' not found")
        return 1

    origin_fac = facilities[ORIGIN]
    dest_fac = facilities[DEST]

    print(f"Origin: {ORIGIN} ({origin_fac.facility_type.value}), TZ: {origin_fac.timezone}")
    print(f"Dest:   {DEST} ({dest_fac.facility_type.value}), TZ: {dest_fac.timezone}")

    direct_miles = haversine_miles(origin_fac.lat, origin_fac.lon, dest_fac.lat, dest_fac.lon)
    print(f"Direct distance: {direct_miles:.1f} miles")
    print()

    # Enumerate paths
    print("Enumerating paths...")
    enumerator = PathEnumerator(facilities, data["run_settings"])

    if ORIGIN == DEST:
        # O=D paths - create both DI and od_mm
        candidates = []

        # Direct injection (zone 0)
        di_path = PathCandidate(
            origin=ORIGIN,
            dest=DEST,
            path_nodes=[ORIGIN],
            path_type=PathType.DIRECT_INJECTION,
            sort_level=SortLevel.SORT_GROUP,
            dest_sort_level=SortLevel.SORT_GROUP,
            total_path_miles=0.0,
            direct_miles=0.0,
            atw_factor=1.0
        )
        candidates.append(di_path)

        # od_mm (zone 1+) - only if origin is hub/hybrid (injection capable)
        if origin_fac.facility_type in (FacilityType.HUB, FacilityType.HYBRID):
            odmm_path = PathCandidate(
                origin=ORIGIN,
                dest=DEST,
                path_nodes=[ORIGIN],
                path_type=PathType.OD_MM,
                sort_level=SortLevel.SORT_GROUP,
                dest_sort_level=SortLevel.SORT_GROUP,
                total_path_miles=0.0,
                direct_miles=0.0,
                atw_factor=1.0
            )
            candidates.append(odmm_path)
    else:
        candidates = enumerator.enumerate_paths_for_od(ORIGIN, DEST)

    print(f"Found {len(candidates)} path candidates")

    # Apply filters
    if PATH_TYPE:
        candidates = [c for c in candidates if c.path_type.value == PATH_TYPE]
        print(f"After path_type filter: {len(candidates)} paths")

    if SORT_LEVEL:
        candidates = [c for c in candidates if c.sort_level.value == SORT_LEVEL]
        print(f"After sort_level filter: {len(candidates)} paths")

    if DEST_SORT_LEVEL:
        candidates = [c for c in candidates if c.dest_sort_level.value == DEST_SORT_LEVEL]
        print(f"After dest_sort_level filter: {len(candidates)} paths")

    if not candidates:
        print("\nNo paths found matching filters.")
        return 0

    # Calculate timings
    print("\nCalculating timings...")
    cpt_generator = CPTGenerator(
        facilities=facilities,
        arc_cpts=data["arc_cpts"]
    )

    run_settings = data["run_settings"]
    engine = TimingEngine(
        facilities=facilities,
        mileage_bands=data["mileage_bands"],
        timing_params=data["timing_params"],
        cpt_generator=cpt_generator,
        reference_date=run_settings.reference_injection_date,
        reference_injection_time=run_settings.reference_injection_time
    )

    timings = []
    for path in candidates:
        try:
            timing = engine.calculate_path_timing(path)
            timings.append(timing)
        except Exception as e:
            print(f"  Warning: Failed to time path {path.path_nodes}: {e}")

    # Check SLA feasibility
    print("Checking SLA feasibility...")
    checker = FeasibilityChecker(data["service_commitments"])

    # Calculate zone for this OD pair
    zone = get_zone_for_distance(direct_miles, data["mileage_bands"])
    zone_num = zone.zone if zone else 2  # Default to zone 2 if not found

    # For DI paths, zone is always 0
    # For od_mm and networked paths, use calculated zone
    updated_timings = []
    for timing in timings:
        if timing.path.path_type == PathType.DIRECT_INJECTION:
            path_zone = 0
        else:
            path_zone = zone_num
        updated_timing = checker.check_feasibility(timing, path_zone)
        updated_timings.append(updated_timing)

    timings = updated_timings

    # Sort by TIT
    timings.sort(key=lambda t: (t.tit_hours, len(t.path.path_nodes), t.path.total_path_miles))

    # Print summary
    print()
    print(f"{'='*90}")
    print(f"=== {ORIGIN} → {DEST}: {len(timings)} paths found ===")
    print(f"{'='*90}")
    print(f"Reference injection: {run_settings.reference_injection_date.strftime('%Y-%m-%d')} "
          f"{run_settings.reference_injection_time.strftime('%H:%M')} (origin local)")

    print_summary_table(timings, facilities)

    # Determine which details to show
    detail_indices = []

    if SHOW_ALL_DETAILS:
        detail_indices = list(range(len(timings)))
    elif SHOW_DETAIL_FOR is not None:
        if 1 <= SHOW_DETAIL_FOR <= len(timings):
            detail_indices = [SHOW_DETAIL_FOR - 1]
        else:
            print(f"\nWarning: SHOW_DETAIL_FOR={SHOW_DETAIL_FOR} is out of range (1-{len(timings)})")
    elif SHOW_TOP_N_DETAILS > 0:
        detail_indices = list(range(min(SHOW_TOP_N_DETAILS, len(timings))))

    # Print detailed breakdowns
    for idx in detail_indices:
        print_detailed_breakdown(timings[idx], facilities, idx + 1)

    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())