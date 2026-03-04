#!/usr/bin/env python3
"""
SLA Path Model v1 - Main Entry Point

Run transit time feasibility analysis for parcel network optimization.

Usage:
    python scripts/run.py [--input INPUT_FILE] [--output OUTPUT_FILE]

Output naming:
    - If --output is specified, uses that path
    - Otherwise, derives name from scenario_id(s) in input file
"""
import argparse
import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sla_path_model.config import DEFAULT_INPUT_FILE, DEFAULT_OUTPUT_FILE, SortLevel, parse_enabled_sort_levels
from sla_path_model.io_loader import InputLoader
from sla_path_model.validators import validate_inputs
from sla_path_model.demand_builder import build_od_demand
from sla_path_model.path_enumeration import enumerate_all_paths
from sla_path_model.timing_engine import calculate_all_path_timings
from sla_path_model.feasibility import check_all_feasibility
from sla_path_model.reporting import build_all_reports
from sla_path_model.write_outputs import write_outputs
from sla_path_model.utils import setup_logging


def build_scenario_sort_levels(scenarios_df) -> dict[str, frozenset]:
    """Build mapping from scenario_id to its enabled sort levels."""
    result = {}
    for _, row in scenarios_df.iterrows():
        scenario_id = str(row["scenario_id"])
        raw = row.get("enabled_sort_levels") if "enabled_sort_levels" in scenarios_df.columns else None
        result[scenario_id] = parse_enabled_sort_levels(raw)
    return result


def derive_output_filename(scenarios_df, output_dir: str = "outputs") -> str:
    scenario_ids = scenarios_df["scenario_id"].astype(str).unique().tolist()

    if len(scenario_ids) == 1:
        filename = f"{scenario_ids[0]}.xlsx"
    else:
        if len(scenario_ids) <= 3:
            combined = "_".join(scenario_ids)
        else:
            combined = f"{scenario_ids[0]}_{scenario_ids[1]}_and_{len(scenario_ids)-2}_more"
        filename = f"{combined}.xlsx"

    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')

    return str(Path(output_dir) / filename)


def main():
    parser = argparse.ArgumentParser(
        description="SLA Path Model v1 - Transit time feasibility analysis"
    )
    parser.add_argument(
        "--input", "-i",
        default=DEFAULT_INPUT_FILE,
        help=f"Input Excel file (default: {DEFAULT_INPUT_FILE})"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output Excel file (default: derived from scenario_id)"
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory when deriving filename from scenario_id (default: outputs)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    import logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(log_level)

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("SLA Path Model v1 - Starting")
    logger.info("=" * 60)

    try:
        logger.info("Step 1: Loading inputs...")
        loader = InputLoader(args.input)
        data = loader.load_all()

        if args.output:
            output_path = args.output
        else:
            output_path = derive_output_filename(data["scenarios"], args.output_dir)

        logger.info(f"Output will be written to: {output_path}")

        logger.info("Step 2: Validating inputs...")
        validate_inputs(data)

        logger.info("Step 3: Building OD demand...")
        od_demands = build_od_demand(data)

        # Build per-scenario sort level config
        scenario_sort_levels = build_scenario_sort_levels(data["scenarios"])
        global_enabled_levels = frozenset().union(*scenario_sort_levels.values())
        logger.info(f"Enabled sort levels (global union): {sorted(sl.value for sl in global_enabled_levels)}")

        logger.info("Step 4: Enumerating paths...")
        od_paths = enumerate_all_paths(data, od_demands, enabled_sort_levels=global_enabled_levels)

        logger.info("Step 5: Calculating path timings...")
        od_timings = calculate_all_path_timings(data, od_paths)

        logger.info("Step 6: Checking SLA feasibility...")
        od_timings = check_all_feasibility(
            od_timings,
            od_demands,
            data["service_commitments"]
        )

        logger.info("Step 7: Building reports...")
        run_settings = data["run_settings"]
        reports = build_all_reports(
            od_demands,
            od_timings,
            top_paths_per_sort_level=run_settings.top_paths_per_sort_level,
            scenario_sort_levels=scenario_sort_levels
        )

        logger.info("Step 8: Writing outputs...")
        write_outputs(reports, output_path)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"SLA Path Model v1 - Complete ({elapsed:.1f}s)")
        logger.info(f"Output written to: {output_path}")
        logger.info("=" * 60)

        if "summary" in reports:
            summary = reports["summary"]
            for _, row in summary.iterrows():
                logger.info(f"  Scenario {row['scenario_id']}:")
                logger.info(f"    Total packages: {row['total_packages']:,.0f}")
                logger.info(f"    Volume at SLA: {row['pct_volume_at_sla']*100:.1f}%")
                logger.info(f"    Avg TIT: {row['avg_tit_hours']:.1f} hours")

        return 0

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())