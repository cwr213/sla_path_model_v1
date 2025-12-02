#!/usr/bin/env python3
"""
SLA Path Model v1 - Main Entry Point

Run transit time feasibility analysis for parcel network optimization.

Usage:
    python scripts/run_model.py [--input INPUT_FILE] [--output OUTPUT_FILE]
"""
import argparse
import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sla_path.config import DEFAULT_INPUT_FILE, DEFAULT_OUTPUT_FILE
from sla_path.io_loader import InputLoader
from sla_path.validators import validate_inputs
from sla_path.demand_builder import build_od_demand
from sla_path.path_enumeration import enumerate_all_paths
from sla_path.timing_engine import calculate_all_path_timings
from sla_path.feasibility import check_all_feasibility
from sla_path.reporting import build_all_reports
from sla_path.write_outputs import write_outputs
from sla_path.utils import setup_logging


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
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output Excel file (default: {DEFAULT_OUTPUT_FILE})"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    import logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(log_level)

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("SLA Path Model v1 - Starting")
    logger.info("=" * 60)

    try:
        # Step 1: Load inputs
        logger.info("Step 1: Loading inputs...")
        loader = InputLoader(args.input)
        data = loader.load_all()

        # Step 2: Validate inputs
        logger.info("Step 2: Validating inputs...")
        validate_inputs(data)

        # Step 3: Build OD demand
        logger.info("Step 3: Building OD demand...")
        od_demands = build_od_demand(data)

        # Step 4: Enumerate paths
        logger.info("Step 4: Enumerating paths...")
        od_paths = enumerate_all_paths(data, od_demands)

        # Step 5: Calculate path timings
        logger.info("Step 5: Calculating path timings...")
        od_timings = calculate_all_path_timings(data, od_paths)

        # Step 6: Check feasibility
        logger.info("Step 6: Checking SLA feasibility...")
        od_timings = check_all_feasibility(
            od_timings,
            od_demands,
            data["service_commitments"]
        )

        # Step 7: Build reports
        logger.info("Step 7: Building reports...")
        reports = build_all_reports(od_demands, od_timings)

        # Step 8: Write outputs
        logger.info("Step 8: Writing outputs...")
        write_outputs(reports, args.output)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"SLA Path Model v1 - Complete ({elapsed:.1f}s)")
        logger.info(f"Output written to: {args.output}")
        logger.info("=" * 60)

        # Print summary
        if "summary" in reports:
            summary = reports["summary"]
            for _, row in summary.iterrows():
                logger.info(f"  Scenario {row['scenario_id']}:")
                logger.info(f"    Total packages: {row['total_packages']:,.0f}")
                logger.info(f"    Volume at SLA: {row['pct_volume_at_sla']:.1f}%")
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