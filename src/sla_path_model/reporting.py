"""
Reporting: build output dataframes from model results.
"""
from collections import defaultdict
from typing import Optional

import pandas as pd

from .config import (
    PathTimingResult, ODDemand, FlowType
)
from .utils import format_path_nodes, setup_logging

logger = setup_logging()


class ReportBuilder:
    """Build output reports from model results."""

    def __init__(
            self,
            od_demands: list[ODDemand],
            od_timings: dict[tuple[str, str], list[PathTimingResult]]
    ):
        self.od_demands = od_demands
        self.od_timings = od_timings

        # Build demand lookup
        self.demand_lookup = {}
        for demand in od_demands:
            key = (demand.scenario_id, demand.origin, demand.dest)
            self.demand_lookup[key] = demand

    def build_od_demand_df(self) -> pd.DataFrame:
        """
        Build od_demand output sheet.

        Columns: scenario_id, origin, dest, pkgs_day, zone, flow_type, day_type
        """
        rows = []
        for demand in self.od_demands:
            rows.append({
                "scenario_id": demand.scenario_id,
                "origin": demand.origin,
                "dest": demand.dest,
                "pkgs_day": demand.pkgs_day,
                "zone": demand.zone,
                "flow_type": demand.flow_type.value,
                "day_type": demand.day_type
            })

        df = pd.DataFrame(rows)
        logger.info(f"Built od_demand with {len(df)} rows")
        return df

    def build_feasible_paths_df(self) -> pd.DataFrame:
        """
        Build feasible_paths output sheet.

        Outputs ALL paths per OD × sort_level for sort model to select from.
        """
        rows = []

        # Get unique scenarios from demands
        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            # Get demands for this scenario
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            for demand in scenario_demands:
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    # Direct injection - no path needed, but include a record
                    rows.append({
                        "scenario_id": scenario_id,
                        "origin": demand.origin,
                        "dest": demand.dest,
                        "path_nodes": demand.dest,  # Single node
                        "path_type": "direct_injection",
                        "sort_level": "n/a",
                        "total_path_miles": 0,
                        "direct_miles": 0,
                        "atw_factor": 1.0,
                        "tit_hours": 0,
                        "sort_window_dwell_hours": 0,
                        "cpt_dwell_hours": 0,
                        "total_dwell_hours": 0,
                        "sla_days": 0,
                        "sla_buffer_days": 0,
                        "sla_target_hours": 0,
                        "sla_met": True,
                        "sla_slack_hours": 0,
                        "required_injection_utc": None,
                        "delivery_datetime_utc": None,
                        "priority_weight": 1.0,
                        "pkgs_day": demand.pkgs_day,
                        "zone": demand.zone
                    })
                    continue

                # Middle mile - get path timings
                key = (demand.origin, demand.dest)
                timings = self.od_timings.get(key, [])

                if not timings:
                    logger.warning(f"No paths found for OD {demand.origin}->{demand.dest}")
                    continue

                for timing in timings:
                    rows.append({
                        "scenario_id": scenario_id,
                        "origin": timing.path.origin,
                        "dest": timing.path.dest,
                        "path_nodes": format_path_nodes(timing.path.path_nodes),
                        "path_type": timing.path.path_type.value,
                        "sort_level": timing.path.sort_level.value,
                        "total_path_miles": round(timing.path.total_path_miles, 1),
                        "direct_miles": round(timing.path.direct_miles, 1),
                        "atw_factor": round(timing.path.atw_factor, 3),
                        "tit_hours": round(timing.tit_hours, 2),
                        "sort_window_dwell_hours": round(timing.sort_window_dwell_hours, 2),
                        "cpt_dwell_hours": round(timing.cpt_dwell_hours, 2),
                        "total_dwell_hours": round(timing.total_dwell_hours, 2),
                        "sla_days": timing.sla_days,
                        "sla_buffer_days": timing.sla_buffer_days,
                        "sla_target_hours": round(timing.sla_target_hours, 2),
                        "sla_met": timing.sla_met,
                        "sla_slack_hours": round(timing.sla_slack_hours, 2),
                        "required_injection_utc": timing.required_injection_utc.isoformat() if timing.required_injection_utc else None,
                        "delivery_datetime_utc": timing.delivery_datetime_utc.isoformat() if timing.delivery_datetime_utc else None,
                        "priority_weight": timing.priority_weight,
                        "pkgs_day": demand.pkgs_day,
                        "zone": demand.zone
                    })

        df = pd.DataFrame(rows)
        logger.info(f"Built feasible_paths with {len(df)} rows")
        return df

    def build_path_timing_detail_df(self) -> pd.DataFrame:
        """
        Build path_timing_detail output sheet for analysis/mapping.

        One row per step in each path.
        """
        rows = []

        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            for demand in scenario_demands:
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    continue

                key = (demand.origin, demand.dest)
                timings = self.od_timings.get(key, [])

                for timing in timings:
                    path_str = format_path_nodes(timing.path.path_nodes)

                    for step in timing.steps:
                        rows.append({
                            "scenario_id": scenario_id,
                            "origin": timing.path.origin,
                            "dest": timing.path.dest,
                            "path_nodes": path_str,
                            "sort_level": timing.path.sort_level.value,
                            "step_sequence": step.step_sequence,
                            "step_type": step.step_type.value,
                            "from_facility": step.from_facility,
                            "to_facility": step.to_facility,
                            "from_lat": step.from_lat,
                            "from_lon": step.from_lon,
                            "to_lat": step.to_lat,
                            "to_lon": step.to_lon,
                            "distance_miles": round(step.distance_miles, 1) if step.distance_miles else 0,
                            "start_utc": step.start_utc.isoformat() if step.start_utc else None,
                            "end_utc": step.end_utc.isoformat() if step.end_utc else None,
                            "duration_minutes": round(step.duration_minutes, 1),
                            "sort_window_dwell_minutes": round(step.sort_window_dwell_minutes, 1),
                            "cpt_dwell_minutes": round(step.cpt_dwell_minutes, 1),
                            "total_dwell_minutes": round(step.total_dwell_minutes, 1)
                        })

        df = pd.DataFrame(rows)
        logger.info(f"Built path_timing_detail with {len(df)} rows")
        return df

    def build_summary_df(self) -> pd.DataFrame:
        """
        Build summary output sheet.

        One row per scenario with aggregate metrics.
        """
        rows = []

        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            total_od_pairs = len(scenario_demands)
            total_packages = sum(d.pkgs_day for d in scenario_demands)

            # Count paths evaluated and feasible
            paths_evaluated = 0
            paths_feasible = 0
            volume_at_sla = 0
            volume_within_buffer = 0
            volume_missed = 0
            tit_sum = 0
            slack_sum = 0
            tit_count = 0

            for demand in scenario_demands:
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    # Direct injection always meets SLA
                    volume_at_sla += demand.pkgs_day
                    continue

                key = (demand.origin, demand.dest)
                timings = self.od_timings.get(key, [])
                paths_evaluated += len(timings)

                # Find best path for this OD
                best_timing = None
                for timing in timings:
                    if timing.sla_met:
                        paths_feasible += 1
                        if best_timing is None or timing.tit_hours < best_timing.tit_hours:
                            best_timing = timing

                if best_timing and best_timing.sla_met:
                    volume_at_sla += demand.pkgs_day
                    tit_sum += best_timing.tit_hours
                    slack_sum += best_timing.sla_slack_hours
                    tit_count += 1
                elif timings:
                    # Check if within buffer
                    best_any = min(timings, key=lambda t: t.tit_hours)
                    if best_any.sla_slack_hours >= -best_any.sla_buffer_days * 24:
                        volume_within_buffer += demand.pkgs_day
                    else:
                        volume_missed += demand.pkgs_day
                    tit_sum += best_any.tit_hours
                    slack_sum += best_any.sla_slack_hours
                    tit_count += 1

            rows.append({
                "scenario_id": scenario_id,
                "total_od_pairs": total_od_pairs,
                "total_packages": round(total_packages, 0),
                "paths_evaluated": paths_evaluated,
                "paths_feasible": paths_feasible,
                "pct_volume_at_sla": round(100 * volume_at_sla / total_packages, 1) if total_packages > 0 else 0,
                "pct_volume_within_buffer": round(100 * volume_within_buffer / total_packages,
                                                  1) if total_packages > 0 else 0,
                "pct_volume_missed": round(100 * volume_missed / total_packages, 1) if total_packages > 0 else 0,
                "avg_tit_hours": round(tit_sum / tit_count, 2) if tit_count > 0 else 0,
                "avg_slack_hours": round(slack_sum / tit_count, 2) if tit_count > 0 else 0
            })

        df = pd.DataFrame(rows)
        logger.info(f"Built summary with {len(df)} rows")
        return df

    def build_sla_miss_detail_df(self) -> pd.DataFrame:
        """
        Build sla_miss_detail output sheet.

        One row per OD pair that missed SLA (best path still doesn't meet it).
        """
        rows = []

        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            for demand in scenario_demands:
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    continue

                key = (demand.origin, demand.dest)
                timings = self.od_timings.get(key, [])

                if not timings:
                    continue

                # Find best path (lowest TIT)
                best = min(timings, key=lambda t: t.tit_hours)

                if not best.sla_met:
                    # Determine bottleneck (simplified - identify largest dwell component)
                    if best.cpt_dwell_hours > best.sort_window_dwell_hours:
                        bottleneck = "CPT availability"
                    elif best.sort_window_dwell_hours > 0:
                        bottleneck = "Sort window"
                    else:
                        bottleneck = "Transit time"

                    rows.append({
                        "scenario_id": scenario_id,
                        "origin": demand.origin,
                        "dest": demand.dest,
                        "zone": demand.zone,
                        "pkgs_day": round(demand.pkgs_day, 0),
                        "sla_days": best.sla_days,
                        "best_tit_hours": round(best.tit_hours, 2),
                        "miss_hours": round(-best.sla_slack_hours, 2),
                        "bottleneck": bottleneck
                    })

        df = pd.DataFrame(rows)
        logger.info(f"Built sla_miss_detail with {len(df)} rows")
        return df


def build_all_reports(
        od_demands: list[ODDemand],
        od_timings: dict[tuple[str, str], list[PathTimingResult]]
) -> dict[str, pd.DataFrame]:
    """
    Build all output reports.

    Args:
        od_demands: List of ODDemand objects
        od_timings: Dictionary mapping (origin, dest) to list of PathTimingResult

    Returns:
        Dictionary of sheet_name -> DataFrame
    """
    builder = ReportBuilder(od_demands, od_timings)

    return {
        "od_demand": builder.build_od_demand_df(),
        "feasible_paths": builder.build_feasible_paths_df(),
        "path_timing_detail": builder.build_path_timing_detail_df(),
        "summary": builder.build_summary_df(),
        "sla_miss_detail": builder.build_sla_miss_detail_df()
    }