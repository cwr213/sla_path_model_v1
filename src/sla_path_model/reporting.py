"""Report generation for SLA path model outputs."""
from collections import defaultdict

import pandas as pd

from .config import PathTimingResult, ODDemand, FlowType, SortLevel, StepType
from .utils import setup_logging

logger = setup_logging()


def _path_ranking_key(timing: PathTimingResult) -> tuple:
    """
    Ranking key for selecting optimal path.
    Priority: shortest TIT, fewest touches, shortest miles.
    """
    num_touches = len(timing.path.path_nodes) - 1
    return (timing.tit_hours, num_touches, timing.path.total_path_miles)


def filter_top_paths_per_sort_level(
        od_timings: dict[tuple[str, str], list[PathTimingResult]],
        top_n: int
) -> dict[tuple[str, str], list[PathTimingResult]]:
    """
    Filter to top N paths per OD pair × sort_level.

    For each OD pair, groups paths by sort_level, ranks within each group,
    and keeps only the top N per sort_level.

    Args:
        od_timings: Dictionary mapping (origin, dest) to list of PathTimingResult
        top_n: Number of top paths to keep per sort_level

    Returns:
        Filtered od_timings with at most top_n paths per sort_level per OD
    """
    filtered = {}
    total_before = 0
    total_after = 0

    for (origin, dest), timings in od_timings.items():
        total_before += len(timings)

        # Group by sort_level
        by_sort_level = defaultdict(list)
        for timing in timings:
            by_sort_level[timing.path.sort_level].append(timing)

        # Keep top N per sort_level
        kept = []
        for sort_level, level_timings in by_sort_level.items():
            # Sort by ranking key and take top N
            level_timings.sort(key=_path_ranking_key)
            kept.extend(level_timings[:top_n])

        filtered[(origin, dest)] = kept
        total_after += len(kept)

    logger.info(
        f"Filtered paths: {total_before} -> {total_after} "
        f"(top {top_n} per OD × sort_level)"
    )

    return filtered


class ReportBuilder:

    def __init__(
            self,
            od_demands: list[ODDemand],
            od_timings: dict[tuple[str, str], list[PathTimingResult]]
    ):
        self.od_demands = od_demands
        self.od_timings = od_timings

    def build_od_demand_df(self) -> pd.DataFrame:
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
        Build feasible_paths sheet with consolidated paths and demand breakdown.

        Each unique path appears once per scenario with demand split by flow type:
        - pkgs_mm: Middle mile demand
        - pkgs_zs: Zone skip demand
        - pkgs_di: Direct injection demand
        - pkgs_day: Total (sum of above)

        Zone columns:
        - zone_mm_zs: Zone for middle mile / zone skip (mileage band based)
        - zone_di: Zone for direct injection (always 0)

        TIT breakdown columns:
        - tit_sort_hours: Induction + full sort + last mile sort time
        - tit_crossdock_hours: Crossdock processing time
        - tit_transit_hours: Linehaul transit time
        - tit_dwell_hours: CPT dwell + sort window dwell time
        """
        rows = []

        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            # Aggregate demand by (origin, dest, flow_type)
            demand_by_od = defaultdict(lambda: {
                FlowType.MIDDLE_MILE: 0.0,
                FlowType.ZONE_SKIP: 0.0,
                FlowType.DIRECT_INJECTION: 0.0,
                'zone_mm_zs': None,
                'zone_di': None
            })

            for demand in scenario_demands:
                key = (demand.origin, demand.dest)
                demand_by_od[key][demand.flow_type] += demand.pkgs_day

                # Track zones by flow type
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    demand_by_od[key]['zone_di'] = demand.zone  # Should be 0
                else:
                    demand_by_od[key]['zone_mm_zs'] = demand.zone

            # Build path rows with consolidated demand
            for (origin, dest), demand_info in demand_by_od.items():
                pkgs_mm = demand_info[FlowType.MIDDLE_MILE]
                pkgs_zs = demand_info[FlowType.ZONE_SKIP]
                pkgs_di = demand_info[FlowType.DIRECT_INJECTION]
                pkgs_day = pkgs_mm + pkgs_zs + pkgs_di
                zone_mm_zs = demand_info['zone_mm_zs']
                zone_di = demand_info['zone_di']

                # Get path timings for this OD
                timings = self.od_timings.get((origin, dest), [])

                if not timings:
                    # No paths found - only output if there's DI demand (O=D with no network path)
                    if pkgs_di > 0:
                        rows.append({
                            "scenario_id": scenario_id,
                            "origin": origin,
                            "dest": dest,
                            "node_1": dest,
                            "node_2": None,
                            "node_3": None,
                            "node_4": None,
                            "node_5": None,
                            "path_type": "direct_injection",
                            "sort_level": "n/a",
                            "dest_sort_level": "n/a",
                            "total_path_miles": 0,
                            "direct_miles": 0,
                            "atw_factor": 1.0,
                            "tit_hours": 0,
                            "tit_sort_hours": 0,
                            "tit_crossdock_hours": 0,
                            "tit_transit_hours": 0,
                            "tit_dwell_hours": 0,
                            "sla_days": 0,
                            "sla_target_hours": 0,
                            "sla_met": True,
                            "sla_slack_hours": 0,
                            "uses_only_active_arcs": True,
                            "pkgs_day": pkgs_di,
                            "pkgs_mm": 0,
                            "pkgs_zs": 0,
                            "pkgs_di": pkgs_di,
                            "zone_mm_zs": None,
                            "zone_di": zone_di
                        })
                    continue

                # Output each unique path once with consolidated demand
                for timing in timings:
                    nodes = timing.path.path_nodes

                    # Calculate TIT breakdown from steps
                    tit_sort_mins = 0.0
                    tit_crossdock_mins = 0.0
                    tit_transit_mins = 0.0

                    for step in timing.steps:
                        if step.step_type in (StepType.INDUCTION_SORT, StepType.FULL_SORT, StepType.LAST_MILE_SORT):
                            tit_sort_mins += step.duration_minutes
                        elif step.step_type == StepType.CROSSDOCK:
                            tit_crossdock_mins += step.duration_minutes
                        elif step.step_type == StepType.TRANSIT:
                            tit_transit_mins += step.duration_minutes

                    rows.append({
                        "scenario_id": scenario_id,
                        "origin": timing.path.origin,
                        "dest": timing.path.dest,
                        "node_1": nodes[0] if len(nodes) > 0 else None,
                        "node_2": nodes[1] if len(nodes) > 1 else None,
                        "node_3": nodes[2] if len(nodes) > 2 else None,
                        "node_4": nodes[3] if len(nodes) > 3 else None,
                        "node_5": nodes[4] if len(nodes) > 4 else None,
                        "path_type": timing.path.path_type.value,
                        "sort_level": timing.path.sort_level.value,
                        "dest_sort_level": timing.path.dest_sort_level.value,
                        "total_path_miles": round(timing.path.total_path_miles, 1),
                        "direct_miles": round(timing.path.direct_miles, 1),
                        "atw_factor": round(timing.path.atw_factor, 3),
                        "tit_hours": round(timing.tit_hours, 2),
                        "tit_sort_hours": round(tit_sort_mins / 60, 2),
                        "tit_crossdock_hours": round(tit_crossdock_mins / 60, 2),
                        "tit_transit_hours": round(tit_transit_mins / 60, 2),
                        "tit_dwell_hours": round(timing.total_dwell_hours, 2),
                        "sla_days": timing.sla_days,
                        "sla_target_hours": round(timing.sla_target_hours, 2),
                        "sla_met": timing.sla_met,
                        "sla_slack_hours": round(timing.sla_slack_hours, 2),
                        "uses_only_active_arcs": timing.uses_only_active_arcs,
                        "pkgs_day": pkgs_day,
                        "pkgs_mm": pkgs_mm,
                        "pkgs_zs": pkgs_zs,
                        "pkgs_di": pkgs_di,
                        "zone_mm_zs": zone_mm_zs,
                        "zone_di": zone_di
                    })

        df = pd.DataFrame(rows)
        logger.info(f"Built feasible_paths with {len(df)} rows")
        return df

    def build_summary_df(self) -> pd.DataFrame:
        rows = []

        scenarios = set(d.scenario_id for d in self.od_demands)

        for scenario_id in scenarios:
            scenario_demands = [d for d in self.od_demands if d.scenario_id == scenario_id]

            total_od_pairs = len(scenario_demands)
            total_packages = sum(d.pkgs_day for d in scenario_demands)

            paths_evaluated = 0
            paths_feasible = 0
            volume_at_sla = 0
            volume_missed = 0
            tit_sum = 0
            tit_count = 0

            for demand in scenario_demands:
                if demand.flow_type == FlowType.DIRECT_INJECTION:
                    volume_at_sla += demand.pkgs_day
                    continue

                key = (demand.origin, demand.dest)
                timings = self.od_timings.get(key, [])
                paths_evaluated += len(timings)

                feasible_for_od = [t for t in timings if t.sla_met]
                paths_feasible += len(feasible_for_od)

                if feasible_for_od:
                    volume_at_sla += demand.pkgs_day
                    best = min(feasible_for_od, key=_path_ranking_key)
                    tit_sum += best.tit_hours
                    tit_count += 1
                elif timings:
                    volume_missed += demand.pkgs_day
                    best = min(timings, key=_path_ranking_key)
                    tit_sum += best.tit_hours
                    tit_count += 1

            rows.append({
                "scenario_id": scenario_id,
                "total_od_pairs": total_od_pairs,
                "total_packages": round(total_packages, 0),
                "paths_evaluated": paths_evaluated,
                "paths_feasible": paths_feasible,
                "pct_volume_at_sla": round(volume_at_sla / total_packages, 4) if total_packages > 0 else 0,
                "pct_volume_missed": round(volume_missed / total_packages, 4) if total_packages > 0 else 0,
                "avg_tit_hours": round(tit_sum / tit_count, 2) if tit_count > 0 else 0
            })

        df = pd.DataFrame(rows)
        logger.info(f"Built summary with {len(df)} rows")
        return df

    def build_sla_miss_detail_df(self) -> pd.DataFrame:
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

                best = min(timings, key=_path_ranking_key)

                if not best.sla_met:
                    rows.append({
                        "scenario_id": scenario_id,
                        "origin": demand.origin,
                        "dest": demand.dest,
                        "zone": demand.zone,
                        "pkgs_day": round(demand.pkgs_day, 0),
                        "sla_days": best.sla_days,
                        "best_tit_hours": round(best.tit_hours, 2),
                        "miss_hours": round(-best.sla_slack_hours, 2)
                    })

        df = pd.DataFrame(rows)
        logger.info(f"Built sla_miss_detail with {len(df)} rows")
        return df


def build_all_reports(
        od_demands: list[ODDemand],
        od_timings: dict[tuple[str, str], list[PathTimingResult]],
        top_paths_per_sort_level: int = None
) -> dict[str, pd.DataFrame]:
    """
    Build all report DataFrames.

    Args:
        od_demands: List of OD demand records
        od_timings: Dictionary of path timing results
        top_paths_per_sort_level: If provided, filter to top N per OD × sort_level

    Returns:
        Dictionary of report DataFrames
    """
    # Apply top N filtering if specified
    if top_paths_per_sort_level is not None and top_paths_per_sort_level > 0:
        od_timings = filter_top_paths_per_sort_level(od_timings, top_paths_per_sort_level)

    builder = ReportBuilder(od_demands, od_timings)

    return {
        "summary": builder.build_summary_df(),
        "od_demand": builder.build_od_demand_df(),
        "feasible_paths": builder.build_feasible_paths_df(),
        "sla_miss_detail": builder.build_sla_miss_detail_df()
    }