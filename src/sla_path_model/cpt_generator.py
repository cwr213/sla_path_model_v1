"""
CPT Generator: Generate Critical Pull Times (departure schedules) for network arcs.

CPTs can come from two sources:
1. Explicit arc_cpts sheet (overrides for specific origin-dest pairs)
2. Generated from facility outbound windows (default)

Generation logic:
- For each facility with an outbound window and cpt_count, evenly space CPTs
- CPTs represent trailer departure times from origin to destination
"""

from datetime import datetime, timedelta
from typing import Optional

from .config import Facility, FacilityType, CPT, MINUTES_PER_DAY
from .time_utils import time_to_minutes, minutes_to_time, local_to_utc, utc_to_local
from .utils import setup_logging

logger = setup_logging()


class CPTGenerator:

    def __init__(self, facilities: dict[str, Facility], arc_cpts: list[CPT]):
        self.facilities = facilities
        self.arc_cpts = arc_cpts

        self._explicit_cpts: dict[tuple[str, str], list[CPT]] = {}
        for cpt in arc_cpts:
            key = (cpt.origin, cpt.dest)
            if key not in self._explicit_cpts:
                self._explicit_cpts[key] = []
            self._explicit_cpts[key].append(cpt)

        for key in self._explicit_cpts:
            self._explicit_cpts[key].sort(key=lambda c: c.cpt_sequence)

        self._generated_cpts: dict[tuple[str, str], list[CPT]] = {}
        self._generate_default_cpts()

        logger.info(
            f"CPT Generator initialized: {len(self._explicit_cpts)} explicit arcs, "
            f"{len(self._generated_cpts)} generated facility schedules"
        )

    def _generate_default_cpts(self):
        for name, fac in self.facilities.items():
            if fac.facility_type not in (FacilityType.HUB, FacilityType.HYBRID):
                continue

            if (fac.outbound_window_start_local is None or
                    fac.outbound_window_end_local is None or
                    fac.outbound_cpt_count is None or
                    fac.outbound_cpt_count < 1):
                continue

            cpts = self._generate_facility_cpts(fac)
            self._generated_cpts[(name, "*")] = cpts

    def _generate_facility_cpts(self, facility: Facility) -> list[CPT]:
        start_mins = time_to_minutes(facility.outbound_window_start_local)
        end_mins = time_to_minutes(facility.outbound_window_end_local)

        if end_mins <= start_mins:
            window_duration = (MINUTES_PER_DAY - start_mins) + end_mins
        else:
            window_duration = end_mins - start_mins

        cpt_count = facility.outbound_cpt_count

        if cpt_count == 1:
            cpt_times = [end_mins]
        else:
            interval = window_duration / (cpt_count - 1) if cpt_count > 1 else 0
            cpt_times = []
            for i in range(cpt_count):
                cpt_mins = start_mins + (i * interval)
                if cpt_mins >= MINUTES_PER_DAY:
                    cpt_mins -= MINUTES_PER_DAY
                cpt_times.append(cpt_mins)

        cpts = []
        for seq, cpt_mins in enumerate(cpt_times, start=1):
            cpt = CPT(
                origin=facility.name,
                dest="*",
                cpt_sequence=seq,
                cpt_local=minutes_to_time(int(cpt_mins)),
                days_of_week=[],
                timezone=facility.timezone
            )
            cpts.append(cpt)

        return cpts

    def get_cpts_for_arc(self, origin: str, dest: str) -> list[CPT]:
        if (origin, dest) in self._explicit_cpts:
            return self._explicit_cpts[(origin, dest)]

        if (origin, "*") in self._generated_cpts:
            facility_cpts = self._generated_cpts[(origin, "*")]
            return [
                CPT(
                    origin=cpt.origin,
                    dest=dest,
                    cpt_sequence=cpt.cpt_sequence,
                    cpt_local=cpt.cpt_local,
                    days_of_week=cpt.days_of_week,
                    timezone=cpt.timezone
                )
                for cpt in facility_cpts
            ]

        logger.debug(f"No CPTs defined for arc {origin}->{dest}")
        return []

    def get_latest_cpt_before(
            self,
            origin: str,
            dest: str,
            before_utc: datetime,
            reference_date: datetime
    ) -> Optional[tuple[datetime, CPT]]:
        cpts = self.get_cpts_for_arc(origin, dest)
        if not cpts:
            return None

        origin_fac = self.facilities.get(origin)
        if not origin_fac:
            return None

        before_local = utc_to_local(before_utc, origin_fac.timezone)
        before_date = before_local.date()

        candidates = []
        for day_offset in range(7):
            check_date = before_date - timedelta(days=day_offset)

            for cpt in cpts:
                if cpt.days_of_week:
                    day_name = check_date.strftime("%a")
                    if day_name not in cpt.days_of_week:
                        continue

                cpt_local = datetime.combine(check_date, cpt.cpt_local)
                cpt_utc = local_to_utc(cpt_local, cpt.timezone)

                if cpt_utc <= before_utc:
                    candidates.append((cpt_utc, cpt))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates[0]

        return None


def get_cpts_for_path(
        path_nodes: list[str],
        cpt_generator: CPTGenerator
) -> dict[tuple[str, str], list[CPT]]:
    arc_cpts = {}

    for i in range(len(path_nodes) - 1):
        from_node = path_nodes[i]
        to_node = path_nodes[i + 1]
        cpts = cpt_generator.get_cpts_for_arc(from_node, to_node)
        arc_cpts[(from_node, to_node)] = cpts

    return arc_cpts