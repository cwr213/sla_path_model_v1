"""
Timing Engine: calculate time-in-transit using forward-chaining from fixed injection time.

Algorithm:
1. Start from fixed injection time (reference_injection_date + reference_injection_time)
2. Work forward through each path segment:
   - Add induction sort time at origin
   - Align to next available CPT (add dwell if waiting)
   - Add transit time
   - Add intermediate processing (crossdock or full sort)
   - Repeat for each hop
   - Add LM sort time at destination (if dest_sort_level = MARKET)
3. Result: arrival time and total TIT from fixed injection
4. sla_slack_hours = SLA target - TIT (positive = meets SLA, negative = misses)
"""
from datetime import datetime, timedelta, time
from typing import Optional

from .config import (
    Facility, FacilityType, PathCandidate, PathStep, PathTimingResult,
    SortLevel, StepType, TimingParams, MileageBand, CPT,
    MINUTES_PER_HOUR
)
from .cpt_generator import CPTGenerator, get_cpts_for_path
from .geo import haversine_miles, get_zone_for_distance, calculate_transit_time_minutes
from .time_utils import local_to_utc, utc_to_local, align_to_window_start
from .utils import setup_logging

logger = setup_logging()


class TimingEngine:

    def __init__(
            self,
            facilities: dict[str, Facility],
            mileage_bands: list[MileageBand],
            timing_params: TimingParams,
            cpt_generator: CPTGenerator,
            reference_date: datetime,
            reference_injection_time: time = None
    ):
        self.facilities = facilities
        self.mileage_bands = sorted(mileage_bands, key=lambda b: b.zone)
        self.timing_params = timing_params
        self.cpt_generator = cpt_generator
        self.reference_date = reference_date
        self.reference_injection_time = reference_injection_time or time(18, 0)

    def calculate_path_timing(self, path: PathCandidate) -> PathTimingResult:
        """
        Calculate TIT using forward-chaining from fixed injection time.
        """
        dest_fac = self.facilities[path.dest]
        origin_fac = self.facilities[path.origin]

        # Fixed injection time in origin's local timezone, converted to UTC
        injection_local = datetime.combine(
            self.reference_date.date(),
            self.reference_injection_time
        )
        injection_utc = local_to_utc(injection_local, origin_fac.timezone)

        steps = []
        current_time_utc = injection_utc
        total_sort_window_dwell = 0.0
        total_cpt_dwell = 0.0
        all_arcs_active = True  # Track if all arcs use active CPTs

        is_od_equal = (path.origin == path.dest)

        # Step 1: Induction sort at origin (MM sort)
        mm_window = origin_fac.get_mm_sort_window()

        if mm_window:
            induction_start_utc, window_dwell = align_to_window_start(
                current_time_utc,
                mm_window,
                self.timing_params.induction_sort_minutes
            )
        else:
            induction_start_utc = current_time_utc
            window_dwell = 0

        induction_end_utc = induction_start_utc + timedelta(
            minutes=self.timing_params.induction_sort_minutes
        )

        induction_step = PathStep(
            step_sequence=1,
            step_type=StepType.INDUCTION_SORT,
            from_facility=path.origin,
            to_facility=path.origin,
            from_lat=origin_fac.lat,
            from_lon=origin_fac.lon,
            to_lat=origin_fac.lat,
            to_lon=origin_fac.lon,
            distance_miles=0,
            start_utc=induction_start_utc,
            end_utc=induction_end_utc,
            duration_minutes=self.timing_params.induction_sort_minutes,
            sort_window_dwell_minutes=window_dwell,
            cpt_dwell_minutes=0,
            total_dwell_minutes=window_dwell
        )
        steps.append(induction_step)
        total_sort_window_dwell += window_dwell
        current_time_utc = induction_end_utc

        # Step 2: Transit legs (skip for O=D)
        if not is_od_equal:
            for i in range(len(path.path_nodes) - 1):
                from_node = path.path_nodes[i]
                to_node = path.path_nodes[i + 1]

                from_fac = self.facilities[from_node]
                to_fac = self.facilities[to_node]

                # Find next CPT after current time
                cpts = self.cpt_generator.get_cpts_for_arc(from_node, to_node)
                cpt_departure_utc, cpt_dwell, arc_is_active = self._find_next_cpt(
                    current_time_utc, cpts, from_fac, from_node, to_node
                )

                # Track if any arc uses inactive CPT
                if not arc_is_active:
                    all_arcs_active = False

                # Calculate transit
                distance = haversine_miles(from_fac.lat, from_fac.lon, to_fac.lat, to_fac.lon)
                band = get_zone_for_distance(distance, self.mileage_bands)

                if band:
                    transit_minutes = calculate_transit_time_minutes(
                        distance, band.circuity_factor, band.mph
                    )
                else:
                    transit_minutes = distance / 50 * MINUTES_PER_HOUR

                arrival_utc = cpt_departure_utc + timedelta(minutes=transit_minutes)

                transit_step = PathStep(
                    step_sequence=len(steps) + 1,
                    step_type=StepType.TRANSIT,
                    from_facility=from_node,
                    to_facility=to_node,
                    from_lat=from_fac.lat,
                    from_lon=from_fac.lon,
                    to_lat=to_fac.lat,
                    to_lon=to_fac.lon,
                    distance_miles=distance,
                    start_utc=cpt_departure_utc,
                    end_utc=arrival_utc,
                    duration_minutes=transit_minutes,
                    sort_window_dwell_minutes=0,
                    cpt_dwell_minutes=cpt_dwell,
                    total_dwell_minutes=cpt_dwell
                )
                steps.append(transit_step)
                total_cpt_dwell += cpt_dwell
                current_time_utc = arrival_utc

                # Intermediate processing (not at destination)
                is_last_leg = (i == len(path.path_nodes) - 2)
                if not is_last_leg:
                    processing_step = self._calculate_intermediate_processing_forward(
                        to_fac, path, i + 1, current_time_utc
                    )

                    if processing_step:
                        steps.append(processing_step)
                        total_sort_window_dwell += processing_step.sort_window_dwell_minutes
                        current_time_utc = processing_step.end_utc

        # Step 3: Last mile sort at destination (if dest_sort_level = MARKET)
        needs_lm_sort = (path.dest_sort_level == SortLevel.MARKET)

        if needs_lm_sort and dest_fac.facility_type in (FacilityType.LAUNCH, FacilityType.HYBRID):
            lm_sort_window = dest_fac.get_lm_sort_window()

            if lm_sort_window:
                lm_start_utc, window_dwell = align_to_window_start(
                    current_time_utc,
                    lm_sort_window,
                    self.timing_params.last_mile_sort_minutes
                )
            else:
                lm_start_utc = current_time_utc
                window_dwell = 0

            lm_end_utc = lm_start_utc + timedelta(
                minutes=self.timing_params.last_mile_sort_minutes
            )

            lm_step = PathStep(
                step_sequence=len(steps) + 1,
                step_type=StepType.LAST_MILE_SORT,
                from_facility=path.dest,
                to_facility=path.dest,
                from_lat=dest_fac.lat,
                from_lon=dest_fac.lon,
                to_lat=dest_fac.lat,
                to_lon=dest_fac.lon,
                distance_miles=0,
                start_utc=lm_start_utc,
                end_utc=lm_end_utc,
                duration_minutes=self.timing_params.last_mile_sort_minutes,
                sort_window_dwell_minutes=window_dwell,
                cpt_dwell_minutes=0,
                total_dwell_minutes=window_dwell
            )
            steps.append(lm_step)
            total_sort_window_dwell += window_dwell
            current_time_utc = lm_end_utc

        # Calculate results
        arrival_time_utc = current_time_utc
        tit_hours = (arrival_time_utc - injection_utc).total_seconds() / 3600
        total_dwell_hours = (total_sort_window_dwell + total_cpt_dwell) / MINUTES_PER_HOUR

        return PathTimingResult(
            path=path,
            tit_hours=tit_hours,
            sort_window_dwell_hours=total_sort_window_dwell / MINUTES_PER_HOUR,
            cpt_dwell_hours=total_cpt_dwell / MINUTES_PER_HOUR,
            total_dwell_hours=total_dwell_hours,
            required_injection_utc=injection_utc,
            delivery_datetime_utc=arrival_time_utc,
            sla_days=0,
            sla_buffer_days=0,
            sla_target_hours=0,
            sla_met=False,
            sla_slack_hours=0,
            priority_weight=1.0,
            steps=steps,
            uses_only_active_arcs=all_arcs_active
        )

    def _find_next_cpt(
            self,
            ready_utc: datetime,
            cpts: list[CPT],
            origin_fac: Facility,
            origin: str,
            dest: str
    ) -> tuple[datetime, float, bool]:
        """
        Find the next CPT departure at or after ready_utc (forward-chaining).
        Returns (departure_utc, dwell_minutes, is_active).
        """
        if not cpts:
            return ready_utc, 0.0, False  # No CPT = not active

        ready_local = utc_to_local(ready_utc, origin_fac.timezone)
        search_date = ready_local.date()

        # Build list of (cpt_utc, is_active) tuples
        cpt_candidates = []
        for cpt in cpts:
            for day_offset in [0, 1, 2, 3, 4]:
                cpt_date = search_date + timedelta(days=day_offset)
                cpt_local = datetime.combine(cpt_date, cpt.cpt_local)
                cpt_utc = local_to_utc(cpt_local, cpt.timezone)
                if cpt_utc >= ready_utc:
                    cpt_candidates.append((cpt_utc, cpt.is_active))

        if cpt_candidates:
            # Sort by time and take earliest
            cpt_candidates.sort(key=lambda x: x[0])
            next_cpt_utc, is_active = cpt_candidates[0]
            dwell_minutes = (next_cpt_utc - ready_utc).total_seconds() / 60
            return next_cpt_utc, max(0, dwell_minutes), is_active

        logger.warning(f"No valid CPT found for arc {origin}->{dest}, using ready time")
        return ready_utc, 0.0, False

    def _calculate_intermediate_processing_forward(
            self,
            facility: Facility,
            path: PathCandidate,
            node_index: int,
            arrival_utc: datetime
    ) -> Optional[PathStep]:
        """
        Calculate processing time at intermediate facilities (forward-chaining).

        Rules:
        - SORT_GROUP: All intermediates crossdock
        - MARKET: All intermediates crossdock
        - REGION: Crossdock at all intermediates EXCEPT regional_sort_hub (2nd-to-last),
                  which does full sort
        """
        sort_level = path.sort_level

        # Check if this is the regional_sort_hub (2nd-to-last node)
        is_regional_hub_sort = False
        if sort_level == SortLevel.REGION:
            regional_hub_node = path.path_nodes[-2]
            if facility.name == regional_hub_node:
                is_regional_hub_sort = True

        # Determine processing type
        if is_regional_hub_sort:
            processing_minutes = self.timing_params.middle_mile_sort_minutes
            step_type = StepType.FULL_SORT
        else:
            processing_minutes = self.timing_params.middle_mile_crossdock_minutes
            step_type = StepType.CROSSDOCK

        mm_window = facility.get_mm_sort_window()

        if mm_window:
            process_start_utc, window_dwell = align_to_window_start(
                arrival_utc,
                mm_window,
                processing_minutes
            )
        else:
            process_start_utc = arrival_utc
            window_dwell = 0

        process_end_utc = process_start_utc + timedelta(minutes=processing_minutes)

        return PathStep(
            step_sequence=0,
            step_type=step_type,
            from_facility=facility.name,
            to_facility=facility.name,
            from_lat=facility.lat,
            from_lon=facility.lon,
            to_lat=facility.lat,
            to_lon=facility.lon,
            distance_miles=0,
            start_utc=process_start_utc,
            end_utc=process_end_utc,
            duration_minutes=processing_minutes,
            sort_window_dwell_minutes=window_dwell,
            cpt_dwell_minutes=0,
            total_dwell_minutes=window_dwell
        )

    def _get_delivery_deadline_utc(self, dest_fac: Facility) -> datetime:
        if dest_fac.lm_sort_end_local is None:
            from datetime import time as dt_time
            deadline_local = datetime.combine(self.reference_date.date(), dt_time(23, 59))
        else:
            deadline_local = datetime.combine(
                self.reference_date.date(),
                dest_fac.lm_sort_end_local
            )

        return local_to_utc(deadline_local, dest_fac.timezone)


def calculate_all_path_timings(
        data: dict,
        od_paths: dict[tuple[str, str], list[PathCandidate]]
) -> dict[tuple[str, str], list[PathTimingResult]]:
    cpt_generator = CPTGenerator(
        facilities=data["facilities"],
        arc_cpts=data["arc_cpts"]
    )

    run_settings = data["run_settings"]

    engine = TimingEngine(
        facilities=data["facilities"],
        mileage_bands=data["mileage_bands"],
        timing_params=data["timing_params"],
        cpt_generator=cpt_generator,
        reference_date=run_settings.reference_injection_date,
        reference_injection_time=run_settings.reference_injection_time
    )

    od_timings = {}
    total_paths = sum(len(paths) for paths in od_paths.values())
    processed = 0

    for (origin, dest), paths in od_paths.items():
        timings = []
        for path in paths:
            try:
                timing = engine.calculate_path_timing(path)
                timings.append(timing)
            except Exception as e:
                logger.warning(f"Failed to calculate timing for path {path.path_nodes}: {e}")

        od_timings[(origin, dest)] = timings
        processed += len(paths)

        if processed % 1000 == 0:
            logger.info(f"Processed {processed}/{total_paths} paths")

    logger.info(f"Calculated timings for {processed} paths")
    return od_timings