"""
Timing Engine: calculate time-in-transit using backward-chaining from delivery deadline.

Algorithm:
1. Start from delivery deadline (lm_sort_end_local at destination)
2. Work backward through each path segment:
   - Subtract processing time
   - Align to sort window (add dwell if outside window)
   - Find latest CPT that arrives before required time
   - Subtract transit time
   - Repeat for each hop
3. Result: required injection time and total TIT
"""

from datetime import datetime, timedelta
from typing import Optional

from .config import (
    Facility, FacilityType, PathCandidate, PathStep, PathTimingResult,
    SortLevel, StepType, TimingParams, MileageBand, CPT,
    MINUTES_PER_HOUR
)
from .cpt_generator import CPTGenerator, get_cpts_for_path
from .geo import haversine_miles, get_zone_for_distance, calculate_transit_time_minutes
from .time_utils import local_to_utc, utc_to_local, align_to_window_end
from .utils import setup_logging

logger = setup_logging()


class TimingEngine:

    def __init__(
            self,
            facilities: dict[str, Facility],
            mileage_bands: list[MileageBand],
            timing_params: TimingParams,
            cpt_generator: CPTGenerator,
            reference_date: datetime
    ):
        self.facilities = facilities
        self.mileage_bands = sorted(mileage_bands, key=lambda b: b.zone)
        self.timing_params = timing_params
        self.cpt_generator = cpt_generator
        self.reference_date = reference_date

    def calculate_path_timing(self, path: PathCandidate) -> PathTimingResult:
        dest_fac = self.facilities[path.dest]
        origin_fac = self.facilities[path.origin]
        delivery_deadline_utc = self._get_delivery_deadline_utc(dest_fac)

        steps = []
        current_time_utc = delivery_deadline_utc
        total_sort_window_dwell = 0.0
        total_cpt_dwell = 0.0

        is_od_equal = (path.origin == path.dest)

        # Last mile sort at destination - only if dest_sort_level is MARKET
        # (SORT_GROUP means already sorted to route level, no LM sort needed)
        needs_lm_sort = (path.dest_sort_level == SortLevel.MARKET)

        if needs_lm_sort and dest_fac.facility_type in (FacilityType.LAUNCH, FacilityType.HYBRID):
            lm_sort_window = dest_fac.get_lm_sort_window()

            if lm_sort_window:
                process_start_utc, window_dwell = align_to_window_end(
                    current_time_utc,
                    lm_sort_window,
                    self.timing_params.last_mile_sort_minutes
                )
            else:
                process_start_utc = current_time_utc - timedelta(
                    minutes=self.timing_params.last_mile_sort_minutes
                )
                window_dwell = 0

            step = PathStep(
                step_sequence=1,
                step_type=StepType.LAST_MILE_SORT,
                from_facility=path.dest,
                to_facility=path.dest,
                from_lat=dest_fac.lat,
                from_lon=dest_fac.lon,
                to_lat=dest_fac.lat,
                to_lon=dest_fac.lon,
                distance_miles=0,
                start_utc=process_start_utc,
                end_utc=current_time_utc,
                duration_minutes=self.timing_params.last_mile_sort_minutes,
                sort_window_dwell_minutes=window_dwell,
                cpt_dwell_minutes=0,
                total_dwell_minutes=window_dwell
            )
            steps.append(step)

            total_sort_window_dwell += window_dwell
            current_time_utc = process_start_utc

        # For O=D paths: no transit legs, no CPT waits
        # Just induction (MM sort) at origin, then LM sort (if needed)
        if not is_od_equal:
            # Process path segments backward (transit legs)
            for i in range(len(path.path_nodes) - 1, 0, -1):
                from_node = path.path_nodes[i - 1]
                to_node = path.path_nodes[i]

                from_fac = self.facilities[from_node]
                to_fac = self.facilities[to_node]

                distance = haversine_miles(from_fac.lat, from_fac.lon, to_fac.lat, to_fac.lon)
                band = get_zone_for_distance(distance, self.mileage_bands)

                if band:
                    transit_minutes = calculate_transit_time_minutes(
                        distance, band.circuity_factor, band.mph
                    )
                else:
                    transit_minutes = distance / 50 * MINUTES_PER_HOUR

                required_departure_utc = current_time_utc - timedelta(minutes=transit_minutes)

                cpts = self.cpt_generator.get_cpts_for_arc(from_node, to_node)
                cpt_departure_utc, cpt_dwell = self._find_latest_cpt(
                    required_departure_utc, cpts, from_fac, from_node, to_node
                )

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

                # Intermediate processing (not at origin, i > 1)
                if i > 1:
                    processing_step = self._calculate_intermediate_processing(
                        from_fac, path, i, cpt_departure_utc
                    )

                    if processing_step:
                        steps.append(processing_step)
                        total_sort_window_dwell += processing_step.sort_window_dwell_minutes
                        current_time_utc = processing_step.start_utc
                    else:
                        current_time_utc = cpt_departure_utc
                else:
                    current_time_utc = cpt_departure_utc

        # Induction sort at origin (MM sort)
        mm_window = origin_fac.get_mm_sort_window()

        if mm_window:
            induction_start_utc, window_dwell = align_to_window_end(
                current_time_utc,
                mm_window,
                self.timing_params.induction_sort_minutes
            )
        else:
            induction_start_utc = current_time_utc - timedelta(
                minutes=self.timing_params.induction_sort_minutes
            )
            window_dwell = 0

        induction_step = PathStep(
            step_sequence=len(steps) + 1,
            step_type=StepType.INDUCTION_SORT,
            from_facility=path.origin,
            to_facility=path.origin,
            from_lat=origin_fac.lat,
            from_lon=origin_fac.lon,
            to_lat=origin_fac.lat,
            to_lon=origin_fac.lon,
            distance_miles=0,
            start_utc=induction_start_utc,
            end_utc=current_time_utc,
            duration_minutes=self.timing_params.induction_sort_minutes,
            sort_window_dwell_minutes=window_dwell,
            cpt_dwell_minutes=0,
            total_dwell_minutes=window_dwell
        )
        steps.append(induction_step)
        total_sort_window_dwell += window_dwell

        steps.reverse()
        for i, step in enumerate(steps):
            step.step_sequence = i + 1

        required_injection_utc = induction_start_utc
        tit_hours = (delivery_deadline_utc - required_injection_utc).total_seconds() / 3600
        total_dwell_hours = (total_sort_window_dwell + total_cpt_dwell) / MINUTES_PER_HOUR

        return PathTimingResult(
            path=path,
            tit_hours=tit_hours,
            sort_window_dwell_hours=total_sort_window_dwell / MINUTES_PER_HOUR,
            cpt_dwell_hours=total_cpt_dwell / MINUTES_PER_HOUR,
            total_dwell_hours=total_dwell_hours,
            required_injection_utc=required_injection_utc,
            delivery_datetime_utc=delivery_deadline_utc,
            sla_days=0,
            sla_buffer_days=0,
            sla_target_hours=0,
            sla_met=False,
            sla_slack_hours=0,
            priority_weight=1.0,
            steps=steps
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

    def _find_latest_cpt(
            self,
            required_by_utc: datetime,
            cpts: list[CPT],
            origin_fac: Facility,
            origin: str,
            dest: str
    ) -> tuple[datetime, float]:
        """Find the latest CPT departure at or before required_by_utc."""
        if not cpts:
            return required_by_utc, 0.0

        # Search for CPTs around the required_by_utc date, not reference_date
        required_local = utc_to_local(required_by_utc, origin_fac.timezone)
        search_date = required_local.date()

        cpt_datetimes = []
        for cpt in cpts:
            # Check today and previous days relative to required time
            for day_offset in [0, -1, -2, -3, -4]:
                cpt_date = search_date + timedelta(days=day_offset)
                cpt_local = datetime.combine(cpt_date, cpt.cpt_local)
                cpt_utc = local_to_utc(cpt_local, cpt.timezone)
                cpt_datetimes.append(cpt_utc)

        valid_cpts = [c for c in cpt_datetimes if c <= required_by_utc]

        if valid_cpts:
            latest_cpt = max(valid_cpts)
            dwell_minutes = (required_by_utc - latest_cpt).total_seconds() / 60
            return latest_cpt, max(0, dwell_minutes)

        logger.warning(f"No valid CPT found for arc {origin}->{dest}, using required time")
        return required_by_utc, 0.0

    def _calculate_intermediate_processing(
            self,
            facility: Facility,
            path: PathCandidate,
            path_index: int,
            must_complete_by_utc: datetime
    ) -> Optional[PathStep]:
        """
        Calculate processing time at intermediate facilities.

        Rules:
        - SORT_GROUP: All intermediates crossdock (already sorted to finest level)
        - MARKET: All intermediates crossdock (sorted to market level at origin)
        - REGION: Crossdock at all intermediates EXCEPT regional_sort_hub (2nd-to-last),
                  which does full sort to market or sort_group level
        """
        sort_level = path.sort_level

        # Check if this is the regional_sort_hub (2nd-to-last node)
        # For REGION sort, this node does full sort; others crossdock
        is_regional_hub_sort = False
        if sort_level == SortLevel.REGION:
            # 2nd-to-last node is at index -2, which in backward iteration is i=1
            # But we're at from_node which is path_nodes[i-1]
            # The regional hub is path_nodes[-2]
            regional_hub_node = path.path_nodes[-2]
            if facility.name == regional_hub_node:
                is_regional_hub_sort = True

        # Determine processing type
        if is_regional_hub_sort:
            # Regional sort hub does full sort
            processing_minutes = self.timing_params.middle_mile_sort_minutes
            step_type = StepType.FULL_SORT
        elif sort_level in (SortLevel.SORT_GROUP, SortLevel.MARKET):
            # Already sorted at origin, just crossdock
            processing_minutes = self.timing_params.middle_mile_crossdock_minutes
            step_type = StepType.CROSSDOCK
        else:
            # REGION sort at non-hub intermediate: crossdock
            processing_minutes = self.timing_params.middle_mile_crossdock_minutes
            step_type = StepType.CROSSDOCK

        mm_window = facility.get_mm_sort_window()

        if mm_window:
            process_start_utc, window_dwell = align_to_window_end(
                must_complete_by_utc,
                mm_window,
                processing_minutes
            )
        else:
            process_start_utc = must_complete_by_utc - timedelta(minutes=processing_minutes)
            window_dwell = 0

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
            end_utc=must_complete_by_utc,
            duration_minutes=processing_minutes,
            sort_window_dwell_minutes=window_dwell,
            cpt_dwell_minutes=0,
            total_dwell_minutes=window_dwell
        )


def calculate_all_path_timings(
        data: dict,
        od_paths: dict[tuple[str, str], list[PathCandidate]]
) -> dict[tuple[str, str], list[PathTimingResult]]:
    cpt_generator = CPTGenerator(
        facilities=data["facilities"],
        arc_cpts=data["arc_cpts"]
    )

    engine = TimingEngine(
        facilities=data["facilities"],
        mileage_bands=data["mileage_bands"],
        timing_params=data["timing_params"],
        cpt_generator=cpt_generator,
        reference_date=data["run_settings"].reference_injection_date
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