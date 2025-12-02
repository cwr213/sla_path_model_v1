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
from zoneinfo import ZoneInfo

from .config import (
    Facility, FacilityType, PathCandidate, PathStep, PathTimingResult,
    SortLevel, StepType, TimingParams, MileageBand, CPT,
    MINUTES_PER_HOUR
)
from .cpt_generator import CPTGenerator, get_cpts_for_path
from .geo import haversine_miles, get_zone_for_distance, calculate_transit_time_minutes
from .time_utils import (
    local_to_utc, utc_to_local, align_to_window_end, is_time_in_window,
    time_to_minutes, UTC
)
from .utils import setup_logging

logger = setup_logging()


class TimingEngine:
    """Calculate time-in-transit for paths using backward-chaining."""

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

    def calculate_path_timing(
            self,
            path: PathCandidate
    ) -> PathTimingResult:
        """
        Calculate complete timing for a path using backward-chaining.

        Args:
            path: PathCandidate to evaluate

        Returns:
            PathTimingResult with timing details
        """
        dest_fac = self.facilities[path.dest]

        # Start from delivery deadline (lm_sort_end_local at destination)
        delivery_deadline_utc = self._get_delivery_deadline_utc(dest_fac)

        # Build path arcs with their CPTs
        arc_cpts = get_cpts_for_path(path.path_nodes, self.cpt_generator)

        # Backward-chain through the path
        steps = []
        current_time_utc = delivery_deadline_utc
        total_sort_window_dwell = 0.0
        total_cpt_dwell = 0.0

        # Process path from end to beginning (backward)
        # Segments: transit legs and processing at each facility

        # Step 1: Last mile sort at destination
        if dest_fac.facility_type in (FacilityType.LAUNCH, FacilityType.HYBRID):
            lm_sort_window = dest_fac.get_lm_sort_window()

            if lm_sort_window:
                # Backward-chain: when must LM sort start to finish by deadline?
                process_start_utc, window_dwell = align_to_window_end(
                    current_time_utc,
                    lm_sort_window,
                    self.timing_params.last_mile_sort_minutes
                )

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

        # Process remaining path segments backward
        # path_nodes: [origin, hub1, hub2, ..., dest]
        # We've processed dest, now process dest<-hubN, hubN processing, hubN<-hubN-1, etc.

        for i in range(len(path.path_nodes) - 1, 0, -1):
            # Arc: path_nodes[i-1] -> path_nodes[i]
            from_node = path.path_nodes[i - 1]
            to_node = path.path_nodes[i]

            from_fac = self.facilities[from_node]
            to_fac = self.facilities[to_node]

            # Transit time for this arc
            distance = haversine_miles(from_fac.lat, from_fac.lon, to_fac.lat, to_fac.lon)
            band = get_zone_for_distance(distance, self.mileage_bands)

            if band:
                transit_minutes = calculate_transit_time_minutes(
                    distance, band.circuity_factor, band.mph
                )
            else:
                # Very short distance, use default
                transit_minutes = distance / 50 * MINUTES_PER_HOUR  # 50 mph default

            # Work backward: arrival at to_node must be by current_time_utc
            # So departure from from_node must be current_time_utc - transit_time
            required_arrival_utc = current_time_utc
            required_departure_utc = current_time_utc - timedelta(minutes=transit_minutes)

            # Find the latest CPT from from_node that departs by required_departure_utc
            cpts = self.cpt_generator.get_cpts_for_arc(from_node, to_node)
            cpt_departure_utc, cpt_dwell = self._find_latest_cpt(
                required_departure_utc, cpts, from_fac
            )

            # Transit step
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

            # Processing at from_node (if not origin)
            if i > 1:  # Not the origin
                # Determine processing type based on sort level
                processing_step = self._calculate_intermediate_processing(
                    from_fac, path.sort_level, cpt_departure_utc
                )

                if processing_step:
                    steps.append(processing_step)
                    total_sort_window_dwell += processing_step.sort_window_dwell_minutes
                    current_time_utc = processing_step.start_utc
                else:
                    current_time_utc = cpt_departure_utc
            else:
                # Origin facility - induction sort
                current_time_utc = cpt_departure_utc

        # Induction sort at origin
        origin_fac = self.facilities[path.origin]
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

        # Reverse steps to chronological order
        steps.reverse()
        for i, step in enumerate(steps):
            step.step_sequence = i + 1

        # Calculate total TIT
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
            sla_days=0,  # To be filled by feasibility module
            sla_buffer_days=0,
            sla_target_hours=0,
            sla_met=False,
            sla_slack_hours=0,
            priority_weight=1.0,
            steps=steps
        )

    def _get_delivery_deadline_utc(self, dest_fac: Facility) -> datetime:
        """Get delivery deadline (lm_sort_end_local) in UTC for reference date."""
        if dest_fac.lm_sort_end_local is None:
            # Default to midnight if not specified
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
            origin_fac: Facility
    ) -> tuple[datetime, float]:
        """
        Find latest CPT departure that is at or before required_by_utc.

        Returns:
            (cpt_departure_utc, dwell_minutes): Departure time and dwell waiting for CPT
        """
        if not cpts:
            # No CPTs, use required time directly
            return required_by_utc, 0.0

        # Convert CPTs to UTC datetimes for reference date
        cpt_datetimes = []
        for cpt in cpts:
            # Check multiple days (today, yesterday, day before)
            for day_offset in [0, -1, -2]:
                cpt_date = self.reference_date.date() + timedelta(days=day_offset)
                cpt_local = datetime.combine(cpt_date, cpt.cpt_local)
                cpt_utc = local_to_utc(cpt_local, cpt.timezone)
                cpt_datetimes.append(cpt_utc)

        # Find latest CPT at or before required time
        valid_cpts = [c for c in cpt_datetimes if c <= required_by_utc]

        if valid_cpts:
            latest_cpt = max(valid_cpts)
            # Dwell is time from arrival at facility to CPT departure
            # (This is an approximation; actual dwell depends on when processing finishes)
            dwell_minutes = (required_by_utc - latest_cpt).total_seconds() / 60
            return latest_cpt, max(0, dwell_minutes)

        # No valid CPT found, go back another day
        for day_offset in [-3, -4, -5]:
            for cpt in cpts:
                cpt_date = self.reference_date.date() + timedelta(days=day_offset)
                cpt_local = datetime.combine(cpt_date, cpt.cpt_local)
                cpt_utc = local_to_utc(cpt_local, cpt.timezone)
                if cpt_utc <= required_by_utc:
                    dwell_minutes = (required_by_utc - cpt_utc).total_seconds() / 60
                    return cpt_utc, max(0, dwell_minutes)

        # Fall back to required time
        logger.warning(f"No valid CPT found for {origin_fac.name}, using required time")
        return required_by_utc, 0.0

    def _calculate_intermediate_processing(
            self,
            facility: Facility,
            sort_level: SortLevel,
            must_complete_by_utc: datetime
    ) -> Optional[PathStep]:
        """
        Calculate processing at intermediate hub based on sort level.

        Args:
            facility: Intermediate facility
            sort_level: Determines crossdock vs full sort
            must_complete_by_utc: Processing must complete by this time

        Returns:
            PathStep for the processing, or None if no processing needed
        """
        # Determine processing time based on sort level
        if sort_level == SortLevel.REGION:
            # Crossdock - fastest
            processing_minutes = self.timing_params.middle_mile_crossdock_minutes
            step_type = StepType.CROSSDOCK
        else:
            # Full sort for market or sort_group level
            processing_minutes = self.timing_params.middle_mile_sort_minutes
            step_type = StepType.FULL_SORT

        # Get sort window
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
            step_sequence=0,  # Will be renumbered
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
    """
    Calculate timings for all enumerated paths.

    Args:
        data: Dictionary from InputLoader.load_all()
        od_paths: Dictionary mapping (origin, dest) to list of PathCandidate

    Returns:
        Dictionary mapping (origin, dest) to list of PathTimingResult
    """
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

        if processed % 100 == 0:
            logger.info(f"Processed {processed}/{total_paths} paths")

    logger.info(f"Calculated timings for {processed} paths")
    return od_timings