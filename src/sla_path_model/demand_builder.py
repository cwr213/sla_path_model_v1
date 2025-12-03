"""
Build OD demand matrix from input data.

Matches logic from build_structures_v4.py - peak_pct_of_annual is a DAILY rate
(fraction of annual volume that flows on a single peak day), NOT a seasonal total.
"""
from collections import defaultdict

from .config import Facility, FacilityType, FlowType, MileageBand, ODDemand
from .geo import haversine_miles, get_zone_for_distance
from .utils import setup_logging

logger = setup_logging()


class DemandBuilder:

    def __init__(
            self,
            facilities: dict[str, Facility],
            zips_df,
            demand_df,
            injection_df,
            scenarios_df,
            mileage_bands: list[MileageBand]
    ):
        self.facilities = facilities
        self.zips_df = zips_df
        self.demand_df = demand_df
        self.injection_df = injection_df
        self.scenarios_df = scenarios_df
        self.mileage_bands = sorted(mileage_bands, key=lambda b: b.zone)

        self._build_destination_shares()
        self._build_injection_shares()

    def _build_destination_shares(self):
        """Calculate destination share by population."""
        self.dest_shares = {}

        if self.zips_df.empty:
            # Fallback: equal distribution to launch/hybrid facilities
            delivery_facs = [
                name for name, fac in self.facilities.items()
                if fac.facility_type in (FacilityType.LAUNCH, FacilityType.HYBRID)
            ]
            n = len(delivery_facs)
            self.dest_shares = {f: 1.0 / n for f in delivery_facs} if n > 0 else {}
            return

        pop_by_fac = self.zips_df.groupby('facility_name_assigned')['population'].sum()
        total_pop = pop_by_fac.sum()

        if total_pop > 0:
            self.dest_shares = (pop_by_fac / total_pop).to_dict()

        logger.info(f"Built destination shares for {len(self.dest_shares)} facilities")

    def _build_injection_shares(self):
        """Build injection facility shares."""
        self.injection_shares = {}

        for _, row in self.injection_df.iterrows():
            fac_name = str(row["facility_name"]).strip()
            share = float(row["absolute_share"])
            self.injection_shares[fac_name] = share

        total = sum(self.injection_shares.values())
        if abs(total - 1.0) > 0.01:
            logger.warning(f"Injection shares sum to {total:.3f}, expected 1.0")

        logger.info(f"Built injection shares for {len(self.injection_shares)} facilities")

    def _get_demand_params(self, year: int, day_type: str) -> dict:
        """Get demand parameters for year/day_type."""
        year_demand = self.demand_df[self.demand_df["year"] == year]

        if len(year_demand) == 0:
            raise ValueError(f"No demand data for year {year}")

        row = year_demand.iloc[0]

        annual_pkgs = float(row['annual_pkgs'])

        if day_type == "peak":
            pct_of_annual = float(row['peak_pct_of_annual'])
            mm_share = float(row['middle_mile_share_peak'])
        else:
            pct_of_annual = float(row['offpeak_pct_of_annual'])
            mm_share = float(row['middle_mile_share_offpeak'])

        # NO division by days - pct_of_annual is already a daily rate
        daily_pkgs = annual_pkgs * pct_of_annual

        return {
            'daily_pkgs': daily_pkgs,
            'mm_share': mm_share
        }

    def _calculate_zone(self, origin: str, dest: str) -> int:
        """Calculate zone from distance."""
        origin_fac = self.facilities[origin]
        dest_fac = self.facilities[dest]

        distance = haversine_miles(
            origin_fac.lat, origin_fac.lon,
            dest_fac.lat, dest_fac.lon
        )

        band = get_zone_for_distance(distance, self.mileage_bands)

        if band:
            return band.zone

        return self.mileage_bands[-1].zone if self.mileage_bands else -1

    def build_demands(self) -> list[ODDemand]:
        """Build OD demand list for all scenarios."""
        demands = []

        for _, scenario in self.scenarios_df.iterrows():
            scenario_id = str(scenario["scenario_id"])
            year = int(scenario["year"])
            day_type = str(scenario["day_type"]).lower().strip()

            logger.info(f"Building demand for scenario {scenario_id} (year={year}, {day_type})")

            params = self._get_demand_params(year, day_type)
            daily_pkgs = params['daily_pkgs']
            mm_share = params['mm_share']

            if daily_pkgs <= 0:
                logger.warning(f"Zero demand for scenario {scenario_id}")
                continue

            scenario_demands = self._build_od_matrix(
                scenario_id, daily_pkgs, mm_share, day_type
            )
            demands.extend(scenario_demands)

            mm_pkgs = sum(d.pkgs_day for d in scenario_demands if d.flow_type == FlowType.MIDDLE_MILE)
            di_pkgs = sum(d.pkgs_day for d in scenario_demands if d.flow_type == FlowType.DIRECT_INJECTION)

            logger.info(
                f"  Scenario {scenario_id}: {len(scenario_demands)} OD pairs, "
                f"{daily_pkgs:,.0f} total pkgs/day (MM: {mm_pkgs:,.0f}, DI: {di_pkgs:,.0f})"
            )

        logger.info(f"Built {len(demands)} total OD demand records")
        return demands

    def _build_od_matrix(
            self,
            scenario_id: str,
            daily_pkgs: float,
            mm_share: float,
            day_type: str
    ) -> list[ODDemand]:
        """Build OD matrix for a single scenario."""
        demands = []

        # Middle-mile volume distributed by injection share then destination share
        mm_daily = daily_pkgs * mm_share

        for origin, inj_share in self.injection_shares.items():
            if inj_share < 0.0001:
                continue

            if origin not in self.facilities:
                logger.warning(f"Unknown injection facility: {origin}")
                continue

            origin_mm = mm_daily * inj_share

            for dest, dest_share in self.dest_shares.items():
                od_pkgs = origin_mm * dest_share

                if od_pkgs < 0.01:
                    continue

                if dest not in self.facilities:
                    logger.warning(f"Unknown destination facility: {dest}")
                    continue

                # O=D only allowed for hybrid facilities
                if origin == dest:
                    fac_type = self.facilities[origin].facility_type
                    if fac_type != FacilityType.HYBRID:
                        continue

                zone = self._calculate_zone(origin, dest)

                demands.append(ODDemand(
                    scenario_id=scenario_id,
                    origin=origin,
                    dest=dest,
                    pkgs_day=od_pkgs,
                    zone=zone,
                    flow_type=FlowType.MIDDLE_MILE,
                    day_type=day_type
                ))

        # Direct injection (Zone 0) - distributed to destinations by population
        di_daily = daily_pkgs * (1 - mm_share)

        for dest, dest_share in self.dest_shares.items():
            di_pkgs = di_daily * dest_share

            if di_pkgs < 0.01:
                continue

            if dest not in self.facilities:
                continue

            demands.append(ODDemand(
                scenario_id=scenario_id,
                origin=dest,  # Direct injection: origin = dest
                dest=dest,
                pkgs_day=di_pkgs,
                zone=0,
                flow_type=FlowType.DIRECT_INJECTION,
                day_type=day_type
            ))

        return demands


def build_od_demand(data: dict) -> list[ODDemand]:
    """Build OD demand from loaded data."""
    builder = DemandBuilder(
        facilities=data["facilities"],
        zips_df=data["zips"],
        demand_df=data["demand"],
        injection_df=data["injection_distribution"],
        scenarios_df=data["scenarios"],
        mileage_bands=data["mileage_bands"]
    )

    return builder.build_demands()