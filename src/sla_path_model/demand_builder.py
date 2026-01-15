"""
Build OD demand matrix from input data.

Three flow types:
1. Direct Injection: O=D at facility assigned for that year (zone 0)
2. Zone Skip: Origin = regional_sort_hub of destination, Dest = facility assigned for that year
3. Middle Mile: Origin = per injection_distribution, Dest = per population for that year

Shares come from demand sheet: direct_injection_share_*, zone_skip_share_*, middle_mile_share_*
These must sum to 1.0 for each day_type.

Zip coverage is year-based via facility_YYYY columns in zips sheet.
Blank = zip not in coverage for that year.
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

        self._parse_facility_year_columns()
        self._build_regional_hub_mapping()
        self._build_injection_shares()

    def _parse_facility_year_columns(self):
        """Find facility_YYYY columns and extract available years."""
        self.facility_year_cols = {}

        for col in self.zips_df.columns:
            if col.startswith('facility_'):
                try:
                    year = int(col.replace('facility_', ''))
                    self.facility_year_cols[year] = col
                except ValueError:
                    continue

        if not self.facility_year_cols:
            raise ValueError(
                "No facility_YYYY columns found in zips sheet. "
                "Expected columns like facility_2024, facility_2025, etc."
            )

        self.available_years = sorted(self.facility_year_cols.keys())
        logger.info(f"Found facility columns for years: {self.available_years}")

    def _build_destination_shares_for_year(self, year: int) -> dict[str, float]:
        """Calculate destination share by population for a specific year."""
        if year not in self.facility_year_cols:
            return {}

        col = self.facility_year_cols[year]

        # Filter to zips with facility assignment (non-blank)
        active_zips = self.zips_df[self.zips_df[col].notna() & (self.zips_df[col] != '')]

        if active_zips.empty:
            return {}

        pop_by_fac = active_zips.groupby(col)['population'].sum()
        total_pop = pop_by_fac.sum()

        if total_pop > 0:
            return (pop_by_fac / total_pop).to_dict()

        return {}

    def _build_regional_hub_mapping(self):
        """
        Build mapping from facility to its regional_sort_hub.
        Also build reverse mapping: regional_sort_hub -> list of facilities it serves.
        """
        self.facility_to_regional_hub = {}
        self.regional_hub_to_facilities = defaultdict(list)

        for name, fac in self.facilities.items():
            if fac.regional_sort_hub:
                self.facility_to_regional_hub[name] = fac.regional_sort_hub
                self.regional_hub_to_facilities[fac.regional_sort_hub].append(name)

        logger.info(
            f"Built regional hub mapping: {len(self.regional_hub_to_facilities)} hubs "
            f"covering {len(self.facility_to_regional_hub)} facilities"
        )

    def _build_injection_shares(self):
        """Build injection facility shares for middle mile by year."""
        # Check if using new year-based format or legacy format
        share_cols = [c for c in self.injection_df.columns if c.startswith('share_')]

        if share_cols:
            # New year-based format
            self.injection_shares_by_year = {}

            for col in share_cols:
                year = int(col.replace('share_', ''))
                shares = {}

                for _, row in self.injection_df.iterrows():
                    fac_name = str(row["facility_name"]).strip()
                    share = float(row[col])
                    if share > 0:  # Only include facilities with non-zero share
                        shares[fac_name] = share

                self.injection_shares_by_year[year] = shares

            logger.info(
                f"Built injection shares for {len(self.injection_df)} facilities "
                f"across years: {sorted(self.injection_shares_by_year.keys())}"
            )

        elif 'absolute_share' in self.injection_df.columns:
            # Legacy format - use same shares for all years
            self.injection_shares_by_year = {}
            shares = {}

            for _, row in self.injection_df.iterrows():
                fac_name = str(row["facility_name"]).strip()
                share = float(row["absolute_share"])
                if share > 0:
                    shares[fac_name] = share

            # Apply same shares to all available years (from facility_year_cols)
            for year in self.available_years:
                self.injection_shares_by_year[year] = shares.copy()

            logger.info(
                f"Built injection shares for {len(shares)} facilities "
                f"(legacy format applied to years: {self.available_years})"
            )
        else:
            raise ValueError(
                "injection_distribution sheet must have either 'absolute_share' or 'share_YYYY' columns"
            )

    def _get_injection_shares(self, year: int) -> dict[str, float]:
        """Get injection shares for a specific year."""
        if year not in self.injection_shares_by_year:
            raise ValueError(
                f"No injection distribution found for year {year}. "
                f"Available years: {sorted(self.injection_shares_by_year.keys())}"
            )
        return self.injection_shares_by_year[year]

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
            zs_share = float(row['zone_skip_share_peak'])
            di_share = float(row['direct_injection_share_peak'])
        else:
            pct_of_annual = float(row['offpeak_pct_of_annual'])
            mm_share = float(row['middle_mile_share_offpeak'])
            zs_share = float(row['zone_skip_share_offpeak'])
            di_share = float(row['direct_injection_share_offpeak'])

        # Validate shares sum to 1.0
        total_share = mm_share + zs_share + di_share
        if abs(total_share - 1.0) > 0.01:
            raise ValueError(
                f"Flow shares must sum to 1.0, got {total_share:.4f} "
                f"(mm={mm_share}, zs={zs_share}, di={di_share})"
            )

        # pct_of_annual is daily rate (fraction of annual that flows on this day type)
        daily_pkgs = annual_pkgs * pct_of_annual

        return {
            'daily_pkgs': daily_pkgs,
            'mm_share': mm_share,
            'zs_share': zs_share,
            'di_share': di_share
        }

    def _calculate_zone(self, origin: str, dest: str) -> int:
        """Calculate zone from distance between facilities."""
        origin_fac = self.facilities[origin]
        dest_fac = self.facilities[dest]

        distance = haversine_miles(
            origin_fac.lat, origin_fac.lon,
            dest_fac.lat, dest_fac.lon
        )

        band = get_zone_for_distance(distance, self.mileage_bands)
        if band:
            return band.zone

        return self.mileage_bands[-1].zone if self.mileage_bands else 1

    def build_demands(self) -> list[ODDemand]:
        """Build OD demand list for all scenarios."""
        demands = []

        for _, scenario in self.scenarios_df.iterrows():
            scenario_id = str(scenario["scenario_id"])
            year = int(scenario["year"])
            day_type = str(scenario["day_type"]).lower().strip()

            logger.info(f"Building demand for scenario {scenario_id} (year={year}, {day_type})")

            # Check year has facility column
            if year not in self.facility_year_cols:
                raise ValueError(
                    f"Scenario '{scenario_id}' uses year {year} but no facility_{year} "
                    f"column found in zips sheet. Available years: {self.available_years}"
                )

            params = self._get_demand_params(year, day_type)
            daily_pkgs = params['daily_pkgs']

            if daily_pkgs <= 0:
                raise ValueError(f"Zero or negative daily demand for scenario {scenario_id}")

            # Build destination shares for this year
            dest_shares = self._build_destination_shares_for_year(year)
            if not dest_shares:
                raise ValueError(
                    f"No destination facilities found for year {year}. "
                    f"Check that facility_{year} column has valid facility names."
                )

            scenario_demands = self._build_od_matrix(scenario_id, params, day_type, dest_shares, year)
            demands.extend(scenario_demands)

            # Log summary by flow type
            mm_pkgs = sum(d.pkgs_day for d in scenario_demands if d.flow_type == FlowType.MIDDLE_MILE)
            zs_pkgs = sum(d.pkgs_day for d in scenario_demands if d.flow_type == FlowType.ZONE_SKIP)
            di_pkgs = sum(d.pkgs_day for d in scenario_demands if d.flow_type == FlowType.DIRECT_INJECTION)

            logger.info(
                f"  Scenario {scenario_id}: {len(scenario_demands)} OD pairs, "
                f"{daily_pkgs:,.0f} total pkgs/day"
            )
            logger.info(
                f"    MM: {mm_pkgs:,.0f} ({100*mm_pkgs/daily_pkgs:.1f}%), "
                f"ZS: {zs_pkgs:,.0f} ({100*zs_pkgs/daily_pkgs:.1f}%), "
                f"DI: {di_pkgs:,.0f} ({100*di_pkgs/daily_pkgs:.1f}%)"
            )
            logger.info(f"    Active destinations: {len(dest_shares)} facilities")

        logger.info(f"Built {len(demands)} total OD demand records")
        return demands

    def _build_od_matrix(
            self,
            scenario_id: str,
            params: dict,
            day_type: str,
            dest_shares: dict[str, float],
            year: int
    ) -> list[ODDemand]:
        """Build OD matrix for a single scenario."""
        demands = []
        daily_pkgs = params['daily_pkgs']

        # 1. DIRECT INJECTION: O=D at facility assigned for this year (zone 0)
        di_daily = daily_pkgs * params['di_share']
        if di_daily > 0:
            for dest, dest_share in dest_shares.items():
                di_pkgs = di_daily * dest_share
                if di_pkgs < 0.01:
                    continue
                if dest not in self.facilities:
                    continue

                demands.append(ODDemand(
                    scenario_id=scenario_id,
                    origin=dest,  # O=D
                    dest=dest,
                    pkgs_day=di_pkgs,
                    zone=0,
                    flow_type=FlowType.DIRECT_INJECTION,
                    day_type=day_type
                ))

        # 2. ZONE SKIP: Origin = regional_sort_hub of dest, Dest = facility assigned for year
        zs_daily = daily_pkgs * params['zs_share']
        if zs_daily > 0:
            for dest, dest_share in dest_shares.items():
                if dest not in self.facilities:
                    continue

                # Get regional_sort_hub for this destination
                regional_hub = self.facility_to_regional_hub.get(dest)
                if not regional_hub:
                    logger.debug(f"No regional_sort_hub for dest {dest}, skipping zone skip")
                    continue

                if regional_hub not in self.facilities:
                    logger.warning(f"Regional hub {regional_hub} not in facilities")
                    continue

                zs_pkgs = zs_daily * dest_share
                if zs_pkgs < 0.01:
                    continue

                # Zone skip always uses mileage bands (zone 0 reserved for direct injection)
                zone = self._calculate_zone(regional_hub, dest)

                demands.append(ODDemand(
                    scenario_id=scenario_id,
                    origin=regional_hub,
                    dest=dest,
                    pkgs_day=zs_pkgs,
                    zone=zone,
                    flow_type=FlowType.ZONE_SKIP,
                    day_type=day_type
                ))

        # 3. MIDDLE MILE: Origin = per injection_distribution (year-specific), Dest = per population
        mm_daily = daily_pkgs * params['mm_share']
        if mm_daily > 0:
            injection_shares = self._get_injection_shares(year)

            for origin, inj_share in injection_shares.items():
                if inj_share < 0.0001:
                    continue
                if origin not in self.facilities:
                    raise ValueError(f"Unknown injection facility: {origin}")

                origin_mm = mm_daily * inj_share

                for dest, dest_share in dest_shares.items():
                    od_pkgs = origin_mm * dest_share
                    if od_pkgs < 0.01:
                        continue
                    if dest not in self.facilities:
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