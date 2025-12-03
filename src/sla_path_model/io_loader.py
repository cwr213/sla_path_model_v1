from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .config import (
    Facility, FacilityType, MileageBand, ServiceCommitment, TimingParams,
    RunSettings, ObjectiveType, CPT
)
from .utils import parse_time_value, parse_days_of_week, setup_logging

logger = setup_logging()


class InputLoader:

    REQUIRED_SHEETS = [
        "facilities",
        "zips",
        "demand",
        "injection_distribution",
        "scenarios",
        "mileage_bands",
        "timing_params",
        "service_commitments",
        "run_settings"
    ]

    def __init__(self, filepath: str):
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise FileNotFoundError(f"Input file not found: {filepath}")

        self.excel = pd.ExcelFile(filepath)
        self._validate_required_sheets()

    def _validate_required_sheets(self):
        missing = [s for s in self.REQUIRED_SHEETS if s not in self.excel.sheet_names]
        if missing:
            raise ValueError(f"Missing required sheets: {missing}")

    def load_facilities(self) -> dict[str, Facility]:
        df = pd.read_excel(self.excel, sheet_name="facilities")

        facilities = {}
        for _, row in df.iterrows():
            name = str(row["facility_name"]).strip()

            try:
                tz = ZoneInfo(row["timezone"])
            except Exception as e:
                raise ValueError(f"Invalid timezone '{row['timezone']}' for facility {name}: {e}")

            facility = Facility(
                name=name,
                facility_type=FacilityType(str(row["type"]).lower().strip()),
                lat=float(row["lat"]),
                lon=float(row["lon"]),
                timezone=tz,
                parent_hub_name=str(row["parent_hub_name"]).strip() if pd.notna(row.get("parent_hub_name")) else None,
                regional_sort_hub=str(row["regional_sort_hub"]).strip() if pd.notna(row.get("regional_sort_hub")) else None,
                is_injection_node=bool(row.get("is_injection_node", False)),
                mm_sort_start_local=parse_time_value(row.get("mm_sort_start_local")),
                mm_sort_end_local=parse_time_value(row.get("mm_sort_end_local")),
                lm_sort_start_local=parse_time_value(row.get("lm_sort_start_local")),
                lm_sort_end_local=parse_time_value(row.get("lm_sort_end_local")),
                outbound_window_start_local=parse_time_value(row.get("outbound_window_start_local")),
                outbound_window_end_local=parse_time_value(row.get("outbound_window_end_local")),
                outbound_cpt_count=int(row["outbound_cpt_count"]) if pd.notna(row.get("outbound_cpt_count")) else None,
                max_inbound_trucks_per_hour=float(row["max_inbound_trucks_per_hour"]) if pd.notna(row.get("max_inbound_trucks_per_hour")) else None,
                max_outbound_trucks_per_hour=float(row["max_outbound_trucks_per_hour"]) if pd.notna(row.get("max_outbound_trucks_per_hour")) else None,
            )
            facilities[name] = facility

        logger.info(f"Loaded {len(facilities)} facilities")
        return facilities

    def load_zips(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel, sheet_name="zips")
        df["zip"] = df["zip"].astype(str).str.zfill(5)
        logger.info(f"Loaded {len(df)} ZIP codes")
        return df

    def load_demand(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel, sheet_name="demand")
        logger.info(f"Loaded demand data for {len(df)} year(s)")
        return df

    def load_injection_distribution(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel, sheet_name="injection_distribution")

        total_share = df["absolute_share"].sum()
        if abs(total_share - 1.0) > 0.01:
            logger.warning(f"Injection distribution shares sum to {total_share:.3f}, expected 1.0")

        logger.info(f"Loaded injection distribution for {len(df)} facilities")
        return df

    def load_scenarios(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel, sheet_name="scenarios")
        logger.info(f"Loaded {len(df)} scenarios")
        return df

    def load_mileage_bands(self) -> list[MileageBand]:
        df = pd.read_excel(self.excel, sheet_name="mileage_bands")

        bands = []
        for _, row in df.iterrows():
            band = MileageBand(
                zone=int(row["zone"]),
                mileage_band_min=float(row["mileage_band_min"]),
                mileage_band_max=float(row["mileage_band_max"]),
                circuity_factor=float(row["circuity_factor"]),
                mph=float(row["mph"])
            )
            bands.append(band)

        bands.sort(key=lambda b: b.zone)
        logger.info(f"Loaded {len(bands)} mileage bands")
        return bands

    def load_timing_params(self) -> TimingParams:
        df = pd.read_excel(self.excel, sheet_name="timing_params")

        params = {}
        for _, row in df.iterrows():
            key = str(row["key"]).strip()
            value = float(row["value"])
            params[key] = value

        required_keys = [
            "induction_sort_minutes",
            "middle_mile_crossdock_minutes",
            "middle_mile_sort_minutes",
            "last_mile_sort_minutes"
        ]

        missing = [k for k in required_keys if k not in params]
        if missing:
            raise ValueError(f"Missing required timing params: {missing}")

        timing = TimingParams(
            induction_sort_minutes=params["induction_sort_minutes"],
            middle_mile_crossdock_minutes=params["middle_mile_crossdock_minutes"],
            middle_mile_sort_minutes=params["middle_mile_sort_minutes"],
            last_mile_sort_minutes=params["last_mile_sort_minutes"]
        )

        logger.info(f"Loaded timing params: {timing}")
        return timing

    def load_arc_cpts(self, facilities: dict[str, Facility]) -> list[CPT]:
        if "arc_cpts" not in self.excel.sheet_names:
            logger.info("No arc_cpts sheet found, will generate CPTs from facility outbound windows")
            return []

        df = pd.read_excel(self.excel, sheet_name="arc_cpts")

        cpts = []
        for _, row in df.iterrows():
            origin = str(row["origin"]).strip()
            dest = str(row["dest"]).strip()

            if origin not in facilities:
                raise ValueError(f"arc_cpts references unknown origin facility: {origin}")

            tz = facilities[origin].timezone

            cpt = CPT(
                origin=origin,
                dest=dest,
                cpt_sequence=int(row["cpt_sequence"]),
                cpt_local=parse_time_value(row["cpt_local"]),
                days_of_week=parse_days_of_week(row.get("days_of_week", "")),
                timezone=tz,
                is_active=bool(int(row["active_arc"]))
            )
            cpts.append(cpt)

        logger.info(f"Loaded {len(cpts)} arc CPT overrides")
        return cpts

    def load_service_commitments(self) -> list[ServiceCommitment]:
        df = pd.read_excel(self.excel, sheet_name="service_commitments")

        commitments = []
        for _, row in df.iterrows():
            commitment = ServiceCommitment(
                origin=str(row["origin"]).strip(),
                dest=str(row["dest"]).strip(),
                zone=int(row["zone"]) if pd.notna(row.get("zone")) else None,
                sla_days=int(row["sla_days"]),
                sla_buffer_days=float(row.get("sla_buffer_days", 0)),
                priority_weight=float(row.get("priority_weight", 1.0))
            )
            commitments.append(commitment)

        logger.info(f"Loaded {len(commitments)} service commitments")
        return commitments

    def load_run_settings(self) -> RunSettings:
        df = pd.read_excel(self.excel, sheet_name="run_settings")

        settings = {}
        for _, row in df.iterrows():
            key = str(row["key"]).strip()
            value = row["value"]
            settings[key] = value

        obj_type_str = str(settings.get("objective_type", "weighted_sla")).lower().strip()
        try:
            objective_type = ObjectiveType(obj_type_str)
        except ValueError:
            raise ValueError(
                f"Invalid objective_type: {obj_type_str}. "
                f"Must be one of {[e.value for e in ObjectiveType]}"
            )

        ref_date = settings.get("reference_injection_date")
        if isinstance(ref_date, str):
            ref_date = datetime.fromisoformat(ref_date)
        elif isinstance(ref_date, datetime):
            pass
        else:
            ref_date = datetime(2025, 6, 15)

        ref_time = settings.get("reference_injection_time")
        if ref_time is None:
            ref_time = time(18, 0)  # Default 18:00
        elif isinstance(ref_time, str):
            ref_time = parse_time_value(ref_time)
        elif isinstance(ref_time, time):
            pass
        elif isinstance(ref_time, datetime):
            ref_time = ref_time.time()
        else:
            ref_time = time(18, 0)

        run_settings = RunSettings(
            objective_type=objective_type,
            max_path_touches=int(settings.get("max_path_touches", 4)),
            max_path_atw_factor=float(settings.get("max_path_atw_factor", 1.5)),
            reference_injection_date=ref_date,
            reference_injection_time=ref_time
        )

        logger.info(f"Loaded run settings: {run_settings}")
        return run_settings

    def load_all(self) -> dict:
        facilities = self.load_facilities()

        return {
            "facilities": facilities,
            "zips": self.load_zips(),
            "demand": self.load_demand(),
            "injection_distribution": self.load_injection_distribution(),
            "scenarios": self.load_scenarios(),
            "mileage_bands": self.load_mileage_bands(),
            "timing_params": self.load_timing_params(),
            "arc_cpts": self.load_arc_cpts(facilities),
            "service_commitments": self.load_service_commitments(),
            "run_settings": self.load_run_settings()
        }