"""
Write model results to Excel output file.
"""
from pathlib import Path

import pandas as pd

from .utils import setup_logging

logger = setup_logging()


def write_outputs(reports: dict[str, pd.DataFrame], output_path: str) -> None:
    """Write all reports to an Excel file with multiple sheets."""
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        sheet_order = ["summary", "od_demand", "feasible_paths", "sla_miss_detail"]

        for sheet_name in sheet_order:
            if sheet_name in reports:
                df = reports[sheet_name]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(df.columns):
                    max_len = max(
                        df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                        len(col)
                    ) + 2
                    worksheet.set_column(i, i, min(max_len, 50))

        for sheet_name, df in reports.items():
            if sheet_name not in sheet_order:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info(f"Wrote output to {output_path}")