"""
Output writer: write model results to Excel file.
"""
from pathlib import Path

import pandas as pd

from .utils import setup_logging

logger = setup_logging()


def write_outputs(
        reports: dict[str, pd.DataFrame],
        output_path: str
) -> None:
    """
    Write all reports to an Excel file.

    Args:
        reports: Dictionary of sheet_name -> DataFrame
        output_path: Path to output Excel file
    """
    # Ensure output directory exists
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write to Excel with multiple sheets
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        # Write sheets in specific order
        sheet_order = [
            "summary",
            "od_demand",
            "feasible_paths",
            "path_timing_detail",
            "sla_miss_detail"
        ]

        for sheet_name in sheet_order:
            if sheet_name in reports:
                df = reports[sheet_name]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-adjust column widths
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(df.columns):
                    max_len = max(
                        df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                        len(col)
                    ) + 2
                    worksheet.set_column(i, i, min(max_len, 50))

        # Write any remaining sheets not in the order list
        for sheet_name, df in reports.items():
            if sheet_name not in sheet_order:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info(f"Wrote output to {output_path}")


def write_csv_outputs(
        reports: dict[str, pd.DataFrame],
        output_dir: str
) -> None:
    """
    Write all reports to separate CSV files.

    Args:
        reports: Dictionary of sheet_name -> DataFrame
        output_dir: Directory to write CSV files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for sheet_name, df in reports.items():
        csv_path = output_path / f"{sheet_name}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Wrote {csv_path}")