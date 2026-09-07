from typing import Optional

from pydantic import BaseModel, Field

try:
    from common.onedata_models import OneDataSecretsModel, RunIdInputMixin
except ModuleNotFoundError:
    from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin


class InputModel(RunIdInputMixin):
    report_json: str = Field(description="Path to mrk_savings_report.json")
    kpi_results_csv: str = Field(description="Path to kpi_results.csv")
    investment_evaluation_csv: str = Field(description="Path to investment_evaluation.csv")
    heatmap_json: Optional[str] = Field(
        default=None,
        description="Sizing heatmap grid from SizingHeatmapPiece.",
    )
    catalog_ranked_recommendation_json: Optional[str] = Field(
        default=None,
        description="Ranked PV module shortlist from CatalogRankerPiece.",
    )
    calibration_json: Optional[str] = Field(
        default=None,
        description=(
            "SHMU calibration report. Shown so the reader can judge how far the "
            "production forecast is backed by measurement."
        ),
    )
    battery_dispatch_csv: Optional[str] = Field(
        default=None,
        description="Interval dispatch from BatterySimPiece, used for the daily profile chart.",
    )
    load_csv: Optional[str] = Field(
        default=None,
        description="Merged load-and-price series from UserInputPiece (shows the prices used).",
    )
    user_input_summary_json: Optional[str] = Field(
        default=None,
        description="UserInputPiece summary: price source, OKTE coverage and datetime horizon.",
    )


class SecretsModel(OneDataSecretsModel):
    pass


class OutputModel(BaseModel):
    dashboard_data_json: str
    dashboard_html: str = Field(
        description="Self-contained HTML dashboard, openable without a server."
    )
