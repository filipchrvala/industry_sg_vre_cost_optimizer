from typing import Optional

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    forecast_csv_path: str = Field(
        description="Path to UC3.4 InferencePiece forecast CSV (final_forecast / PVOUT)."
    )
    load_csv: Optional[str] = Field(
        default=None,
        description=(
            "Load CSV to align against. The forecast covers daylight only, because "
            "preprocessing drops night rows; when this is set the profile is "
            "reindexed onto the load timestamps with zeros at night, which is what "
            "BatterySimPiece requires."
        ),
    )
    irradiance_scale_factor: float = Field(
        default=1.0,
        description="Scale from ShmuCalibrationPiece; 1.0 leaves the forecast untouched.",
    )


class OutputModel(BaseModel):
    message: str
    virtual_solar_csv: str = Field(description="virtual_solar.csv for BatterySimPiece")
    coverage_report_json: str = Field(
        description="Row counts, alignment result and the applied scale factor."
    )
