from pydantic import BaseModel, Field

try:
    from common.onedata_models import OneDataSecretsModel, RunIdInputMixin
except ModuleNotFoundError:
    from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin


class InputModel(RunIdInputMixin):
    weather_csv_path: str = Field(
        description="Open-Meteo dataset CSV produced by OpenMeteoPVDataPiece."
    )
    scenario_yaml: str = Field(
        description="Scenario YAML providing site.latitude / site.longitude and the PV array."
    )
    max_station_distance_km: float = Field(
        default=120.0,
        description=(
            "Do not calibrate against a station further than this from the site. "
            "Beyond it the measurement says more about the station than the site."
        ),
    )
    enabled: bool = Field(
        default=True,
        description="Set false to skip the SHMU download and run on the model series alone.",
    )


class SecretsModel(OneDataSecretsModel):
    pass


class OutputModel(BaseModel):
    message: str
    calibration_json: str = Field(
        description="Bias report against SHMU measurement plus the applied scale factor."
    )
    calibrated_training_csv: str = Field(
        description=(
            "Open-Meteo features with PVOUT recomputed from measured irradiance, "
            "restricted to the overlap window. Training target for the UC3.4 "
            "error-correction model. Empty file when no measurement is available."
        )
    )
    calibration_available: bool = Field(
        description="True when a usable measured overlap was found."
    )
    irradiance_scale_factor: float = Field(
        description="Multiply modelled irradiance by this to remove the mean measured bias."
    )
