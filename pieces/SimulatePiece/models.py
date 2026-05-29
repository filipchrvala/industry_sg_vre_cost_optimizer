from pydantic import BaseModel, Field


class InputModel(BaseModel):
    load_csv: str = Field(description="Path to historical load CSV")
    scenario_yaml: str = Field(description="Path to scenario YAML")
    virtual_solar_csv: str = Field(description="Path to virtual_solar.csv from SolarSimPiece")
    battery_dispatch_csv: str = Field(description="Path to battery_dispatch.csv from BatterySimPiece")
    battery_summary_csv: str = Field(description="Path to battery_summary.csv from BatterySimPiece")
    ranked_catalog_json: str = Field(default="", description="Optional ranked catalog recommendation JSON")
    inverter_catalog_json: str = Field(default="", description="Optional inverter catalog JSON")
    battery_catalog_json: str = Field(default="", description="Optional battery catalog JSON")
    catalog_manifest_json: str = Field(default="", description="Optional catalog sync manifest JSON")


class OutputModel(BaseModel):
    message: str
    report_json: str
