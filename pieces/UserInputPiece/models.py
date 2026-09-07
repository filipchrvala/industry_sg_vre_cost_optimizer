from pydantic import BaseModel, Field

try:
    from common.onedata_models import OneDataSecretsModel, RunIdInputMixin
except ModuleNotFoundError:
    from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin



class InputModel(RunIdInputMixin):
    load_csv: str = Field(
        description="Path to historical load CSV (datetime + load_kw; price_eur_per_kwh optional)"
    )
    prices_csv: str = Field(
        default="",
        description=(
            "Optional path to a prices CSV (datetime + price_eur_per_kwh). "
            "If omitted and load_csv has no price column, UserInputPiece pulls "
            "OKTE day-ahead prices for the same datetime horizon as the load."
        ),
    )
    scenario_yaml: str = Field(description="Path to scenario YAML")


class SecretsModel(OneDataSecretsModel):
    pass


class OutputModel(BaseModel):
    message: str
    load_csv: str
    scenario_yaml: str
    run_id: str = ""
    user_input_summary_json: str = Field(
        default="",
        description="JSON with price source, merge mode and the datetime horizon actually used.",
    )
