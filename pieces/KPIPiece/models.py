from pydantic import BaseModel, Field

from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin


class InputModel(RunIdInputMixin):
    report_json: str = Field(description="Path to mrk_savings_report.json")


class SecretsModel(OneDataSecretsModel):
    pass


class OutputModel(BaseModel):
    message: str
    kpi_results_csv: str
