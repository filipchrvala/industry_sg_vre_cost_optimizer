from pydantic import BaseModel, Field

try:
    from common.onedata_models import OneDataSecretsModel, RunIdInputMixin
except ModuleNotFoundError:
    from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin


class InputModel(RunIdInputMixin):
    report_json: str = Field(description="Path to mrk_savings_report.json")
    kpi_results_csv: str = Field(description="Path to kpi_results.csv")
    investment_evaluation_csv: str = Field(description="Path to investment_evaluation.csv")


class SecretsModel(OneDataSecretsModel):
    pass


class OutputModel(BaseModel):
    dashboard_data_json: str
