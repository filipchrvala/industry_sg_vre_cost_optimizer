from pydantic import BaseModel, Field


class InputModel(BaseModel):
    report_json: str = Field(description="Path to mrk_savings_report.json")
    kpi_results_csv: str = Field(description="Path to kpi_results.csv")
    investment_evaluation_csv: str = Field(description="Path to investment_evaluation.csv")


class OutputModel(BaseModel):
    dashboard_data_json: str
