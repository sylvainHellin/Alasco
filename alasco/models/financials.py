"""Financial reporting models for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, field_validator

from alasco.models.base import MonetaryAmount

if TYPE_CHECKING:
    from alasco.client import AlascoClient


def _parse_monetary(value: Any) -> MonetaryAmount | None:
    """Parse monetary amount from various formats."""
    if value is None:
        return None
    if isinstance(value, MonetaryAmount):
        return value
    if isinstance(value, dict):
        return MonetaryAmount(**value)
    return None


class ContractFinancialsAttributes(BaseModel):
    """Financial attributes for a Contract."""

    model_config = ConfigDict(extra="ignore")

    name: str
    project: str  # UUID reference
    contract_unit: str  # UUID reference
    cost_element: str | None = None  # UUID reference to CostElement

    # Budget fields
    initial_budget: MonetaryAmount | None = None
    current_budget: MonetaryAmount | None = None

    # Contract amounts
    main_contract_amount: MonetaryAmount | None = None
    change_orders_amount: MonetaryAmount | None = None
    contract_amount_in_draft: MonetaryAmount | None = None

    # Change orders
    open_change_orders_amount: MonetaryAmount | None = None
    open_change_orders_amount_unweighted: MonetaryAmount | None = None

    # Forecasts
    contract_cost_forecast_worst_case: MonetaryAmount | None = None
    contract_cost_forecast_real_case: MonetaryAmount | None = None

    # Budget deviations
    contract_budget_deviation_worst_case: MonetaryAmount | None = None
    contract_budget_deviation_real_case: MonetaryAmount | None = None

    # Reserves and retentions
    reserves_amount: MonetaryAmount | None = None
    contractual_retentions: MonetaryAmount | None = None
    manual_retentions: MonetaryAmount | None = None

    # Payment info
    approved_amount: MonetaryAmount | None = None
    paid_amount: MonetaryAmount | None = None

    # Other fields
    progress_reported: MonetaryAmount | None = None
    realised_cash_discount: MonetaryAmount | None = None
    realised_cash_discount_paid: MonetaryAmount | None = None
    risk_amount: MonetaryAmount | None = None
    contractual_cost_allocations: MonetaryAmount | None = None
    contractual_discounts: MonetaryAmount | None = None
    contract_planned_receivables_amount: MonetaryAmount | None = None
    contract_realised_receivables_amount: MonetaryAmount | None = None

    @field_validator(
        "initial_budget",
        "current_budget",
        "main_contract_amount",
        "change_orders_amount",
        "contract_amount_in_draft",
        "open_change_orders_amount",
        "open_change_orders_amount_unweighted",
        "contract_cost_forecast_worst_case",
        "contract_cost_forecast_real_case",
        "contract_budget_deviation_worst_case",
        "contract_budget_deviation_real_case",
        "reserves_amount",
        "contractual_retentions",
        "manual_retentions",
        "approved_amount",
        "paid_amount",
        "progress_reported",
        "realised_cash_discount",
        "realised_cash_discount_paid",
        "risk_amount",
        "contractual_cost_allocations",
        "contractual_discounts",
        "contract_planned_receivables_amount",
        "contract_realised_receivables_amount",
        mode="before",
    )
    @classmethod
    def parse_monetary_amount(cls, v: Any) -> MonetaryAmount | None:
        return _parse_monetary(v)


class ContractFinancials(BaseModel):
    """Contract financials resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contract_financials"
    attributes: ContractFinancialsAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def project_id(self) -> str:
        return self.attributes.project

    @property
    def contract_unit_id(self) -> str:
        return self.attributes.contract_unit

    @property
    def cost_element_id(self) -> str | None:
        return self.attributes.cost_element

    @classmethod
    def list(
        cls,
        client: AlascoClient,
        project_ids: list[str] | None = None,
        contract_unit_ids: list[str] | None = None,
    ) -> list[ContractFinancials]:
        """List contract financials from the reporting endpoint."""
        params = {}
        if project_ids:
            params["filter[project.in]"] = ",".join(project_ids)
        if contract_unit_ids:
            params["filter[contract_unit.in]"] = ",".join(contract_unit_ids)
        data = client.get_all("/reporting/contracts/", params)
        return [cls.model_validate(item) for item in data]


class ContractUnitFinancialsAttributes(BaseModel):
    """Financial attributes for a Contract Unit."""

    model_config = ConfigDict(extra="ignore")

    name: str
    project: str  # UUID reference
    cost_element: str | None = None  # UUID reference to CostElement

    # Budget fields
    initial_budget: MonetaryAmount | None = None
    current_budget: MonetaryAmount | None = None

    # Contract amounts
    main_contract_amount: MonetaryAmount | None = None
    change_orders_amount: MonetaryAmount | None = None
    contract_amount_in_draft: MonetaryAmount | None = None

    # Change orders
    open_change_orders_amount: MonetaryAmount | None = None
    open_change_orders_amount_unweighted: MonetaryAmount | None = None

    # Forecasts
    contract_cost_forecast_worst_case: MonetaryAmount | None = None
    contract_cost_forecast_real_case: MonetaryAmount | None = None
    project_cost_forecast_worst_case: MonetaryAmount | None = None
    project_cost_forecast_real_case: MonetaryAmount | None = None

    # Budget deviations
    contract_budget_deviation_worst_case: MonetaryAmount | None = None
    contract_budget_deviation_real_case: MonetaryAmount | None = None
    project_budget_deviation_worst_case: MonetaryAmount | None = None
    project_budget_deviation_real_case: MonetaryAmount | None = None

    # Reserves and retentions
    reserves_amount: MonetaryAmount | None = None
    contractual_retentions: MonetaryAmount | None = None
    manual_retentions: MonetaryAmount | None = None

    # Payment info
    approved_amount: MonetaryAmount | None = None
    paid_amount: MonetaryAmount | None = None

    # Other fields
    progress_reported: MonetaryAmount | None = None
    realised_cash_discount: MonetaryAmount | None = None
    realised_cash_discount_paid: MonetaryAmount | None = None
    risk_amount: MonetaryAmount | None = None
    contractual_cost_allocations: MonetaryAmount | None = None
    contractual_discounts: MonetaryAmount | None = None
    contract_planned_receivables_amount: MonetaryAmount | None = None
    contract_realised_receivables_amount: MonetaryAmount | None = None

    @field_validator(
        "initial_budget",
        "current_budget",
        "main_contract_amount",
        "change_orders_amount",
        "contract_amount_in_draft",
        "open_change_orders_amount",
        "open_change_orders_amount_unweighted",
        "contract_cost_forecast_worst_case",
        "contract_cost_forecast_real_case",
        "project_cost_forecast_worst_case",
        "project_cost_forecast_real_case",
        "contract_budget_deviation_worst_case",
        "contract_budget_deviation_real_case",
        "project_budget_deviation_worst_case",
        "project_budget_deviation_real_case",
        "reserves_amount",
        "contractual_retentions",
        "manual_retentions",
        "approved_amount",
        "paid_amount",
        "progress_reported",
        "realised_cash_discount",
        "realised_cash_discount_paid",
        "risk_amount",
        "contractual_cost_allocations",
        "contractual_discounts",
        "contract_planned_receivables_amount",
        "contract_realised_receivables_amount",
        mode="before",
    )
    @classmethod
    def parse_monetary_amount(cls, v: Any) -> MonetaryAmount | None:
        return _parse_monetary(v)


class ContractUnitFinancials(BaseModel):
    """Contract Unit financials resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contract_unit_financials"
    attributes: ContractUnitFinancialsAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def project_id(self) -> str:
        return self.attributes.project

    @property
    def cost_element_id(self) -> str | None:
        return self.attributes.cost_element

    @classmethod
    def list(
        cls,
        client: AlascoClient,
        project_ids: list[str] | None = None,
    ) -> list[ContractUnitFinancials]:
        """List contract unit financials from the reporting endpoint."""
        params = {}
        if project_ids:
            params["filter[project.in]"] = ",".join(project_ids)
        data = client.get_all("/reporting/contract_units/", params)
        return [cls.model_validate(item) for item in data]
