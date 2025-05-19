from typing import Literal, Optional

import requests
from requests import Response
from pydantic import BaseModel

from .utils import Utils


class Contract(BaseModel):
    name: Optional[str] = None
    contract_unit_id: Optional[str] = None
    id: Optional[str] = None
    contractor_id: Optional[str] = None
    contract_number: Optional[str] = None
    amount_net: Optional[float] = None
    amount_tax: Optional[float] = None
    is_reverse_charge: bool = False
    cost_center: Optional[str] = None
    description: Optional[str] = None
    date_contract: Optional[str] = None
    status: Literal["DRAFT", "COMPLETED", "ORDERED", "PARTIAL_COMPLETED"] = "DRAFT"


class DataUpdater:
    def __init__(self, header, verbose=False) -> None:
        self.URL_REPORTING = "https://api.alasco.de/v1/reporting/contract_units"
        self.URL_PROJECT = "https://api.alasco.de/v1/projects/"
        self.URL_PROPERTIES = "https://api.alasco.de/v1/properties/"
        self.URL_CONTRACTS = "https://api.alasco.de/v1/contracts/"
        self.URL_CONTRACTORS = "https://api.alasco.de/v1/contractors/"
        self.URL_CONTRACTING_ENTITIES = "https://api.alasco.de/v1/contracting_entities/"
        self.URL_CONTRACT_UNITS = "https://api.alasco.de/v1/contract_units/"
        self.URL_INVOICES = "https://api.alasco.de/v1/invoices/"
        self.header = header
        self.verbose = verbose  # set to True if all details needs to be printed
        self.utils = Utils()

    def create_contract(self, contract: Contract) -> Response:
        """Create a new contract in the Alasco system via API.

        This method takes a Contract object and creates a corresponding contract
        in the Alasco system by making a POST request to the contracts endpoint.

        Args:
            contract (Contract): A Contract object containing the contract details.
                Mandatory fields:
                - name: The name of the contract (must be 250 chars or less)
                - contract_unit_id: ID of the contract unit this contract belongs to

                Optional fields:
                - contractor_id: ID of the contractor for this contract
                - contract_number: Contract reference number
                - amount_net: Net amount of the contract in EUR
                - amount_tax: Tax amount of the contract in EUR
                - is_reverse_charge: Whether reverse charge applies (default: False)
                - cost_center: Cost center code/identifier
                - description: Detailed description of the contract
                - date_contract: Contract date in ISO format
                - status: Contract status (DRAFT, COMPLETED, ORDERED, PARTIAL_COMPLETED)

        Returns:
            Response: The HTTP response object from the API request.
                On success, this contains the created contract data.
                On failure, this contains error details.
                If the contract name exceeds 250 characters, returns a Response
                object with a 400 status code and appropriate error message.
        """

        response = requests.Response()
        response.status_code = 400
        if contract.name is None:
            response._content = b'{"errors":[{"detail":"You need to provide a name for the contract."}]}'
            return response
        elif len(contract.name) > 250:
            response._content = b'{"errors":[{"detail":"Contract name exceeds maximum length of 250 characters"}]}'
            return response
        elif contract.contract_unit_id is None:
            response._content = b'{"errors":[{"detail":"You need to provide a contract unit id for the contract."}]}'
            return response
        del response

        # Create the skeletton body with the mandatory fields
        request_body = {
            "data": {
                "attributes": {
                    "name": contract.name,
                    "is_reverse_charge": contract.is_reverse_charge,
                },
                "relationships": {
                    "contract_unit": {
                        "data": {
                            "id": contract.contract_unit_id,
                            "type": "CONTRACT_UNIT",
                        }
                    },
                },
                "type": "CONTRACT",
            }
        }

        # Update the request body with the additional parameters passed as arguments
        if contract.amount_net is not None:
            request_body["data"]["attributes"]["amount_with_vat"] = {
                "currency": "EUR",
                "net": contract.amount_net,
            }
            if contract.amount_tax is not None:
                request_body["data"]["attributes"]["amount_with_vat"]["tax"] = (
                    contract.amount_tax
                )
        if contract.contract_number is not None:
            request_body["data"]["attributes"]["contract_number"] = (
                contract.contract_number
            )
        if contract.cost_center is not None:
            request_body["data"]["attributes"]["cost_center"] = contract.cost_center
        if contract.date_contract is not None:
            request_body["data"]["attributes"]["date_contract"] = contract.date_contract
        if contract.status is not None:
            request_body["data"]["attributes"]["status"] = contract.status
        if contract.description is not None:
            request_body["data"]["attributes"]["description"] = contract.description
        if contract.contractor_id is not None:
            request_body["data"]["relationships"]["contractor"] = {
                "data": {
                    "id": contract.contractor_id,
                    "type": "CONTRACTOR",
                }
            }

        # Send the request
        headers = self.header.copy()
        headers.update({"Content-Type": "application/json"})

        response = requests.post(
            url="https://api.alasco.de/v1/contracts/",
            headers=headers,
            json=request_body,
        )

        return response

    def update_contract(self, contract: Contract) -> Response:
        if contract.name is not None and len(contract.name) > 250:
            response = requests.Response()
            response.status_code = 400
            response._content = b'{"errors":[{"detail":"Contract name exceeds maximum length of 250 characters"}]}'
            return response

        elif contract.id is None:
            response = requests.Response()
            response.status_code = 400
            response._content = (
                b'{"errors":[{"detail":"You need to provide a valid contract id."}]}'
            )
            return response

        # Create the skeletton body with the mandatory fields
        request_body = {"data": {"attributes": {}, "id": contract.id}}

        # Update the request body with the additional parameters passed as arguments
        if contract.name is not None:
            request_body["data"]["attributes"]["name"] = contract.name
        if contract.amount_net is not None:
            request_body["data"]["attributes"]["amount"] = {
                "currency": "EUR",
                "net": contract.amount_net,
            }
            if contract.amount_tax is not None:
                request_body["data"]["attributes"]["amount"]["tax"] = (
                    contract.amount_tax
                )
        if contract.contract_number is not None:
            request_body["data"]["attributes"]["contract_number"] = (
                contract.contract_number
            )
        if contract.cost_center is not None:
            request_body["data"]["attributes"]["cost_center"] = contract.cost_center
        if contract.date_contract is not None:
            request_body["data"]["attributes"]["date_contract"] = contract.date_contract
        if contract.status is not None:
            request_body["data"]["attributes"]["status"] = contract.status
        if contract.description is not None:
            request_body["data"]["attributes"]["description"] = contract.description
        if contract.contractor_id is not None:
            request_body["data"].get("relationships", {})["contractor"] = {
                "data": {
                    "id": contract.contractor_id,
                    "type": "CONTRACTOR",
                }
            }
        if contract.contract_unit_id is not None:
            request_body["data"].get("relationships", {})["contract_unit"] = {
                "data": {"id": contract.contract_unit_id, "type": "CONTRACT_UNIT"}
            }

        # Send the request
        headers = self.header.copy()
        headers.update({"Content-Type": "application/json"})

        ######################################## DEBUG
        import json

        print(f"https://api.alasco.de/v1/contracts/{contract.id}/")
        print(json.dumps(request_body, indent=2))
        ######################################## DEBUG

        response = requests.patch(
            url=f"https://api.alasco.de/v1/contracts/{contract.id}/",
            headers=headers,
            json=request_body,
        )
        return response
