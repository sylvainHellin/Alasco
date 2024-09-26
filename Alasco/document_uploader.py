import requests
from alasco.document_downloader import DocumentDownloader
from alasco.data_fetcher import DataFetcher

class DocumentUploader:

	def __init__(self, header, verbose = False) -> None:
		self.header = header
		self.verbose = verbose # set to True if all details needs to be printed
		self.data_fetcher = DataFetcher(header=self.header, verbose=self.verbose)
		self.document_downloader = DocumentDownloader(header=self.header, verbose=self.verbose)

		# URL API end points
		self.url_upload_contract = "https://api.alasco.de/v1/contracts/{contract_id}/documents/"
		self.url_upload_change_order = "https://api.alasco.de/v1/change_orders/{change_order_id}/documents/"
		self.url_upload_invoice = "https://api.alasco.de/v1/invoices/{invoice_id}/documents/"
		
		# document types
		self.document_types_change_order = ["ATTACHMENT", "AUDITED_CHANGE_ORDER", "CHANGE_ORDER", "CHANGE_ORDER_OFFER", "COVERSHEET_EXTERNAL",\
											"EXTERNAL_CORRESPONDENCE", "INTERNAL_CORRESPONDENCE", "OTHER", "PLANS", "PROTOCOL", "VALUATIONS"]
		self.document_types_invoice = ["ATTACHMENT", "AUDITED_INVOICE", "COVERSHEET_EXTERNAL", "EXTERNAL_CORRESPONDENCE", "INTERNAL_CORRESPONDENCE",\
										"INVOICE", "OTHER", "PAYMENT_CERTIFICATE", "PLANS", "PROTOCOL", "REVISED_INVOICE", "VALUATIONS"]
	
	def _file_to_bytes(self, file_path:str) -> bytes:
		with open(file_path, "rb") as file:
			return file.read()

	def upload_contract(self, **kwargs) -> requests.Response:
		"""
		Upload a contract document to the specified contract.

		Parameters:
		-----------
		**kwargs : dict
			A dictionary containing the following keys:
			- document_type (str): The type of the document. Must be either 'CONTRACT' or 'ATTACHMENT'.
			- file_path (str): The path to the file to be uploaded.
			- file_name (str): The name of the file to be uploaded.
			- contract_id (str): The ID of the contract to which the document will be uploaded.

		Returns:
		--------
		requests.Response
			The response object from the API call.

		Raises:
		-------
		ValueError
			If any of the required parameters (document_type, file_path, file_name, contract_id) are missing.
			If the document_type is not 'CONTRACT' or 'ATTACHMENT'.
		"""

		# unpack kwargs
		document_type = kwargs.get("document_type")
		file_path = kwargs.get("file_path")
		file_name = kwargs.get("file_name")
		contract_id = kwargs.get("contract_id")

		# Check if required parameters are provided
		if not all([document_type, file_path, file_name, contract_id]):
			raise ValueError("Missing required parameters. Please provide document_type, file_path, file_name, and contract_id")

		# Check doc types corrects
		if document_type != "CONTRACT" and document_type != "ATTACHMENT":
			raise ValueError(f"The document_type must to be either 'CONTRACT' or 'ATTACHMENT'.\nYou specified: {document_type}.")
		
		# Prepare argument for the API call
		url = self.url_upload_contract.format(contract_id=contract_id)
		file_content = self._file_to_bytes(file_path=file_path)
		files = {
			"upload": (file_name, file_content)
		}
		headers = self.header
		data = {
			"document_type": document_type
		}

		# Make the call
		response = requests.post(
			url = url,
			data=data,
			headers=headers,
			files=files
		)
		
		return response

	def upload_change_order(self, **kwargs) -> requests.Response:
		"""
		Upload a change order document to the specified change order.

		Parameters:
		-----------
		**kwargs : dict
			A dictionary containing the following keys:
			- document_type (str): The type of the document. 
			- file_path (str): The path to the file to be uploaded.
			- file_name (str): The name of the file to be uploaded.
			- change_order_id (str): The ID of the change order to which the document will be uploaded.

		Returns:
		--------
		requests.Response
			The response object from the API call.

		Raises:
		-------
		ValueError
			If any of the required parameters (document_type, file_path, file_name, change_order_id) are missing.
			If the document_type does not match any of the acceptable types
		"""

		# unpack kwargs
		document_type = kwargs.get("document_type")
		file_path = kwargs.get("file_path")
		file_name = kwargs.get("file_name")
		change_order_id = kwargs.get("change_order_id")

		# Check if required parameters are provided
		if not all([document_type, file_path, file_name, change_order_id]):
			raise ValueError("Missing required parameters. Please provide document_type, file_path, file_name, and change_order_id")

		# Check doc types corrects
		if document_type not in self.document_types_change_order:
			raise ValueError(f"The document_type does not match any of the acceptable types.\nYou specified: {document_type}.")
		
		# Prepare argument for the API call
		url = self.url_upload_change_order.format(change_order_id=change_order_id)
		file_content = self._file_to_bytes(file_path=file_path)
		files = {
			"upload": (file_name, file_content)
		}
		headers = self.header
		data = {
			"document_type": document_type
		}

		# Make the call
		response = requests.post(
			url = url,
			data=data,
			headers=headers,
			files=files
		)
		
		if self.verbose:
			print(response)
			
		return response

	def upload_invoice(self, **kwargs) -> requests.Response:
		"""
		Upload an invoice document to the specified invoice.

		Parameters:
		-----------
		**kwargs : dict
			A dictionary containing the following keys:
			- document_type (str): The type of the document. 
			- file_path (str): The path to the file to be uploaded.
			- file_name (str): The name of the file to be uploaded.
			- invoice_id (str): The ID of the invoice to which the document will be uploaded.

		Returns:
		--------
		requests.Response
			The response object from the API call.

		Raises:
		-------
		ValueError
			If any of the required parameters (document_type, file_path, file_name, invoice_id) are missing.
			If the document_type does not match any of the acceptable types
		"""

		# unpack kwargs
		document_type = kwargs.get("document_type")
		file_path = kwargs.get("file_path")
		file_name = kwargs.get("file_name")
		invoice_id = kwargs.get("invoice_id")

		# Check if required parameters are provided
		if not all([document_type, file_path, file_name, invoice_id]):
			raise ValueError("Missing required parameters. Please provide document_type, file_path, file_name, and invoice_id")

		# Check doc types corrects
		if document_type not in self.document_types_invoice:
			raise ValueError(f"The document_type does not match any of the acceptable types.\nYou specified: {document_type}.")
		
		# Prepare argument for the API call
		url = self.url_upload_invoice.format(invoice_id=invoice_id)
		file_content = self._file_to_bytes(file_path=file_path)
		files = {
			"upload": (file_name, file_content)
		}
		headers = self.header
		data = {
			"document_type": document_type
		}

		# Make the call
		response = requests.post(
			url = url,
			data=data,
			headers=headers,
			files=files
		)
		
		if self.verbose:
			print(response)
			
		return response