import requests
import os
import pandas as pd
import re
from alasco.data_fetcher import DataFetcher
from datetime import date

class DocumentDownloader:
	"""
	Initializes the DocumentDownloader class.

	Args:
		header (dict): The header containing API key and token for authentication.
		verbose (bool, optional): If True, enables verbose mode. Defaults to False.
		download_path (str | None, optional): The path where documents will be downloaded. 
			If None, defaults to an empty string. If "standard", defaults to "outputs/{today's date}". Defaults to None.

	Attributes:
		header (dict): Stores the API key and token for authentication.
		verbose (bool): Indicates if verbose mode is enabled.
		BASE_URL (str): The base URL for the Alasco API.
		data_fetcher (DataFetcher): An instance of DataFetcher to fetch data from the API.
		today (date): The current date.
		download_path (str): The path where documents will be downloaded.
	"""

	def __init__(self, header, verbose = False, download_path: str | None = None) -> None:
		self.header = header
		self.verbose = verbose
		self.BASE_URL = "https://api.alasco.de/v1/"
		self.data_fetcher = DataFetcher(header=header, verbose=verbose)
		self.today = date.today()

		if download_path is None:
			download_path = ""
		elif download_path == "standard":
			download_path = f"outputs/{self.today}"
			
		self.download_path = download_path
		
		if not os.path.exists(self.download_path):
			os.makedirs(self.download_path)
			print(f"{self.download_path} was created")
		else:
			print(f"{self.download_path} already exist")

	def _prepare_url_get_contract_documents(self, contract_id:str):
		url = self.BASE_URL
		url += f"contracts/{contract_id}/documents/"
		return url

	def _prepare_url_get_change_order_documents(self, change_order_id:str):
		url = self.BASE_URL
		url += f"change_orders/{change_order_id}/documents/"
		return url

	def _prepare_url_get_invoice_documents(self, invoice_id:str):
		url = self.BASE_URL
		url += f"invoices/{invoice_id}/documents/"
		return url

	def get_contract_documents(self, contract_ids: list) -> pd.DataFrame:
		"""
		Fetches and concatenates contract documents for the given contract IDs.

		Args:
			contract_ids (list): A list of contract IDs for which to fetch documents.

		Returns:
			pd.DataFrame: A DataFrame containing the concatenated contract documents.

		Example:
			>>> contract_ids = ["123", "456"]
			>>> df_contract_documents = downloader.get_contract_documents(contract_ids)
			>>> print(df_contract_documents)
		"""
		urls = [self._prepare_url_get_contract_documents(contract_id) for contract_id in contract_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_contract_documents = pd.concat(collection)
		return df_contract_documents
		
	def get_change_order_documents(self, change_order_ids: list) -> pd.DataFrame:
		"""
		Fetches and concatenates change order documents for the given change order IDs.

		Args:
			change_order_ids (list): A list of change order IDs for which to fetch documents.

		Returns:
			pd.DataFrame: A DataFrame containing the concatenated change order documents.

		Example:
			>>> change_order_ids = ["789", "101"]
			>>> df_change_order_documents = downloader.get_change_order_documents(change_order_ids)
			>>> print(df_change_order_documents)
		"""
			
		urls = [self._prepare_url_get_change_order_documents(change_order_id) for change_order_id in change_order_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_change_order_documents = pd.concat(collection)
		return df_change_order_documents

	def get_invoice_documents(self, invoice_ids: list) -> pd.DataFrame:
		"""
		Fetches and concatenates invoice documents for the given invoice IDs.

		Args:
			invoice_ids (list): A list of invoice IDs for which to fetch documents.

		Returns:
			pd.DataFrame: A DataFrame containing the concatenated invoice documents.

		Example:
			>>> invoice_ids = ["111", "222"]
			>>> df_invoice_documents = downloader.get_invoice_documents(invoice_ids)
			>>> print(df_invoice_documents)
		"""
		urls = [self._prepare_url_get_invoice_documents(invoice_id) for invoice_id in invoice_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_invoice_documents = pd.concat(collection)
		return df_invoice_documents
	def download_documents(self, document_download_links: list, document_names: list, download_path: str | None = None):
		"""
		Downloads documents from the provided download links and saves them with the given names.

		Args:
			document_download_links (list): A list of URLs from which to download documents.
			document_names (list): A list of names to save the downloaded documents as.
			download_path (str | None, optional): The path where documents will be downloaded. 
				If None, uses the instance's download_path attribute. Defaults to None.

		Raises:
			IndexError: If the lengths of document_download_links and document_names do not match.

		Example:
			>>> document_download_links = ["https://example.com/doc1", "https://example.com/doc2"]
			>>> document_names = ["doc1.pdf", "doc2.pdf"]
			>>> downloader.download_documents(document_download_links, document_names)
		"""

		# Check if the lengths of the download links and names match
		if len(document_download_links) != len(document_names):
			raise IndexError("The lists of document names and document values should have the same length.")

		# Use the provided download path or the instance's download path
		if download_path is None:
			download_path = self.download_path

		# Create the download directory if it does not exist
		if not os.path.exists(download_path):
			os.makedirs(download_path)
			print(f"Created directory: {download_path}")

		# Download each document and save it with the corresponding name
		for name, link in zip(document_names, document_download_links):
			if self.verbose:
				print(f"Downloading document from {name}")

			response = requests.get(link, headers=self.header)
			if response.status_code == 200:
				file_name = os.path.join(download_path, name)
				with open(file_name, 'wb') as file:
					file.write(response.content)
				if self.verbose:
					print(f"Document saved to {file_name}")
			else:
				print(f"Failed to download document from {link}. Status code: {response.status_code}")

	def _name_contract(self, row: pd.Series, document_type: str | None = None) -> str:
		"""
		Generates a standardized contract name based on the contractor name, contract number, and document type.

		This function removes any characters that are not alphanumeric, underscores, hyphens, parentheses, or spaces
		from the contractor name, contract number, and document type. It then concatenates these cleaned strings
		into a single contract name in the format: 'contractor_name_contract_number_document_type.pdf'.

		Parameters:
		row (pd.Series): A pandas Series containing the columns 'contractor_name', 'contract_number', and 'document_type'.

		Returns:
		str: The standardized contract name.

		Example:
		>>> import pandas as pd
		>>> row = pd.Series({
		...     "contractor_name": "ACME Corp.",
		...     "contract_number": "123/456",
		...     "document_type": "CONTRACT"
		... })
		>>> name_contract(row)
		'ACME_Corp_123456_CONTRACT.pdf'
		"""
		if document_type is None:
			document_type  = "CONTRACT"
		
		contractor_name = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contractor_name"])
		contract_number = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contract_number"])

		contract_name = f"{contractor_name}_{contract_number}_{document_type}.pdf"

		return contract_name
	
	def download_contracts(self, df: pd.DataFrame, document_type: str | None = "CONTRACT", sub_folder: str | None = "contracts") -> None:
		"""
		Downloads contract documents based on the DataFrame, document type, and optional sub-folder. 
		The name of the document is generated using the _name_contract() method. 
		The documents will be downloaded in the sub-folder if provided, else in the main download_path.

		Parameters:
		df (pd.DataFrame): DataFrame containing contract information, including 'contract_id'.
		document_type (str | None): The type of document to download. Defaults to "CONTRACT". If None, downloads all types.
		sub_folder (str | None): Optional sub-folder to save the downloaded documents. If None, uses the default download path.

		Returns:
		None
		"""

		# Extract unique contract IDs from the DataFrame
		contract_ids = df["contract_id"].drop_duplicates().tolist()

		# Fetch contract document links using the extracted contract IDs
		df_contract_links = self.get_contract_documents(contract_ids=contract_ids)

		# Rename columns for clarity
		df_contract_links = df_contract_links.rename(columns={
			"id": "contract_document_id",
			"relationships.contract.data.id": "contract_id", 
			"links.download": "download_link"
		})

		# Select relevant columns from the fetched contract links DataFrame
		df_contract_links = df_contract_links[["contract_document_id", "filename", "contract_id", "download_link", "document_type"]]

		# Merge the original DataFrame with the contract links DataFrame on 'contract_id'
		df_merged = pd.merge(df, df_contract_links, on="contract_id", how="outer")

		# Filter the merged DataFrame by the specified document type, if provided
		if document_type is not None:
			df_merged = df_merged[df_merged["document_type"] == document_type]

		# Extract download links from the filtered DataFrame
		download_links = df_merged["download_link"].tolist()

		# Generate standardized contract names for each row in the original DataFrame
		contract_names = df_merged.apply(lambda row: self._name_contract(row=row, document_type=document_type), axis=1)

		if sub_folder is not None:
			download_path = self.download_path + "/" + sub_folder
		else:
			download_path = self.download_path
			
		# Download the documents using the extracted download links and generated contract names
		self.download_documents(document_download_links=download_links, document_names=contract_names, download_path=download_path)

		return None

	def _name_invoice(self, row: pd.Series, document_type: str | None = None) -> str:
		"""
		Generates a standardized name for an invoice document based on the row data and document type.

		This function removes any characters that are not alphanumeric, underscores, hyphens, parentheses, or spaces
		from the contractor name, contract number, and invoice number. It then concatenates these cleaned strings
		into a single invoice name in the format: 'contractor_name_contract_number_invoice_number_document_type.pdf'.

		Parameters:
		row (pd.Series): A pandas Series containing the columns 'contractor_name', 'contract_number', and 'invoice_number'.
		document_type (str | None): The type of document. Defaults to "INVOICE".

		Returns:
		str: The standardized name for the invoice document.

		Example:
		>>> import pandas as pd
		>>> row = pd.Series({
		...     "contractor_name": "ACME Corp.",
		...     "contract_number": "123/456",
		...     "invoice_number": "789/1011"
		... })
		>>> _name_invoice(row)
		'ACME_Corp_123456_7891011_INVOICE.pdf'
		"""
		if document_type is None:
			document_type = "INVOICE"

		contract_number = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contract_number"])
		contractor_name = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contractor_name"])
		invoice_number = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["invoice_number"])

		invoice_name = f"{contractor_name}_{contract_number}_{invoice_number}_{document_type}.pdf"		
		
		return invoice_name

	def download_invoices(self, df: pd.DataFrame, document_type: str | None = "INVOICE", sub_folder: str | None = "invoices") -> None:
		"""
		Downloads invoice documents based on the DataFrame, document type, and optional sub-folder. 
		The name of the document is generated using the _name_invoice() method. 
		The documents will be downloaded in the sub-folder if provided, else in the main download_path.

		Parameters:
		df (pd.DataFrame): DataFrame containing invoice information, including 'invoice_id'.
		document_type (str | None): The type of document to download. Defaults to "INVOICE". If None, downloads all types.
		sub_folder (str | None): Optional sub-folder to save the downloaded documents. If None, uses the default download path.

		Returns:
		None
		"""

		# Extract unique invoice IDs from the DataFrame
		invoice_ids = df["invoice_id"].drop_duplicates().tolist()

		# Fetch invoice document links using the extracted invoice IDs
		df_invoice_links = self.get_invoice_documents(invoice_ids=invoice_ids)

		# Rename columns for clarity
		df_invoice_links = df_invoice_links.rename(columns={
			"id": "invoice_document_id", 
			"relationships.invoice.data.id": "invoice_id", 
			"links.download": "download_link"})

		# Select relevant columns from the fetched invoice links DataFrame
		df_invoice_links = df_invoice_links[["invoice_document_id", "filename", "invoice_id", "download_link", "document_type"]]

		# Merge the original DataFrame with the invoice links DataFrame on 'invoice_id'
		df_merged = pd.merge(df, df_invoice_links, on="invoice_id", how="outer")

		# Filter the merged DataFrame by the specified document type, if provided
		if document_type is not None:
			df_merged = df_merged[df_merged["document_type"] == document_type]

		# Extract download links from the filtered DataFrame
		download_links = df_merged["download_link"].tolist()

		# Generate standardized invoice names for each row in the original DataFrame
		invoice_names = df_merged.apply(lambda row: self._name_invoice(row=row, document_type=document_type), axis=1)

		if sub_folder is not None:
			download_path = self.download_path + "/" + sub_folder
		else:
			download_path = self.download_path

		# Download the documents using the extracted download links and generated invoice names
		self.download_documents(document_download_links=download_links, document_names=invoice_names, download_path=download_path)

		return None
	
	def _name_change_order(self, row: pd.Series, document_type: str | None = None) -> str:
		"""
		Generates a standardized change order name based on the contractor name, contract number, document type, and change order identifier.

		This function removes any characters that are not alphanumeric, underscores, hyphens, parentheses, or spaces
		from the contractor name, contract number, document type, and change order identifier. It then concatenates these cleaned strings
		into a single change order name in the format: 'contractor_name_contract_number_document_type_change_order_identifier.pdf'.

		Parameters:
		row (pd.Series): A pandas Series containing the columns 'contractor_name', 'contract_number', 'document_type', and 'change_order_identifier'.
		document_type (str | None): The type of document. Defaults to "CHANGE-ORDER".

		Returns:
		str: The standardized change order name.

		Example:
		>>> import pandas as pd
		>>> row = pd.Series({
		...     "contractor_name": "ACME Corp.",
		...     "contract_number": "123/456",
		...     "document_type": "CHANGE-ORDER",
		...     "change_order_identifier": "CO-789"
		... })
		>>> name_change_order(row)
		'ACME_Corp_123456_CHANGE-ORDER_CO-789.pdf'
		"""
		if document_type is None:
			document_type  = "CHANGE-ORDER"
		
		contractor_name = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contractor_name"])
		contract_number = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["contract_number"])
		change_order_identifier = re.sub(r'[^a-zA-Z0-9_\-() ]', '', row["change_order_identifier"])

		change_order_name = f"{contractor_name}_{contract_number}_{document_type}_{change_order_identifier}.pdf"

		return change_order_name

	def download_change_orders(self, df: pd.DataFrame, document_type: str | None = "CHANGE-ORDER", sub_folder: str | None = "change_orders") -> None:
		"""
		Downloads change order documents based on the DataFrame, document type, and optional sub-folder. 
		The name of the document is generated using the _name_change_order() method. 
		The documents will be downloaded in the sub-folder if provided, else in the main download_path.

		Parameters:
		df (pd.DataFrame): DataFrame containing change order information, including 'change_order_id'.
		document_type (str | None): The type of document to download. Defaults to "CHANGE-ORDER". If None, downloads all types.
		sub_folder (str | None): Optional sub-folder to save the downloaded documents. If None, uses the default download path.

		Returns:
		None
		"""

		# Extract unique change order IDs from the DataFrame
		change_order_ids = df["change_order_id"].drop_duplicates().tolist()

		# Fetch change order document links using the extracted change order IDs
		df_change_order_links = self.get_change_order_documents(change_order_ids=change_order_ids)

		# Rename columns for clarity
		df_change_order_links = df_change_order_links.rename(columns={
			"id": "change_order_document_id",
			"relationships.change_order.data.id": "change_order_id",
			"links.download": "download_link"
		})
		# Select relevant columns from the fetched contract links DataFrame
		df_change_order_links = df_change_order_links[["change_order_document_id", "change_order_id", "download_link", "document_type", "filename"]]
		# Merge the original DataFrame with the contract links DataFrame on 'change_order_id'
		df_merged = pd.merge(df, df_change_order_links, on="change_order_id", how="outer")

		# Filter the merged DataFrame by the specified document type, if provided
		if document_type is not None:
			df_merged = df_merged[df_merged["document_type"] == document_type]

		# Extract download links from the filtered DataFrame
		download_links = df_merged["download_link"].tolist()

		# Generate standardized contract names for each row in the original DataFrame
		contract_names = df_merged.apply(lambda row: self._name_change_order(row=row, document_type=document_type), axis=1)

		if sub_folder is not None:
			download_path = self.download_path + "/" + sub_folder
		else:
			download_path = self.download_path

		# Download the documents using the extracted download links and generated contract names
		self.download_documents(document_download_links=download_links, document_names=contract_names, download_path=download_path)

		return None