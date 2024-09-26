import requests
import pandas as pd
from alasco.data_transformer import DataTransformer
from alasco.utils import Utils

class DataFetcher:
	"""
	The DataFetcher class is responsible for fetching data from the Alasco API.

	Args:
		header (dict): The header containing API key and token for authentication.
		verbose (bool, optional): If True, enables verbose mode. Defaults to False.

	Attributes:
		URL_REPORTING (str): The URL for fetching reporting contract units.
		URL_PROJECT (str): The URL for fetching projects.
		URL_PROPERTIES (str): The URL for fetching properties.
		URL_CONTRACTS (str): The URL for fetching contracts.
		URL_CHANGE_ORDERS (str): The URL for fetching change orders.
		URL_CONTRACTORS (str): The URL for fetching contractors.
		URL_CONTRACTING_ENTITIES (str): The URL for fetching contracting entities.
		URL_CONTRACT_UNITS (str): The URL for fetching contract units.
		URL_INVOICES (str): The URL for fetching invoices.
		header (dict): Stores the API key and token for authentication.
		verbose (bool): Indicates if verbose mode is enabled.
		transform (DataTransformer): An instance of DataTransformer to transform the fetched data.
		utils (Utils): An instance of Utils for utility functions.
	"""

	def __init__(self, header, verbose = False) -> None:
		self.URL_REPORTING = "https://api.alasco.de/v1/reporting/contract_units"
		self.URL_PROJECT = "https://api.alasco.de/v1/projects/"
		self.URL_PROPERTIES = "https://api.alasco.de/v1/properties/"
		self.URL_CONTRACTS = "https://api.alasco.de/v1/contracts/"
		self.URL_CHANGE_ORDERS = "https://api.alasco.de/v1/change_orders/"
		self.URL_CONTRACTORS = "https://api.alasco.de/v1/contractors/"
		self.URL_CONTRACTING_ENTITIES = "https://api.alasco.de/v1/contracting_entities/"
		self.URL_CONTRACT_UNITS = "https://api.alasco.de/v1/contract_units/"
		self.URL_INVOICES = "https://api.alasco.de/v1/invoices/"
		self.header = header
		self.verbose = verbose # set to True if all details needs to be printed
		self.transform = DataTransformer()
		self.utils = Utils()

	def get_json(self, url: str, filters: list = [], verbose: bool = None) -> list | RuntimeError:
		"""
		Fetches JSON data from the given URL with optional filters.

		Args:
			url (str): The URL to fetch data from.
			filters (list, optional): A list containing attribute, operation, and filter value for filtering the data. Defaults to an empty list.
			verbose (bool, optional): If provided, overrides the instance's verbose setting. Defaults to None.

		Returns:
			list: A list of JSON responses from the API.

		Raises:
			RuntimeError: If an HTTP error occurs during the API call.
		"""
		# Override the instance's verbose setting if provided
		if verbose is not None:
			self.verbose = verbose

		# Apply filters to the URL if provided
		if filters:
			attribute, operation, filter = filters
			if isinstance(filter, list):
				filter = ",".join(filter)
			url = url + f"?filter[{attribute}.{operation}]={filter}"

		try:
			# Make the API request
			response = requests.get(url=url, headers=self.header)
			# Raise an HTTPError for bad responses
			response.raise_for_status()
			# Parse the JSON response
			response_json = response.json()
			list_responses_json = [response_json]

			# Check for pagination and fetch additional pages if necessary
			nextPageUrl = response_json.get("links", {}).get("next")
			if nextPageUrl is not None:
				nextPageUrl = response_json["links"]["next"]
				nextPagesJSON = self.get_json(url=nextPageUrl)
				list_responses_json += nextPagesJSON

			# Print verbose information if enabled
			if self.verbose:
				print("\n\n")
				print("-" * 50)
				print(f"API call to url: {url[:100]} ...")
				print("\n\n")

			return list_responses_json

		except requests.exceptions.HTTPError as http_err:
			# Handle HTTP errors
			print(f"API call failed to url: {url}")
			raise RuntimeError(f"HTTP error occurred: {http_err}")

		except Exception as err:
			# Handle other errors
			print(f"API call failed to url: {url}")
			raise RuntimeError(f"Other error occurred: {err}")

	def get_df(self, url, filters=None, chunk_size: int = 50):
		"""
		Fetches data from the API and converts it to a DataFrame.

		This method handles API calls with optional filters and can split large filter values into smaller chunks
		to avoid errors with the HTTP request.

		Parameters:
		url (str): The API endpoint URL.
		filters (tuple, optional): A tuple containing the attribute, operation, and filter values.
			Example: ("id", "in", [1, 2, 3, ..., 1000]). Defaults to None.
		chunk_size (int, optional): The size of each chunk of filter values. Defaults to 50.

		Returns:
		pd.DataFrame: The concatenated DataFrame containing data from all API calls.
		"""
		def fetch_and_convert(url, filters):
			listJSON = self.get_json(url=url, filters=filters, verbose=self.verbose)
			return self.transform.convert_list_JSON_to_DataFrame(listJSON=listJSON)

		if filters is not None:
			# Destructure the filters 
			attribute, operation, filter_values = filters

			# If filter_values are lists or tuples, check if they are not too long
			if isinstance(filter_values, (list, tuple)):
				# If they are too long, split them into chunks to avoid getting an error with the HTTP request
				if len(filter_values) > chunk_size:
					filter_chunks = self.utils.split_list(input_list=filter_values, chunk_size=chunk_size)
					dfs = []

					# Chain the API calls with the chunks and concatenate the results
					for index, chunk in enumerate(filter_chunks):
						if self.verbose:
							print(f"API call Nr. {index + 1} from {len(filter_chunks)} to url: {url}")
						chunk_filters = (attribute, operation, chunk)
						df = fetch_and_convert(url, chunk_filters)
						dfs.append(df)

					return pd.concat(dfs, ignore_index=True)

		return fetch_and_convert(url, filters)

	
	def get_projects(self, property_ids: list | None = None, project_name: str | None = None) -> pd.DataFrame:
		"""
		Fetches project data from the API, optionally filtered by property IDs or project name.

		Parameters:
		property_ids (list | None): List of property IDs to filter the projects. If None, fetches all projects.
		project_name (str | None): Project name to filter the projects. If None, fetches all projects.

		Returns:
		pd.DataFrame: DataFrame containing project data.
		"""
		# Determine the filters based on provided parameters
		if project_name is not None:
			filters = ("name", "contains", project_name)
			if self.verbose:
				print(f"Fetching project data for projects which name contains: {project_name}")
		elif property_ids is not None:
			filters = ("property", "in", property_ids)
			if self.verbose:
				print(f"Fetching project data for property_ids: {property_ids}")
		else:
			if self.verbose:
				print("Fetching all project data.")
			filters = None

		# Fetch the project data
		df_projects = self.get_df(url=self.URL_PROJECT, filters=filters)
		
		return df_projects

	def get_properties(self, property_ids: list | None = None, property_name: str | None = None) -> pd.DataFrame:
		"""
		Fetches property data from the API, optionally filtered by property IDs or property name.

		Parameters:
		property_ids (list | None): List of property IDs to filter the properties. If None, fetches all properties.
		property_name (str | None): Property name to filter the properties. If None, fetches all properties.

		Returns:
		pd.DataFrame: DataFrame containing property data.
		"""
		# Determine the filters based on provided parameters
		if property_ids is not None:
			filters = ("id", "in", property_ids)
			if self.verbose:
				print(f"Fetching properties data for property_ids: {property_ids[:3]} ...")
		elif property_name is not None:
			filters = ("name", "contains", property_name)
			if self.verbose:
				print(f"Fetching properties data for property which name contains: {property_name}")
		else:
			if self.verbose:
				print("Fetching all properties data.")
			filters = None

		# Fetch the properties data
		df_properties = self.get_df(url=self.URL_PROPERTIES, filters=filters)

		return df_properties

	def get_reporting(self, project_ids: list) -> pd.DataFrame:
		"""
		Fetches reporting data from the API for the given list of project IDs.

		Parameters:
		project_ids (list): List of project IDs to fetch the reporting data for.

		Returns:
		pd.DataFrame: DataFrame containing the concatenated reporting data for the given project IDs.
		"""
		filters = ("project", "in", project_ids)
		df_reporting = self.get_df(url=self.URL_REPORTING, filters=filters, chunk_size=10)
		return df_reporting
	
	def get_contractors(self, contractor_ids: list | None = None, contractor_name: str | None = None) -> pd.DataFrame:
		"""
		Fetches contractor data from the API. Optionally, filter based on provided contractor IDs or contractor name.

		Parameters:
		contractor_ids (list | None, optional): List of contractor IDs to filter the contractors. If None, fetches all contractors.
		contractor_name (str | None, optional): Contractor name to filter the contractors. If None, fetches all contractors.

		Returns:
		pd.DataFrame: DataFrame containing contractor data.
		"""
		# Determine the filters based on provided parameters
		if contractor_ids is not None:
			filters = ("id", "in", contractor_ids)
			if self.verbose:
				print(f"Fetching contractors with ids in: {contractor_ids[:3]} ...")
				print("-"*50)
			if len(contractor_ids) > 50:
				df_contractors = self.get_df(url=self.URL_CONTRACTORS, filters=filters)
		elif contractor_name is not None:
			filters = ("name", "contains", contractor_name)
			if self.verbose:
				print(f"Fetching contractors which name contains: {contractor_name}")
				print("-"*50)
		else:
			filters = None
			if self.verbose:
				print("Fetching all contractors")
				print("-"*50)

		# Fetch the contractors data
		df_contractors = self.get_df(url=self.URL_CONTRACTORS, filters=filters)

		return df_contractors

	def get_contracts(
			self,
			contract_number: str | None = None,
			cost_center: str | None = None,
			contract_ids: list | None = None,
			contractor_ids: list | None = None,
			contract_unit_ids: list | None = None
			) -> pd.DataFrame:
		"""
		Fetches contract data from the API. Optionally, filter based on provided parameters.

		Parameters:
		contract_number (str | None, optional): Contract number to filter the contracts. If None, fetches all contracts.
		cost_center (str | None, optional): Cost center to filter the contracts. If None, fetches all contracts.
		contract_ids (list | None, optional): List of contract IDs to filter the contracts. If None, fetches all contracts.
		contractor_ids (list | None, optional): List of contractor IDs to filter the contracts. If None, fetches all contracts.
		contract_unit_ids (list | None, optional): List of contract unit IDs to filter the contracts. If None, fetches all contracts.

		Returns:
		pd.DataFrame: DataFrame containing contract data.

		Raises:
		ValueError: If more than one optional parameter is provided.
		"""
		# Check that only one of the optional parameters is not None
		optional_params = [contract_number, cost_center, contract_ids, contractor_ids, contract_unit_ids]
		if sum(param is not None for param in optional_params) > 1:
			raise ValueError("Only one of the optional parameters can be not None.")

		# Determine the filters based on provided parameters
		if contract_number is not None:
			filters = ("contract_number", "exact", contract_number)
			if self.verbose:
				print(f"Fetching contracts with contract number: {contract_number}")
		elif cost_center is not None:
			filters = ("cost_center", "exact", cost_center)
			if self.verbose:
				print(f"Fetching contracts with cost center: {cost_center}")
		elif contract_ids is not None:
			filters = ("id", "in", contract_ids)
			if self.verbose:
				print(f"Fetching contracts with ids in: {contract_ids[:3]} ...")
		elif contractor_ids is not None:
			filters = ("contractor", "in", contractor_ids)
			if self.verbose:
				print(f"Fetching contracts with contractor ids in: {contractor_ids[:3]} ...")
		elif contract_unit_ids is not None:
			filters = ("contract_unit", "in", contract_unit_ids)
			if self.verbose:
				print(f"Fetching contracts with contract unit ids in: {contract_unit_ids[:3]} ...")
		else:
			filters = None
			if self.verbose:
				print("Fetching all contracts")

		# Fetch the contracts data
		df_contracts = self.get_df(url=self.URL_CONTRACTS, filters=filters)

		return df_contracts

	def get_contracting_entities(self, name: str | None = None):
		"""
		Fetches contracting entities from the API. Optionally, filter based on the provided name.

		Parameters:
		name (str | None, optional): Name to filter the contracting entities. If None, fetches all contracting entities.

		Returns:
		pd.DataFrame: DataFrame containing contracting entities data.
		"""
		filters = ("name", "contains", name) if name is not None else None

		if self.verbose:
			if name is not None:
				print(f"Fetching contracting entities which name contains: {name}")
			else:
				print("Fetching all contracting entities.")

		df_contracting_entities = self.get_df(url=self.URL_CONTRACTING_ENTITIES, filters=filters)

		return df_contracting_entities

	def get_contract_units(self, contract_unit_ids: list | None = None, project_ids: list | None = None) -> pd.DataFrame:
		"""
		Fetches contract units from the API. Optionally, filter based on provided contract unit IDs or project IDs.

		Parameters:
		contract_unit_ids (list | None, optional): List of contract unit IDs to filter. If None, fetches all contract units.
		project_ids (list | None, optional): List of project IDs to filter. If None, fetches all contract units.

		Returns:
		pd.DataFrame: DataFrame containing contract units data.
		"""
		if contract_unit_ids is not None:
			filters = ("id", "in", contract_unit_ids)
			if self.verbose:
				print(f"Fetching contract units with ids in: {contract_unit_ids[:3]}...")
		elif project_ids is not None:
			filters = ("project", "in", project_ids)
			if self.verbose:
				print(f"Fetching contract units with project ids in: {project_ids[:3]} ...")
		else:
			filters = None
			if self.verbose:
				print("Fetching all contract units")

		# Fetch the contract units data
		df_contract_units = self.get_df(url=self.URL_CONTRACT_UNITS, filters=filters)

		return df_contract_units

	def get_invoices(self, invoice_ids: list | None = None, contract_ids: list | None = None) -> pd.DataFrame:
		"""
		Fetches invoices from the API. Optionally, filter based on provided invoice IDs or contract IDs.

		Parameters:
		invoice_ids (list | None, optional): List of invoice IDs to filter. If None, fetches all invoices.
		contract_ids (list | None, optional): List of contract IDs to filter. If None, fetches all invoices.

		Returns:
		pd.DataFrame: DataFrame containing invoices data.
		"""
		if invoice_ids is not None:
			filters = ("id", "in", invoice_ids)
			if self.verbose:
				print(f"Fetching invoices with ids in: {invoice_ids[:3]} ...")
		elif contract_ids is not None:
			filters = ("contract", "in", contract_ids)
			if self.verbose:
				print(f"Fetching invoices with contract ids in: {contract_ids[:3]} ...")
		else:
			filters = None
			if self.verbose:
				print("Fetching all invoices")

		# Fetch the invoices data
		df_invoices = self.get_df(url=self.URL_INVOICES, filters=filters)

		return df_invoices
	
	def get_change_orders(self, change_order_ids: list | None = None, contract_ids: list | None = None) -> pd.DataFrame:
		"""
		Fetches change orders from the API. Optionally, filter based on provided change order IDs or contract IDs.

		Parameters:
		change_order_ids (list | None, optional): List of change order IDs to filter. If None, fetches all change orders.
		contract_ids (list | None, optional): List of contract IDs to filter. If None, fetches all change orders.

		Returns:
		pd.DataFrame: DataFrame containing change orders data.

		Raises:
		ValueError: If neither change_order_ids nor contract_ids are provided.
		"""
		if change_order_ids is not None:
			if self.verbose:
				print(f"Fetching change orders with ids in: {change_order_ids[:3]} ...")
			filters = ("id", "in", change_order_ids)
		elif contract_ids is not None:
			if self.verbose:
				print(f"Fetching change orders belonging to contract ids in: {contract_ids[:3]} ...")
			filters = ("contract", "in", contract_ids)
		else:
			raise ValueError("Please provide either a list of change order ids or a list of contract ids.")

		df_change_orders = self.get_df(url=self.URL_CHANGE_ORDERS, filters=filters)

		return df_change_orders
		
	def get_all_df(self, property_name: str | None = None, project_name: str | None = None):
		"""
		Fetches and returns multiple DataFrames based on the provided property name and project name.

		This method sequentially fetches properties, projects, contract units, contracts, contractors, invoices, 
		and change orders from the API, and returns them as a dictionary of DataFrames.

		Parameters:
		property_name (str | None, optional): Property name to filter the properties. If None, fetches all properties.
		project_name (str | None, optional): Project name to filter the projects. If None, fetches all projects.

		Returns:
		dict: A dictionary containing the fetched DataFrames with the following keys:
			- 'properties': DataFrame containing property data.
			- 'projects': DataFrame containing project data.
			- 'contract_units': DataFrame containing contract unit data.
			- 'contracts': DataFrame containing contract data.
			- 'contractors': DataFrame containing contractor data.
			- 'invoices': DataFrame containing invoice data.
			- 'change_orders': DataFrame containing change order data.
		"""
		# Fetch properties and their IDs
		df_properties = self.get_properties(property_name=property_name)
		property_ids = self.utils.get_ids(df_properties)

		# Fetch projects and their IDs
		df_projects = self.get_projects(property_ids=property_ids, project_name=project_name)
		project_ids = self.utils.get_ids(df_projects)

		# Fetch contract units and their IDs
		df_contract_units = self.get_contract_units(project_ids=project_ids)
		contract_unit_ids = self.utils.get_ids(df_contract_units)

		# Fetch contracts and their IDs
		df_contracts = self.get_contracts(contract_unit_ids=contract_unit_ids)
		contract_ids = self.utils.get_ids(df_contracts)

		# Fetch contractor IDs from contracts
		contractor_ids = df_contracts["contractor"].tolist()
		# Filter the None elements (for example, if a contract has been added without entering the contractor)
		contractor_ids = [id for id in contractor_ids if id is not None]

		# Fetch contractors, invoices, and change orders
		df_contractors = self.get_contractors(contractor_ids=contractor_ids)
		df_invoices = self.get_invoices(contract_ids=contract_ids)
		df_change_orders = self.get_change_orders(contract_ids=contract_ids)

		# Collect all DataFrames into a dictionary
		dfs = {
			'properties': df_properties,
			'projects': df_projects,
			'contract_units': df_contract_units,
			'contracts': df_contracts,
			'contractors': df_contractors,
			'invoices': df_invoices,
			'change_orders': df_change_orders
		}

		return dfs
	
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

		if self.verbose:
			print(f"Getting contract documents for {len(contract_ids)} documents with ids : {contract_ids[:3]} ...")

		urls = [self.utils._prepare_url_get_contract_documents(contract_id) for contract_id in contract_ids]
		collection = []
		for url in urls:
			doc = self.get_df(url=url)
			if not doc.empty:
				collection.append(doc)

		# if no document is uploaded, will return an empty DataFrame		
		if not collection:
			return pd.DataFrame()
		else:
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
			
		if self.verbose:
			print(f"Getting change order documents for {len(change_order_ids)} documents with ids : {change_order_ids[:3]} ...")
		urls = [self.utils._prepare_url_get_change_order_documents(change_order_id) for change_order_id in change_order_ids]
		collection = []
		for url in urls:
			doc = self.get_df(url=url)
			if not doc.empty:
				collection.append(doc)

		# if no document is uploaded, will return an empty DataFrame		
		if not collection:
			return pd.DataFrame()
		else:
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
		if self.verbose:
			print(f"Getting invoice documents for {len(invoice_ids)} documents with ids : {invoice_ids[:3]} ...")

		urls = [self.utils._prepare_url_get_invoice_documents(invoice_id) for invoice_id in invoice_ids]
		collection = []
		for url in urls:
			doc = self.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		
		# if no document is uploaded, will return an empty DataFrame		
		if not collection:
			return pd.DataFrame()
		else:
			df_invoice_documents = pd.concat(collection)
			return df_invoice_documents