import pandas as pd
from alasco.utils import Utils

class DataTransformer:
	"""
	A class used to transform JSON data into pandas DataFrames and perform other utility functions.

	Attributes:
		verbose (bool): If True, enables verbose output.
	"""

	def __init__(self, verbose=False) -> None:
		"""
		Initializes the Transform class with optional verbose output.

		Parameters:
			verbose (bool): If True, enables verbose output. Default is False.
		"""
		self.verbose = verbose
		self.utils = Utils()

	def convert_JSON_to_DataFrame(self, JSONdata, verbose: bool = None) -> pd.DataFrame:
		"""
		Converts a single JSON object to a pandas DataFrame.

		Parameters:
			JSONdata (dict): The JSON data to convert. Must contain a 'data' key with the data to be converted.
			verbose (bool): If True, enables verbose output for this function call. Default is None.

		Returns:
			pd.DataFrame: The converted DataFrame.

		Raises:
			KeyError: If the 'data' key is not found in the JSON response.
		"""
		if verbose is not None:
			self.verbose = verbose

		if self.verbose:
			print("Function convertOneJSONtoDataFrame called.")
			print("The data in the json object is:\n")
			self.utils.printJSON(JSONdata)

		if 'data' not in JSONdata:
			raise KeyError("'data' key not found in the JSON response")

		df = pd.json_normalize(data=JSONdata["data"])
		columns = df.columns
		NameDict = {name: name.split("attributes.")[-1] for name in columns}
		df.rename(mapper=NameDict, axis=1, inplace=True)

		return df

	def convert_list_JSON_to_DataFrame(self, listJSON: list) -> pd.DataFrame:
		"""
		Converts a list of JSON objects to a single pandas DataFrame.

		Parameters:
			listJSON (list): A list of JSON data to convert. Each element in the list should be a dictionary representing a JSON object.

		Returns:
			pd.DataFrame: The concatenated DataFrame containing data from all JSON objects in the list. If any JSON object does not contain data, it will be filtered out.

		Raises:
			TypeError: If listJSON is not a list of dictionaries.
		"""
		if not isinstance(listJSON, list) or not all(isinstance(item, dict) for item in listJSON):
			raise TypeError("listJSON must be a list of dictionaries")

		# Convert each JSON object to a DataFrame
		dfs = [self.convert_JSON_to_DataFrame(JSONdata=json) for json in listJSON]

		# Filter out DataFrames that are either empty and drop columns that contain only NaN values
		dfs = [df.dropna(axis=1, how="all") for df in dfs if not df.empty]

		# If there are more than 1 DataFrame, concatenate them
		if len(dfs) > 1:  
			df = pd.concat(dfs, ignore_index=True)
		elif len(dfs) == 1:
			df = dfs[0]
		else:
			df = pd.DataFrame()
		return df
	# INSERT_YOUR_REWRITE_HERE

	def consolidate_core_DataFrames(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
		"""
		Consolidates multiple DataFrames into a single DataFrame by merging them on specific keys. 
		It creates a copy of the input DataFrame to avoid side effects.

		Parameters:
		dfs (dict[str, pd.DataFrame]): A dictionary containing the DataFrames to be consolidated. 
									   The dictionary must contain the following keys: 'properties', 'projects', 
									   'contract_units', 'contracts', 'contractors'.
									   It is reccomended to create this dfs collection using the alasco.data_fetcher.get_all_df() method.

		Returns:
		pd.DataFrame: A consolidated DataFrame containing merged data from the input DataFrames.

		Raises:
		KeyError: If any required key or column is missing in the input dictionary or DataFrames.
		"""
		# Define the required keys that must be present in the input dictionary
		required_keys = ["properties", "projects", "contract_units", "contracts", "contractors"]
		for key in required_keys:
			if key not in dfs:
				raise KeyError(f"'{key}' key not found in the input dictionary")

		# Create copies of the DataFrames to avoid mutating the original dfs
		properties_df = dfs["properties"].copy()
		projects_df = dfs["projects"].copy()
		contract_units_df = dfs["contract_units"].copy()
		contracts_df = dfs["contracts"].copy()
		contractors_df = dfs["contractors"].copy()

		# Check for required columns in the 'properties' DataFrame
		if not all(col in properties_df.columns for col in ["id", "name"]):
			raise KeyError("Required columns missing in 'properties' DataFrame")
		# Select and rename columns in the 'properties' DataFrame
		df_core = properties_df[["id", "name"]]
		df_core = df_core.rename(columns={"id": "property_id", "name": "property_name"})

		# Check for required columns in the 'projects' DataFrame
		if not all(col in projects_df.columns for col in ["id", "name", "relationships.property.data.id"]):
			raise KeyError("Required columns missing in 'projects' DataFrame")
		# Select and rename columns in the 'projects' DataFrame
		projects_df = projects_df[["id", "name", "relationships.property.data.id"]]
		projects_df = projects_df.rename(columns={"id": "project_id", "name": "project_name", "relationships.property.data.id": "property_id"})
		# Merge 'projects' DataFrame with the main DataFrame on 'property_id'
		df_core = pd.merge(df_core, projects_df, on="property_id")

		# Check for required columns in the 'contract_units' DataFrame
		if not all(col in contract_units_df.columns for col in ["id", "name", "relationships.project.data.id"]):
			raise KeyError("Required columns missing in 'contract_units' DataFrame")
		# Select and rename columns in the 'contract_units' DataFrame
		contract_units_df = contract_units_df[["id", "name", "relationships.project.data.id"]]
		contract_units_df = contract_units_df.rename(columns={
			"id": "contract_unit_id", 
			"name": "contract_unit_name", 
			"relationships.project.data.id": "project_id",
			})
		# Merge 'contract_units' DataFrame with the main DataFrame on 'project_id'
		df_core = pd.merge(df_core, contract_units_df, on="project_id")

		# Check for required columns in the 'contracts' DataFrame
		if not all(col in contracts_df.columns for col in ["id", "name", "contract_number", "contract_unit", "contractor"]):
			raise KeyError("Required columns missing in 'contracts' DataFrame")
		# Select and rename columns in the 'contracts' DataFrame
		contracts_df = contracts_df[["id", "name", "contract_number", "contract_unit", "contractor"]]
		contracts_df = contracts_df.rename(columns={
			"id": "contract_id",
			"name": "contract_name",
			"contract_unit": "contract_unit_id",
			"contractor": "contractor_id"
		})
		# Merge 'contracts' DataFrame with the main DataFrame on 'contract_unit_id'
		df_core = pd.merge(df_core, contracts_df, on="contract_unit_id")		

		# Check for required columns in the 'contractors' DataFrame
		if not all(col in contractors_df.columns for col in ["id", "name"]):
			raise KeyError("Required columns missing in 'contractors' DataFrame")
		# Select and rename columns in the 'contractors' DataFrame
		contractors_df = contractors_df[["id", "name"]]
		contractors_df = contractors_df.rename(columns={"id": "contractor_id", "name": "contractor_name"})
		# Merge 'contractors' DataFrame with the main DataFrame on 'contractor_id'
		df_core = pd.merge(df_core, contractors_df, on="contractor_id")
		df_core = df_core.drop_duplicates()
		df_core = df_core.reset_index(drop=True)

		return df_core
		
	def consolidate_invoices_DataFrame(self, df_core: pd.DataFrame, df_invoices: pd.DataFrame) -> pd.DataFrame:
		"""
		Consolidates the invoices DataFrame with the core DataFrame.

		This function merges the provided invoices DataFrame with the core DataFrame on the 'contract_id' column.
		It ensures that the necessary columns are selected and renamed appropriately before merging.

		Parameters:
		df_core (pd.DataFrame): The core DataFrame containing the main data.
		df_invoices (pd.DataFrame): The invoices DataFrame containing invoice data.

		Returns:
		pd.DataFrame: A DataFrame resulting from merging the core DataFrame with the invoices DataFrame on 'contract_id'.
		"""
		# Check for required columns in the 'df_invoices' DataFrame
		required_columns = ["id", "contract", "external_identifier"]
		if not all(col in df_invoices.columns for col in required_columns):
			missing_cols = [col for col in required_columns if col not in df_invoices.columns]
			raise KeyError(f"Required columns missing in 'df_invoices' DataFrame: {missing_cols}")

		df_invoices_copy = df_invoices[required_columns].copy()
		df_invoices_copy = df_invoices_copy.rename(columns={"id": "invoice_id", "contract": "contract_id", "external_identifier": "invoice_number"})
		df_merged = pd.merge(df_core, df_invoices_copy, on="contract_id")
		df_merged = df_merged.drop_duplicates()
		df_merged = df_merged.reset_index(drop=True)

		return df_merged

	def consolidate_change_orders_DataFrame(self, df_core: pd.DataFrame, df_change_orders: pd.DataFrame) -> pd.DataFrame:
		"""
		Consolidates the change orders DataFrame with the core DataFrame.

		This function merges the provided change orders DataFrame with the core DataFrame on the 'contract_id' column.
		It ensures that the necessary columns are selected and renamed appropriately before merging.

		Parameters:
		df_core (pd.DataFrame): The core DataFrame containing the main data.
		df_change_orders (pd.DataFrame): The change orders DataFrame containing change order data.

		Returns:
		pd.DataFrame: A DataFrame resulting from merging the core DataFrame with the change orders DataFrame on 'contract_id'.
		"""
		# Check for required columns in the 'df_change_orders' DataFrame
		required_columns = ["id", "contract", "name", "identifier"]
		if not all(col in df_change_orders.columns for col in required_columns):
			missing_cols = [col for col in required_columns if col not in df_change_orders.columns]
			raise KeyError(f"Required columns missing in 'df_change_orders' DataFrame: {missing_cols}")

		df_change_orders_copy = df_change_orders[required_columns].copy()
		df_change_orders_copy = df_change_orders_copy.rename(columns={
			"id": "change_order_id", 
			"contract": "contract_id",
			"name": "change_order_name",
			"identifier": "change_order_identifier"
			})
		df_merged = pd.merge(df_core, df_change_orders_copy, on="contract_id")
		df_merged = df_merged.drop_duplicates()
		df_merged = df_merged.reset_index(drop=True)
		
		return df_merged

	