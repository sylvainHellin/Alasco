import pandas as pd
from Alasco.utils import Utils

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

	def consolidate_DataFrames(self, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:

		df = dfs["properties"][["id", "name"]]
		df = df.rename(columns={"id": "property_id", "name": "property_name"})
		
		