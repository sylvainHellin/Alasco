import json
import pandas as pd
from datetime import date

TODAY = date.today()

class Utils:
	def __init__(self) -> None:
		pass
		
	def printResponse(self, response):
		responseJSON = response.json()
		responseSTR = json.dumps(responseJSON, indent=3)
		print(f"Status code: {response.status_code}")
		print(f"Response:\n{responseSTR}")

	def printJSON(self, responseJSON):
		responseSTR = json.dumps(responseJSON, indent=3)
		print(f"Response:\n{responseSTR}")


	def headerParameters(self, token: str, key: str):
		"""
		return the header for the API calls to Alasco
		"""
		headerParameters = {
			"X-API-KEY": key,
			"X-API-TOKEN": token,
		}
		return headerParameters


	def split_list(self, input_list: list, chunk_size: int) -> list:
		"""
		Splits a list into smaller chunks of a specified size.

		Parameters:
			input_list (list): The list to split.
			chunk_size (int): The size of each chunk.

		Returns:
			list: A list of lists, where each sublist is a chunk of the original list.

		Raises:
			ValueError: If chunk_size is less than or equal to 0.
		"""
		if chunk_size <= 0:
			raise ValueError("chunk_size must be greater than 0")
		return [input_list[i*chunk_size:(i+1)*chunk_size] for i in range((len(input_list) + chunk_size - 1) // chunk_size)]

	def get_ids(self, df: pd.DataFrame) -> list:
		"""
		Extracts the 'id' column from a DataFrame and returns it as a list.

		Parameters:
		df (pd.DataFrame): The DataFrame from which to extract the 'id' column.

		Returns:
		list: A list of IDs extracted from the 'id' column of the DataFrame.

		Raises:
		KeyError: If the 'id' column is not found in the DataFrame.
		"""
		try:
			return df["id"].tolist()
		except KeyError:
			raise KeyError("The key 'id' was not found in the DataFrame.")

