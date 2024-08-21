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

def normalizeJSON(JSONdata):
	"""
	convert the data from Json to a Dataframe

	Input:
	- vergabeeinheitenJSON: Vergabeeinheiten in JSON format

	Output:
	- df: Vergabeeinheiten as Dataframe
	"""

	df = pd.json_normalize(data=JSONdata["data"])
	columns = df.columns
	NameDict = {}
	for name in columns:
		rename = name.split("attributes.")[-1]
		NameDict[name] = rename
	df.rename(mapper=NameDict, axis=1, inplace=True)

	return df

def testFloat (x):
	'''
	Test if a x contains "0000" and convert it to float
	'''

	if x is not None:
		try:
			return x[-4:] == "0000"
		except:
			print(f"Error while testing: x = {x}")
	else:
		return False


def JSONtoDF(listJSON):
	"""
	convert a list of Json objects to a consolidated pandas DataFrame

	Input:
	- listJSON: a list of JSON objects

	Output:
	- df: a DataFrame with all JSON objects concatenated
	"""

	df = normalizeJSON(JSONdata=listJSON[0])
	for i in range(1, len(listJSON)):
		dfi = (normalizeJSON(JSONdata=listJSON[i]))
		df = pd.concat(objs=[df, dfi])

	return df

def nameMapping(df):
	"""
	Return the mapping between the projects ID Number and their name

	Input:
	- df: a DataFrame

	Output (tuple):
	- IDtoName: dict of mapping between id and name
	- NametoID: dict of mapping between name and id
	"""

	IDtoName = {}
	NametoID = {}
	for i in range(df[["id", "name"]].shape[0]):
		IDtoName[df["id"].iloc[i]] = df["name"].iloc[i]
		NametoID[df["name"].iloc[i]] = df["id"].iloc[i]

	return IDtoName, NametoID


def addKostengruppe(df: pd.DataFrame, mapping: dict):
	"""
	Add a column "KG" to the dataframe according to the mapping between Vergabeeinheiten and Kostengruppen

	Input:
	- df: the Dataframe to be updated
	- mapping: mapping between Vergabeeinheit and Kostengruppe

	Output:
	- df: updated Dataframe
	"""
	df["KG"] = df["name"].apply(lambda x: mapping[x])

	return df


def addProjectName(df: pd.DataFrame, df_projects: pd.DataFrame):
	"""
	Add a column "project_name" to the dataframe according to the mapping between the project_id and their names

	Input:
	- df: the Dataframe to be updated
	- df_projects: the DataFrame with project id and name
	Output:
	- df: updated Dataframe
	"""
	IDtoName, NametoID = nameMapping(df=df_projects)
	df["project_name"] = df["project"].apply(lambda x: IDtoName[x])

	return df


def addColumn(df: pd.DataFrame, mapping: dict, colName: str, origin: str):
	"""
	Add a column to the dataframe according to the mapping between Project ID and Property Name

	Input:
	- df: the Dataframe to be updated
	- mapping: map between origin and colName
	- colName: Name of the new column
	- origin: name of the existing column name to be used for mapping

	Output:
	- df: updated Dataframe
	"""
	df_update = df.copy()
	df_update[colName] = df_update[origin].apply(lambda x: mapping[x])

	return df_update


def mappingProjectIDtoProperty(dfProperties: pd.DataFrame, dfProjects: pd.DataFrame):
	"""
	Create a dictionary to map a project id to an immobilien's name

	Input:
	- dfImmobilien: a pandas Dataframe where the name and the id of the immobilien are find
	- dfProjects: a pandas Dataframe where the name and the id of the projects are find

	Output:
	- mapIDtoImmobilien: a dict linking an Project ID to an Immoblien id to it's name
	"""

	mapProjectIDtoImmobilien = {}
	for id in dfProjects["id"]:
		propertyID = dfProjects[dfProjects["id"] == id][
			"relationships.property.data.id"
		].iloc[0]
		propertyName = dfProperties[dfProperties["id"] == propertyID]["name"].iloc[0]
		mapProjectIDtoImmobilien[id] = propertyName

	return mapProjectIDtoImmobilien


def mappingProjectIDtoName(dfProjects: pd.DataFrame):
	"""
	Create a dictionary to map a project id to an immobilien's name

	Input:
	- dfImmobilien: a pandas Dataframe where the name and the id of the immobilien are find
	- dfProjects: a pandas Dataframe where the name and the id of the projects are find

	Output:
	- mapIDtoImmobilien: a dict linking an Project ID to an Immoblien id to it's name
	"""

	mapProjectIDtoName = {}
	for id in dfProjects["id"]:
		mapProjectIDtoName[id] = dfProjects[dfProjects["id"] == id]["name"].iloc[0]

	return mapProjectIDtoName


def mappingVEtoKG(VEliste: pd.Series, VEtoKG: None):
	"""
	Create a dictionary to map a project id to an immobilien's name

	Input:
	- dfImmobilien: a pandas Dataframe where the name and the id of the immobilien are find
	- dfProjects: a pandas Dataframe where the name and the id of the projects are find

	Output:
	- mapIDtoImmobilien: a dict linking an Project ID to an Immoblien id to it's name
	"""
	if VEtoKG is None:
		VEtoKG = {
			"1-1": 100,
			"2-2": 200,
			"2-3": 200,
			"3-3": 300,
			"3-4": 400,
			"3-5": 500,
			"3-6": 600,
			"3-9": 300,
			"4-7": 700,
			"5-8": "sonstiges",
			"6-9": "sonstiges",
			"7-1": "sonstiges",
			"8-9": "sonstiges",
			"9-9": "sonstiges",
		}

	mapVEtoKG = {}

	for VE in VEliste:
		try:
			mapVEtoKG[VE] = VEtoKG[VE[:3]]
		except:
			mapVEtoKG[VE] = "sonstiges"
	return mapVEtoKG


# Rename Projects
def extractProjectName(name):
	"""
	Decompose the name, get rid of the part before the first "-", get rid of the part after the last "_"
	"""
	x1 = name.split("- ")[1]
	x2 = x1.split("_")[:-1]
	x3 = "-".join(x2)
	return x3


def addSubTotal(df):
	"""
	Add Sub-total rows to the DataFrame, for each "Immobilien" and "KG"

	Input: df, a Dataframe

	Output: df, the augmented DataFrame
	"""
	df = df.copy()
	Immobilien = df["Immobilien"].unique()
	Kostengruppe = df["KG"].unique()
	df_subtotal = df.iloc[0:0, :]

	for im in Immobilien:
		for KG in Kostengruppe:
			sub_df = df[df["KG"] == KG]
			sub_df = sub_df[sub_df["Immobilien"] == im]
			sub_total = sub_df.iloc[:, 3:].sum(axis=0)
			sub_total["Immobilien"] = im
			sub_total["KG"] = KG
			sub_total["Project Name"] = " Zwischensumme"
			sub_total = pd.DataFrame(data=sub_total).transpose()
			df_subtotal = pd.concat([df_subtotal, sub_total], axis=0)
		# 	break
		# break
	df_subtotal.head()
	df = pd.concat([df, df_subtotal], axis=0)
	return df
