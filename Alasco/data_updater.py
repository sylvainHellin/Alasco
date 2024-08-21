class DataUpdater:

	def __init__(self, header, verbose = False) -> None:
		self.URL_REPORTING = "https://api.alasco.de/v1/reporting/contract_units"
		self.URL_PROJECT = "https://api.alasco.de/v1/projects/"
		self.URL_PROPERTIES = "https://api.alasco.de/v1/properties/"
		self.URL_CONTRACTS = "https://api.alasco.de/v1/contracts/"
		self.URL_CONTRACTORS = "https://api.alasco.de/v1/contractors/"
		self.URL_CONTRACTING_ENTITIES = "https://api.alasco.de/v1/contracting_entities/"
		self.URL_CONTRACT_UNITS = "https://api.alasco.de/v1/contract_units/"
		self.URL_INVOICES = "https://api.alasco.de/v1/invoices/"
		self.header = header
		self.verbose = verbose # set to True if all details needs to be printed
		