# Link to the official documentation: https://developer.alasco.de/

from Alasco.data_fetcher import DataFetcher
from Alasco.data_updater import DataUpdater
from Alasco.data_transformer import DataTransformer
from Alasco.document_downloader import DocumentDownloader
from Alasco.document_uploader import DocumentUploader
from Alasco.utils import Utils

class Alasco:
	def __init__(self, token, key, verbose = False):
		# self.header = {
		# 	"X-API-KEY": key,
		# 	"X-API-TOKEN": token,
		# }
		self.verbose = verbose
		
		self.utils = Utils()
		self.data_transformer = DataTransformer(verbose=verbose)

		self.header = self.utils.headerParameters(token=token, key=key)
		self.data_fetcher = DataFetcher(self.header, verbose=verbose)
		self.data_updater = DataUpdater(self.header, verbose=verbose)
		self.document_downloader = DocumentDownloader(header=self.header, verbose=verbose)
		self.document_uploader = DocumentUploader(header=self.header, verbose=verbose)


