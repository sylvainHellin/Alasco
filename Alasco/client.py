# Link to the official documentation: https://developer.alasco.de/

from alasco.data_fetcher import DataFetcher
from alasco.data_updater import DataUpdater
from alasco.data_transformer import DataTransformer
from alasco.document_downloader import DocumentDownloader
from alasco.document_uploader import DocumentUploader
from alasco.utils import Utils

class Alasco:
	def __init__(self, token, key, verbose = False, download_path: str | None = None):

		self.verbose = verbose
		
		self.utils = Utils()
		self.header = self.utils.headerParameters(token=token, key=key)

		self.data_transformer = DataTransformer(verbose=verbose)
		self.data_fetcher = DataFetcher(self.header, verbose=verbose)
		self.data_updater = DataUpdater(self.header, verbose=verbose)
		self.document_downloader = DocumentDownloader(header=self.header, verbose=verbose, download_path=download_path)
		self.document_uploader = DocumentUploader(header=self.header, verbose=verbose)


