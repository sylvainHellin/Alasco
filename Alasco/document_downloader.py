import requests
import os
import pandas as pd
from Alasco.data_fetcher import DataFetcher
from datetime import date

class DocumentDownloader:

	def __init__(self, header, verbose = False, download_path = None) -> None:
		self.header = header
		self.verbose = verbose
		self.BASE_URL = "https://api.alasco.de/v1/"
		self.data_fetcher = DataFetcher(header=header, verbose=verbose)
		self.today = date.today()

		if download_path is None:
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

	def get_contract_documents(self, contract_ids:list):
		urls = [self._prepare_url_get_contract_documents(contract_id) for contract_id in contract_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_contract_documents = pd.concat(collection)
		return df_contract_documents
	
	def get_change_order_documents(self, change_order_ids:list):
		urls = [self._prepare_url_get_change_order_documents(change_order_id) for change_order_id in change_order_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_change_order_documents = pd.concat(collection)
		return df_change_order_documents

	def get_invoice_documents(self, invoice_ids:list):
		urls = [self._prepare_url_get_invoice_documents(invoice_id) for invoice_id in invoice_ids]
		collection = []
		for url in urls:
			doc = self.data_fetcher.get_df(url=url)
			if not doc.empty:
				collection.append(doc)
		df_invoice_documents = pd.concat(collection)
		return df_invoice_documents
	
	def download_documents(self, document_download_links:list, document_names:list, download_path:str|None=None):

		if len(document_download_links) != len(document_names):
			raise IndexError("The lists of document names and document values should have the same length.")

		if download_path is None:
			download_path = self.download_path

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
			