"""
alasco - A Python module to facilitate interaction with the Alasco API.

This is an alpha version for submodules client, data_fetcher, data_transformer, document_downloader and utils. 
Submodules document_uploader and data_updater are placeholders for now and will be implemented in the future.

Changelog zur Version 0.0.5:
- changed wrong implementation of document_downloader.download_change_orders.
- updated the verbose mode of the document_downloader sub-module.
- added method to download all documents from a project: document_downloader.batch_download_documents.

Author: sylvain hellin
Version: 0.0.6
"""

__version__ = "0.0.6"
__author__ = "sylvain hellin"

# Import key classes or functions
from .client import Alasco