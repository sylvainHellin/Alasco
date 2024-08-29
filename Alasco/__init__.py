"""
alasco - A Python module to facilitate interaction with the Alasco API.

This is an alpha version for submodules client, data_fetcher, data_transformer, document_downloader and utils. 
Submodules document_uploader and data_updater are placeholders for now and will be implemented in the future.

Changelog zur Version 0.0.7:
- moved get_documents methods to data_fetcher
- moved _prepaer_urls methods to utils
- added upload_document methods 

Author: sylvain hellin
Version: 0.0.7
"""

__version__ = "0.0.7"
__author__ = "sylvain hellin"

# Import key classes or functions
from .client import Alasco