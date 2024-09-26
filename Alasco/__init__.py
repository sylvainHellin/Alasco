"""
alasco - A Python module to facilitate interaction with the Alasco API.

This is an alpha version for submodules client, data_fetcher, data_transformer, document_downloader and utils. 
Submodules document_uploader and data_updater are placeholders for now and will be implemented in the future.

Changelog zur Version 0.0.7:
- moved get_documents methods to data_fetcher
- moved _prepaer_urls methods to utils
- add .drop_duplicates() after consolidating data in data_transformer
- added upload_document methods 

TODO:
- N.A.

Author: sylvain hellin
Version: 0.0.8
"""

__version__ = "0.0.8"
__author__ = "sylvain hellin"

# Import key classes or functions
from .client import Alasco