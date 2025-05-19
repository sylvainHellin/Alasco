"""
alasco - A Python module to facilitate interaction with the Alasco API.

This is an alpha version for submodules client, data_fetcher, data_transformer, document_downloader and utils.
Submodules document_uploader and data_updater are placeholders for now and will be implemented in the future.

Changelog to Version 0.0.8:
- added new methods to create and update contracts in data_updater

TODO:
- N.A.

Author: sylvain hellin
Version: 0.0.9
"""

__version__ = "0.0.9"
__author__ = "sylvain hellin"

# Import key classes or functions
from .client import Alasco
from .data_updater import Contract

__all__ = ["Alasco", "Contract"]
