from datetime import datetime
from sqlite3 import NotSupportedError
import pyodbc
from core.functions import get_database_connection

from django.utils.translation import gettext as _

import logging

from ferdolt import models as ferdolt_models
from flux import models as flux_models

class Node:
    def __init__(self, database: ferdolt_models.Database=None, path_to_pentaho=None, **kwargs):
        try:
            self.target_database_connection = get_database_connection(database)
        except ValueError:
            self.target_database_connection = None
            logging.info( _("Invalid database passed, set the target_database_connection to None") )
    
    def extract(self, use_pentaho=True, **kwargs):
        if use_pentaho:

            pass
        else:
            if not self.target_database_connection:
                raise NotSupportedError( "Your database connection is invalid" )
            
            start_time = flux_models.Extraction.last()
            start_time = start_time if start_time else None

            cursor = self.target_database_connection.cursor()
            