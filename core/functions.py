import logging
from multiprocessing.sharedctypes import Value

from ferdolt import models as ferdolt_models
from flux import models as flux_models

import re
import pyodbc
from django.utils.translation import gettext as _

import datetime as dt
import json

sql_server_regex = re.compile("sql\s*server", re.I)
postgresql_regex = re.compile("postgres", re.I)

def custom_converter(object):
    if isinstance(object, dt.datetime):
        return object.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if isinstance(object, dt.date):
        return object.strftime("%Y-%m-%d")

    if isinstance(object, dt.time):
        return object.strftime("%H:%M:%S.%f")
    
    return object.__str__()

def get_database_connection(database: ferdolt_models.Database) -> pyodbc.Cursor:
    # if not isinstance( database, ferdolt_models.Database ):
    #     raise ValueError( _("Invalid database passed, expected a %(expected)s object, got a %(gotten)s object instead" % { 'expected': str(ferdolt_models.Database), 'gotten': type(database) }) )

    dbms_name = database.dbms_version.dbms.name

    if sql_server_regex.search(dbms_name):
        driver = "{SQL Server Native Client 11.0}"
    
    if postgresql_regex.search(dbms_name):
        driver = "{PostgresQL Unicode}"

    connection_string = (
        f"Driver={driver};"
        f"Server={database.host};"
        f"Database={database.name};"
        f"UID={database.username};"
        f"PWD={database.password};"
        )
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except pyodbc.ProgrammingError as e:
        print(_("Error connecting to the %(database_name)s database"))
        return None
    except pyodbc.InterfaceError as e:
        logging.error(f"Error connecting to the {database.name} database on {database.host}:{database.port}. Connection string: {connection_string}")
        return None

def extract_raw(database: ferdolt_models.Database, start_time: dt.datetime, end_time: None):
    connection = get_database_connection(database)
    
    database_dictionary = {}

    if connection:  
        cursor = connection.cursor()

        if not start_time:
            latest_extraction = flux_models.Extraction.objects.filter(extractiondatabase__database=database).last()

            start_time = latest_extraction.start_time if latest_extraction else None
        
        for table in ferdolt_models.Table.objects.filter(schema__database=database):
            schema_dictionary = database_dictionary.setdefault(table.schema.name, {})
            table_results = schema_dictionary.setdefault(table.name, [])

            query = f"""
            SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table.schema.name}.{table.name} 
            { "WHERE last_updated >= ?" if start_time else "" }
            """

            rows = cursor.execute(query, start_time) if start_time else cursor.execute(query)

            columns = [ column[0] for column in cursor.description ]

            for row in rows:
                row_dictionary = dict( zip( columns, row ) )
                table_results.append(row_dictionary)
    else:
        raise ValueError(_("Invalid connection to the %(database)s database" % { 'database': database.name }))

    return database_dictionary

def transform(data, filename: str, format='json'):
    try:
        with open( filename, "a+" ) as _:
            _.write( json.dumps( data, default=custom_converter ) )
            return ( filename, _ )
    except FileNotFoundError as e:
        raise e

def get_query_to_display_foreign_key_referenced_tables(database_record=None):
    return  f"""
            select OBJECT_NAME(parent_object_id) ChildTable, OBJECT_NAME(referenced_object_id) ReferencedTable FROM sys.foreign_keys 
            WHERE parent_object_id=object_id('production.products');
            """