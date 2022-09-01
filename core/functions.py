import logging
from multiprocessing.sharedctypes import Value

import psycopg

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
        connection_string = (
            f"Driver={driver};"
            f"Server={database.host};"
            f"Database={database.name};"
            f"UID={database.username};"
            )
        try:
            # we append the password here instead of above for security reasons as we will be logging the connection string in case of errors
            connection = pyodbc.connect(connection_string + f"PWD={database.password};")
            return connection
        except pyodbc.ProgrammingError as e:
            print(_("Error connecting to the %(database_name)s database"))
            return None
        except pyodbc.InterfaceError as e:
            logging.error(f"Error connecting to the {database.name} database on {database.host}:{database.port}. Connection string: {connection_string}")
            return None
    
    if postgresql_regex.search(dbms_name):
        connection_string = f"dbname={database.name.lower()} user={database.username} host={database.host} "
        
        try:
            # we append the password here instead of above for security reasons as we will be logging the connection string in case of errors
            connection = psycopg.connect(connection_string + f"password={database.password}")
            return connection
        except psycopg.OperationalError as e:
            logging.error(f"Error connecting to the Postgres database {database.name} on {database.host}:{database.port}. Connection string: '{connection_string}'. Error: {str(e)}")
            return None

def get_create_temporary_table_query(database, temporary_table_name, columns_and_datatypes_string):
    """
    Get the query to create a temporary table based on the database
    """
    dbms_name = database.dbms_version.dbms.name

    if sql_server_regex.search(dbms_name):
        return f"CREATE ##{temporary_table_name} {columns_and_datatypes_string}"

    if postgresql_regex.search(dbms_name):
        return f"CREATE TEMP TABLE {temporary_table_name} {columns_and_datatypes_string}"

def get_temporary_table_name(database, temporary_table_name: str):
    """
    Get the query to create a temporary table based on the database
    """
    dbms_name = database.dbms_version.dbms.name
    string = ''

    if sql_server_regex.search(dbms_name):
        return f"##{temporary_table_name}"

    if postgresql_regex.search(dbms_name):
        return f"{temporary_table_name}"

def get_dbms_booleans(database) -> dict:
    dbms_name = database.dbms_version.dbms.name

    is_sqlserver_db, is_postgres_db, is_mysql_db = False, False, False

    if sql_server_regex.search(dbms_name):
        is_sqlserver_db = True
    elif postgresql_regex.search(dbms_name):
        is_postgres_db = True
    elif 0:
        is_mysql_db = True

    return {
        "is_sqlserver_db": is_sqlserver_db, 
        "is_postgres_db": is_postgres_db, 
        "is_mysql_db": is_mysql_db
    }

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

def get_column_datatype(is_postgres, is_sqlserver, is_mysql, data_type_string: str):
    if is_postgres:
        if data_type_string.lower() in set([ 'timestamp with time zone', 'timestamp without time zone', 'double_precision' ]):
            return data_type_string.split(' ')[0]
        if data_type_string in set( [ 'character varying' ] ):
            return 'varchar'
        if data_type_string in set( ['character'] ):
            return 'char'
    
    return data_type_string