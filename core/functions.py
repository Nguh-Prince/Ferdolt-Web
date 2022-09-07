import logging
from multiprocessing.sharedctypes import Value

from cryptography.fernet import Fernet

from django.db.utils import IntegrityError

import psycopg
from core.exceptions import InvalidDatabaseConnectionParameters

from flux import models as flux_models
from ferdolt import models as ferdolt_models
from ferdolt_web.settings import FERNET_KEY

import re
import pyodbc
from django.utils.translation import gettext as _

import datetime as dt
import json

sql_server_regex = re.compile("sql\s*server", re.I)
postgresql_regex = re.compile("postgres", re.I)

def encrypt(object, encoding='utf-8'):
    f = Fernet(FERNET_KEY)

    string = str(object)
    string = string.encode(encoding)

    token = f.encrypt(string)

    return ( token, token.decode(encoding) )

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
        return f"CREATE TABLE #{temporary_table_name} {columns_and_datatypes_string}"

    if postgresql_regex.search(dbms_name):
        return f"CREATE TEMP TABLE {temporary_table_name} {columns_and_datatypes_string}"

def get_temporary_table_name(database, temporary_table_name: str):
    """
    Get the query to create a temporary table based on the database
    """
    dbms_name = database.dbms_version.dbms.name
    string = ''

    if sql_server_regex.search(dbms_name):
        return f"#{temporary_table_name}"

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

def get_table_foreign_key_references(table: ferdolt_models.Table):
    is_postgres_db, is_sqlserver_db, is_mysql_db = False, False, False

    database = table.schema.database

    connection = get_database_connection(database)

    if connection:
        cursor = connection.cursor()

        dbms_booleans = get_dbms_booleans(database)

        query = None

        if dbms_booleans["is_sqlserver_db"]:
            query = f"""
                    select OBJECT_NAME(fks.referenced_object_id) table_name, 
                    sc2.name schema_name, COL_NAME(fks.referenced_object_id, fc2.referenced_column_id) column_name, 
                    COL_NAME(fks.parent_object_id, fc.parent_column_id) referencing_column 
                    FROM sys.foreign_keys fks 
                    LEFT JOIN sys.tables tab ON referenced_object_id = tab.object_id 
                    LEFT JOIN sys.schemas sc2 ON tab.schema_id = sc2.schema_id 
                    LEFT JOIN sys.foreign_key_columns fc ON fks.object_id = fc.constraint_object_id 
                    LEFT JOIN sys.foreign_key_columns fc2 ON fks.object_id = fc2.constraint_object_id
                    WHERE fks.parent_object_id=object_id('{table.schema.name}.{table.name}')
            """
        if query:
            rows = cursor.execute(query)
            columns = [ column[0] for column in cursor.description ]

            for row in rows:
                record = dict( zip( columns, row ) )

                try:
                    referenced_column = ferdolt_models.Column.objects.get( table__schema__database=database, table__name__iexact=record["table_name"], 
                    name__iexact=record["column_name"] )

                    try:
                        try:
                            referencing_column = ferdolt_models.Column.objects.get( table=table, name__iexact=record["referencing_column"] ) 
                            constraint = ferdolt_models.ColumnConstraint.objects.get_or_create( column=referencing_column, 
                            is_foreign_key=True, defaults={'is_primary_key': False} )

                            constraint
                            constraint[0].references = referenced_column
                            constraint[0].save()

                            logging.info(f"Successfully { 'created' if constraint[1] else 'modified' } the foreign key constraint")
                        except ferdolt_models.ColumnConstraint.MultipleObjectsReturned as e:
                            logging.error(f"Multiple foreign key constraints on the {referencing_column.name.lower()} column")

                    except ferdolt_models.Column.DoesNotExist as e:
                        logging.error(f"Couldn't find the {record['column_name']} column in the {record['table_name']} table")
                    
                    except IntegrityError as e:
                        logging.error(f"Error adding foreign key constraint on {table.name}{record['referencing_column']} to {record['table_name']}.{record['column_name']}. Error: {str(e)}")

                    except ferdolt_models.Column.MultipleObjectsReturned as e:
                        logging.error(f"Multiple {record['column_name']} columns in the {record['table_name']} table")

                except ferdolt_models.Column.DoesNotExist as e:
                    logging.error(f"Couldn't find the {record['column_name']} column in the {record['table_name']} table")


def get_database_details( database ):
    is_postgres_db, is_sqlserver_db, is_mysql_db = False, False, False

    connection = get_database_connection(database)

    if connection:
        cursor = connection.cursor()

        dbms_name = database.dbms_version.dbms.name

        if sql_server_regex.search(dbms_name):
            query = """
                SELECT T.TABLE_NAME, T.TABLE_SCHEMA, C.COLUMN_NAME, C.DATA_TYPE, 
                C.CHARACTER_MAXIMUM_LENGTH, C.DATETIME_PRECISION, C.NUMERIC_PRECISION, C.IS_NULLABLE, TC.CONSTRAINT_TYPE 
                FROM INFORMATION_SCHEMA.TABLES T LEFT JOIN 
                INFORMATION_SCHEMA.COLUMNS C ON C.TABLE_NAME = T.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA 
                LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
                CU.COLUMN_NAME = C.COLUMN_NAME AND CU.TABLE_NAME = C.TABLE_NAME AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA LEFT JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME 
                WHERE T.TABLE_TYPE = 'BASE TABLE' ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME
            """
            is_sqlserver_db = True

        if postgresql_regex.search(dbms_name):
            query = """
                SELECT T.TABLE_NAME, T.TABLE_SCHEMA, C.COLUMN_NAME, C.DATA_TYPE, 
                C.CHARACTER_MAXIMUM_LENGTH, C.DATETIME_PRECISION, C.NUMERIC_PRECISION, C.IS_NULLABLE, TC.CONSTRAINT_TYPE 
                FROM INFORMATION_SCHEMA.TABLES T LEFT JOIN 
                INFORMATION_SCHEMA.COLUMNS C ON C.TABLE_NAME = T.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA 
                LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
                CU.COLUMN_NAME = C.COLUMN_NAME AND CU.TABLE_NAME = C.TABLE_NAME AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA LEFT JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME 
                WHERE T.TABLE_TYPE = 'BASE TABLE' AND NOT T.TABLE_SCHEMA = 'pg_catalog' AND NOT T.TABLE_SCHEMA = 'information_schema' 
                ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME
            """
            is_postgres_db = True

        if query:
            results = cursor.execute(query)
            columns = [ column[0].lower() for column in cursor.description ]

            dictionary = {}

            for row in results:
                row_dictionary = dict( zip(columns, row) )
                schema_dictionary = dictionary.setdefault(
                    row_dictionary['table_schema'], {}
                )
                table_dictionary = schema_dictionary.setdefault(
                    row_dictionary['table_name'], {}
                )

                column_dictionary = table_dictionary.setdefault(
                row_dictionary['column_name'], {})

                if not column_dictionary:
                    column_dictionary = {
                        'data_type': get_column_datatype(is_postgres=is_postgres_db, is_sqlserver=is_sqlserver_db, 
                            is_mysql=is_mysql_db, data_type_string=row_dictionary['data_type']),
                        'character_maximum_length': row_dictionary['character_maximum_length'],
                        'datetime_precision': row_dictionary['datetime_precision'],
                        'numeric_precision': row_dictionary['numeric_precision'],
                        'constraint_type': set([row_dictionary['constraint_type']]),
                        'is_nullable': row_dictionary['is_nullable'].lower() == 'yes'
                    }
                else:
                    column_dictionary['constraint_type'].add( row_dictionary['constraint_type'] )

                table_dictionary[row_dictionary['column_name']] = column_dictionary

            for schema in dictionary.keys():
                schema_record = ferdolt_models.DatabaseSchema.objects.get_or_create(name=schema.lower(), database=database)[0]
                schema_dictionary = dictionary[schema]

                for table in schema_dictionary.keys():
                    table_record: ferdolt_models.Table = ferdolt_models.Table.objects.get_or_create(schema=schema_record, name=table.lower())[0]
                    table_dictionary = schema_dictionary[table]

                    for column in table_dictionary.keys():
                        column_dictionary = table_dictionary[column]
                        try:
                            column_record = ferdolt_models.Column.objects.get_or_create(table=table_record, name=column)[0]
                            
                            column_record.data_type = column_dictionary['data_type']
                            column_record.datetime_precision = column_dictionary['datetime_precision']
                            column_record.character_maximum_length = column_dictionary['character_maximum_length']
                            column_record.numeric_precision = column_dictionary['numeric_precision']
                            column_record.is_nullable = column_dictionary['is_nullable']
                            column_record.save()

                            column_record.columnconstraint_set.all().delete()

                            for constraint in column_dictionary['constraint_type']:
                                primary_key_regex = re.compile("primary key", re.I)
                                foreign_key_regex = re.compile("foreign key", re.I)
                                
                                if constraint:
                                    if primary_key_regex.search(constraint):
                                        ferdolt_models.ColumnConstraint.objects.create(column=column_record, is_primary_key=True)
                                    
                                    if foreign_key_regex.search(constraint):
                                        ferdolt_models.ColumnConstraint.objects.create(column=column_record, is_foreign_key=True)

                            get_table_foreign_key_references(table_record)
                        except ferdolt_models.Column.MultipleObjectsReturned as e:
                            logging.error(f"Error when trying to get column {column} from table {table}. Error: {str(e)}")
                            breakpoint()


    else:
        raise InvalidDatabaseConnectionParameters("""Error connecting to the database. Check if the credentials are correct or if the database is running""")
