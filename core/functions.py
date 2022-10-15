import logging
from sqlite3 import ProgrammingError
import time

from cryptography.fernet import Fernet

from django.db.models import F, Q
from django.db.utils import IntegrityError

import psycopg
from core.exceptions import InvalidDatabaseConnectionParameters, InvalidDatabaseStructure, NotSupported

from flux import models as flux_models
from ferdolt import models as ferdolt_models
from ferdolt_web.settings import FERNET_KEY, SERVER_ID

import re
import pyodbc
from django.utils.translation import gettext as _

import datetime as dt
import json

sql_server_regex = re.compile("sqlserver", re.I)
postgresql_regex = re.compile("postgres", re.I)
deletion_table_regex = re.compile("_deletion$")

def encrypt(object, encoding='utf-8'):
    f = Fernet(FERNET_KEY)

    string = object
    
    if not isinstance(object, bytes):
        string = str(object)
        string = string.encode(encoding)

    token = f.encrypt(string)

    return ( token, token.decode(encoding) )

def decrypt(object, encoding='utf-8'):
    f = Fernet(FERNET_KEY)
    
    string = object

    if not isinstance(object, bytes):
        string = str(object)
        string = string.encode(encoding)

    token = f.decrypt(string)

    return ( token, token.decode(encoding=encoding) )

def custom_converter(object):
    if isinstance(object, dt.datetime):
        return object.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if isinstance(object, dt.date):
        return object.strftime("%Y-%m-%d")

    if isinstance(object, dt.time):
        return object.strftime("%H:%M:%S.%f")
    
    return object.__str__()

def get_type_and_precision(column_name, column_dictionary) -> str:
    string = f"{column_name} "
    type = column_dictionary['data_type']

    if type in ['char', 'nchar', 'varchar', 'nvarchar']:
        return f"{string} {type}({column_dictionary['character_maximum_length']})"

    if type in ['decimal']:
        return f"{string} {type}({column_dictionary['numeric_precision']})"

    return f"{string} {type}"

def get_column_dictionary(table: ferdolt_models.Table, column_name: str) -> dict:
    try:
        column: ferdolt_models.Column = table.column_set.get(name=column_name)
        return {
            'data_type': column.data_type,
            'character_maximum_length': column.character_maximum_length,
            'data_type': column.data_type,
            'numeric_precision': column.numeric_precision,
            'datetime_precision': column.datetime_precision
        }
    except ferdolt_models.Column.DoesNotExist as e:
        logging.error(f"[In flux.serializers] no column with name {column_name} exists in the {table.__str__()} table")
        return None

def get_database_connection(database: ferdolt_models.Database) -> pyodbc.Cursor:
    dbms_name = database.dbms_version.dbms.name

    if sql_server_regex.search(dbms_name):
        driver = "{SQL Server Native Client 11.0}"
        connection_string = (
            f"Driver={driver};"
            f"Server={database.get_host};"
            f"Database={database.name};"
            f"UID={database.get_username};"
            )
        try:
            # we append the password here instead of above for security reasons as we will be logging the connection string in case of errors
            connection = pyodbc.connect(connection_string + f"PWD={database.get_password};")
            return connection
        except pyodbc.ProgrammingError as e:
            print(_("Error connecting to the %(database_name)s database"))
            return None
        except pyodbc.InterfaceError as e:
            logging.error(f"Error connecting to the {database.name} database on {database.host}:{database.port}. Connection string: {connection_string}")
            print(f"Error connecting to the {database.name} database on {database.host}:{database.port}. Connection string: {connection_string}")
            return None
    
    if postgresql_regex.search(dbms_name):
        connection_string = f"dbname={database.name.lower()} user={database.get_username} host={database.get_host} "
        
        try:
            # we append the password here instead of above for security reasons as we will be logging the connection string in case of errors
            connection = psycopg.connect(connection_string + f"password={database.get_password}")
            return connection
        except psycopg.OperationalError as e:
            logging.error(f"Error connecting to the Postgres database {database.name} on {database.host}:{database.port}. Connection string: '{connection_string}'. Error: {str(e)}")
            print(f"Error connecting to the Postgres database {database.name} on {database.host}:{database.port}. Connection string: '{connection_string}'. Error: {str(e)}")
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

def get_query_placeholder(is_postgres_db, is_sqlserver_db, is_mysql_db):
    validate_function(is_postgres_db=is_postgres_db, is_mysql_db=is_mysql_db, is_sqlserver_db=is_sqlserver_db)
    
    if is_sqlserver_db:
        return "?"
    if is_postgres_db:
        return "%s"

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

def get_default_schema(is_postgres_db, is_sqlserver_db, is_mysql_db):
    if is_postgres_db:
        return "public"
    if is_sqlserver_db:
        return "dbo"
    if is_mysql_db:
        return ""

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

def get_column_datatype(is_postgres_db, is_sqlserver_db, is_mysql_db, data_type_string: str):
    if is_postgres_db:
        if data_type_string.lower() in set([ 'timestamp with time zone', 'timestamp without time zone', 'double_precision' ]):
            return data_type_string.split(' ')[0]
        if data_type_string in set( [ 'character varying' ] ):
            return 'varchar'
        if data_type_string in set( ['character'] ):
            return 'char'
    
    return data_type_string

def get_table_foreign_key_references(table: ferdolt_models.Table, connection=None):
    database = table.schema.database

    if not connection:
        connection = get_database_connection(database)

    if connection:
        cursor = connection.cursor()

        dbms_booleans = get_dbms_booleans(database)

        query = None

        if dbms_booleans["is_sqlserver_db"]:
            # select the table and column name of the referenced table as well as 
            # the column name of the referencing column
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
        elif dbms_booleans['is_postgres_db']:
            query = f"""
            SELECT ccu.table_name as table_name, ccu.table_schema as schema_name, ccu.column_name as column_name,
            kcu.column_name referencing_column  
            from information_schema.table_constraints as tc 
            JOIN information_schema.key_column_usage as kcu 
                on tc.constraint_name=kcu.constraint_name and tc.table_schema = kcu.table_schema 
            JOIN information_schema.constraint_column_usage as ccu 
                ON ccu.constraint_name=tc.constraint_name AND ccu.table_schema=tc.table_schema 
            WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_name='{table.name}' 
                AND tc.table_schema='{table.schema.name}'
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

                            logging.info(f"Successfully { 'created' if constraint[1] else 'modified' } the foreign key constraint in the {table.__str__()} table")
                        except ferdolt_models.ColumnConstraint.MultipleObjectsReturned as e:
                            logging.error(f"Multiple foreign key constraints on the {referencing_column.name.lower()} column in the {table.__str__()} table")

                    except ferdolt_models.Column.DoesNotExist as e:
                        logging.error(f"Couldn't find the {record['column_name']} column in the {record['table_name']} table")
                    
                    except IntegrityError as e:
                        logging.error(f"Error adding foreign key constraint on {table.name}{record['referencing_column']} to {record['table_name']}.{record['column_name']}. Error: {str(e)}")

                    except ferdolt_models.Column.MultipleObjectsReturned as e:
                        logging.error(f"Multiple {record['column_name']} columns in the {record['table_name']} table")

                except ferdolt_models.Column.DoesNotExist as e:
                    logging.error(f"Couldn't find the {record['column_name']} column in the {record['table_name']} table")

def get_database_structure_dictionary(database, connection):
    """
    Takes in a database object and returns a dictionary with the schemas and tables in those schemas
    found in the database
    """
    cursor = connection.cursor()

    dbms_booleans = get_dbms_booleans(database)

    if dbms_booleans["is_sqlserver_db"]:
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
    elif dbms_booleans['is_postgres_db']:
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
                    'data_type': get_column_datatype(**dbms_booleans, data_type_string=row_dictionary['data_type']),
                    'character_maximum_length': row_dictionary['character_maximum_length'],
                    'datetime_precision': row_dictionary['datetime_precision'],
                    'numeric_precision': row_dictionary['numeric_precision'],
                    'constraint_type': set([row_dictionary['constraint_type']]),
                    'is_nullable': row_dictionary['is_nullable'].lower() == 'yes'
                }
            else:
                column_dictionary['constraint_type'].add( row_dictionary['constraint_type'] )

            table_dictionary[row_dictionary['column_name']] = column_dictionary

        return dictionary

def create_database_objects_records_from_structure_dictionary( database, dictionary ):
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
                    print(f"Error when trying to get column {column} from table {table}. Error: {str(e)}")

def get_database_details( database ):
    connection = get_database_connection(database)

    if connection:
        dictionary = get_database_structure_dictionary(database, connection)

        create_database_objects_records_from_structure_dictionary(database, dictionary)
        connection.close()

    else:
        raise InvalidDatabaseConnectionParameters("""Error connecting to the database. Check if the credentials are correct or if the database is running""")

def synchronize_database( connection, database_record, dictionary, temporary_tables_created=None ):
    if temporary_tables_created is None:
        temporary_tables_created = set([])

    cursor = connection.cursor()

    dictionary_key = list( dictionary.keys() )[0] if len(dictionary.keys()) == 1 else None

    if database_record.name.lower() in dictionary.keys():
        dictionary_key = database_record.name.lower()
    
    if not dictionary_key:
        logging.error( f"[In core.functions.synchronize_database]. The dictionary has multiple keys and the database's name is not one of them" )
        print( f"[In core.functions.synchronize_database]. The dictionary has multiple keys and the database's name is not one of them" )
    else:
        dbms_booleans = get_dbms_booleans(database_record)

        item: dict = dictionary[dictionary_key]

        tables_set = set([])

        for schema in item.keys():
            for table in item[schema].keys():
                tables_set.add(table.lower())

        database_tables = ferdolt_models.Table.objects.filter(
            Q(schema__database=database_record)
        ) 

        # get tables that are not deletion tables
        tables = database_tables.filter(
            ~Q( id__in=database_tables.filter(deletion_table__isnull=False)
                                        .values("deletion_table__id"))
            & Q( name__in=tables_set )
            & Q( schema__name__in=item.keys() )
        ).annotate(deletion_target_level=F("deletion_target__level")).order_by("level")

        # get tables that are deletion tables
        deletion_tables = database_tables.filter(
            Q( id__in=tables.values("deletion_table__id") )
        ).annotate(deletion_target_level=F("deletion_target__level")).order_by("-deletion_target__level")
        
        for table in list(tables) + list(deletion_tables):
            table_name = table.name.lower() if table.deletion_target_level is None else table.deletion_target.name.lower()
            schema_name = table.schema.name.lower() if table.deletion_target_level is None else table.deletion_target.schema.name.lower()

            table_dictionary_key = "rows" if table.deletion_target_level is None else "deletions"

            table_rows = item[schema_name][table_name][table_dictionary_key]

            if table_rows:
                table_columns = set([
                    f["name"] for f in table.column_set.values("name") if f["name"] in table_rows[0].keys()
                ])
                if table.deletion_target_level is None:
                    primary_key_columns = [
                        f["name"] for f in table.column_set.filter(columnconstraint__is_primary_key=True).values("name")
                    ]
                else: 
                    primary_key_columns = [
                        f["name"] for f in table.deletion_target.column_set.filter(columnconstraint__is_primary_key=True).values("name")
                    ]
                temporary_table_name = f"{table.schema.name.lower()}_{table.name.lower()}_temporary_table"
                temporary_table_actual_name = get_temporary_table_name(database_record, temporary_table_name)

                try:
                    # we create the temporary table only once and use the same table for each of the unapplied files
                    if temporary_table_actual_name not in temporary_tables_created:
                        logging.info(f"Creating the {temporary_table_actual_name} temp table")
                        create_temporary_table_query = get_create_temporary_table_query( database_record, temporary_table_name,  f"( { ', '.join( [ get_type_and_precision(column, get_column_dictionary(table, column)) for column in table_columns ] ) } )" )
        
                        cursor.execute(create_temporary_table_query)
                        temporary_tables_created.add( temporary_table_actual_name )

                    try:
                        # emptying the temporary table in case of previous data
                        try:
                            cursor.execute(f"DELETE FROM {temporary_table_actual_name}")
                        except pyodbc.ProgrammingError as e:
                            logging.error(f"Error deleting from the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                            print(f"Error deleting from the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                            connection.rollback()
                            raise e

                        insert_into_temporary_table_query = f"""
                        INSERT INTO {temporary_table_actual_name} ( { ', '.join( [ column for column in table_columns ] ) } ) 
                        VALUES ( { ', '.join( [ '?' if isinstance(cursor, pyodbc.Cursor) else '%s'  for _ in table_columns ] ) } );
                        """
                        rows_to_insert = []

                        # getting the list of tables for bulk insert
                        for row in table_rows:
                            rows_to_insert.append( tuple(
                                row[f] for f in table_columns
                            ) )

                        cursor.executemany(insert_into_temporary_table_query, rows_to_insert)
                        if dbms_booleans['is_sqlserver_db']:
                            # set identity_insert on to be able to explicitly write values for identity columns
                            try:
                                cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} ON")
                            except pyodbc.ProgrammingError as e:
                                logging.error(f"Error occured when setting identity_insert on for {schema_name}.{table_name} table")
                                print(f"Error occured when setting identity_insert on for {schema_name}.{table_name} table")
                                raise e

                        merge_query = None

                        if table.deletion_target_level is None: # the table is not a deletion table
                            merge_query = ""

                            if dbms_booleans["is_sqlserver_db"]:
                                merge_query = f"""
                                    merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                        {
                                            ' AND '.join( [ f"t.{column} = s.{column}" for column in primary_key_columns ] ) 
                                        }
                                    ) 
                                    when matched and t.last_updated < s.last_updated then 
                                    update set {
                                        ' , '.join(
                                            [ f"{column} = s.{column}" for column in table_columns if column not in primary_key_columns ] )
                                    }

                                    when not matched then 
                                        insert ( { ', '.join( [ column for column in table_columns ] ) } ) values ( { ', '.join( [ f"s.{column}" for column in table_columns ] ) } )
                                    ;
                                """
                        
                            elif dbms_booleans['is_postgres_db']:
                                merge_query = f"""
                                INSERT INTO {schema_name}.{table_name} (SELECT * FROM {temporary_table_actual_name}) 
                                ON CONFLICT ( { ', '.join( [ column for column in primary_key_columns ] ) } )
                                DO 
                                    UPDATE SET { ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns if column not in primary_key_columns ) }
                                """
                        
                        else:
                            deletion_table_row_identifier = "row_tracking_id"

                            if dbms_booleans["is_sqlserver_db"]:
                                merge_query = f"""
                                merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                    {
                                        f"t.tracking_id = s.{deletion_table_row_identifier}"
                                    }
                                ) 
                                when matched then 
                                delete;
                                """
                            elif dbms_booleans["is_postgres_db"]:
                                merge_query = f"""
                                DELETE FROM {schema_name}.{table_name} WHERE { 
                                    ' AND, '.join(
                                        f"{column} IN (SELECT {column} FROM {temporary_table_actual_name})" 
                                        for column in primary_key_columns
                                    )
                                    }
                                """

                        # execute merge query
                        # merge is used to either insert update or do nothing based on certain conditions
                        try:
                            if merge_query:
                                cursor.execute(merge_query)
                                connection.commit()
                        except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                            logging.error(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
                            print(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
                            cursor.connection.rollback()
                            flag = False
                            raise e
                        
                        except (pyodbc.IntegrityError, psycopg.IntegrityError) as e:
                            logging.error(f"Error executing merge query\n {merge_query}. \n Exception: {str(e)}")
                            print(f"Error executing merge query\n {merge_query}. \n Exception: {str(e)}")
                            connection.rollback()
                            raise e
                            flag = False
                            

                        if dbms_booleans['is_sqlserver_db']:
                            # set identity_insert off as only one table can have identity_insert on per session
                            # if we don't set it off for this table, no other table will be able to have identity_insert on
                            try:
                                cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} OFF")
                            except pyodbc.ProgrammingError as e:
                                logging.error(f"Error setting identity_insert off for {schema_name}.{table_name} table. Error encountered: {str(e)}")
                                print(f"Error setting identity_insert off for {schema_name}.{table_name} table. Error encountered: {str(e)}")
                                connection.rollback()
                                raise e
                                flag = False

                    except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                        logging.error(f"Error inserting into temporary table {temporary_table_actual_name}. Error encountered: {str(e)}")
                        # print(f"Error inserting into temporary table {temporary_table_actual_name}. Error encountered: {str(e)}")
                        logging.error(f"Query to insert into temporary table {insert_into_temporary_table_query}")
                        cursor.connection.rollback()
                        flag = False
                        raise e

                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                    logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                    print(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                    cursor.connection.rollback()
                    print(f"The temporary tables that have already been created are: ")
                    print(temporary_tables_created)
                    print(f"The query to create the temporary tables: {create_temporary_table_query}")
                    flag = False
                    raise e
            
def create_sequence_query(sequence_name, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False, data_type='int', start=1, minvalue=1, cycle='true', increment=1, maxvalue=99):
    if not is_postgres_db and not is_mysql_db and not is_sqlserver_db:
        raise ValueError("One of the followin have to be True: is_postgres_db, is_mysql_db, is_sqlserver_db")

    if is_sqlserver_db:
        return f"""
        IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = '{sequence_name}')
        BEGIN
            CREATE SEQUENCE {sequence_name} AS {data_type} START WITH {start} INCREMENT BY {increment} MINVALUE {minvalue} { 'CYCLE' if cycle else '' } MAXVALUE {maxvalue}
        END    
        """
    if is_postgres_db:
        return f"""
        CREATE SEQUENCE IF NOT EXISTS {sequence_name} AS {data_type} 
        START WITH {start} INCREMENT BY {increment} MINVALUE {minvalue} {'CYCLE' if cycle else ''} MAXVALUE {maxvalue}
        """


def create_datetime_column_with_default_now_query(table, is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False, column_name='last_updated', use_timezone=False):
    """
    returns the query used to create a datetime column with default as the current time in the specified dbms
    """

    if is_sqlserver_db:
        return f"""
            IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = 'last_updated')
                BEGIN
                    ALTER TABLE {table.schema.name}.{table.name} ADD last_updated DATETIME2(6) DEFAULT CURRENT_TIMESTAMP;
                END
        """
    elif is_postgres_db:
        return f"""
        ALTER TABLE {table.get_queryname()} ADD COLUMN IF NOT EXISTS last_updated timestamp DEFAULT NOW() { "AT TIME ZONE 'UTC'" if use_timezone else '' }
        """

def get_create_tracking_id_column_query(table, is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False, column_name='tracking_id', length=len(SERVER_ID) + 16):
    """
    returns the query to create the tracking_id column
    """
    if is_sqlserver_db:
        return f"""
            IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = '{column_name}')
            BEGIN
                ALTER TABLE {table.schema.name}.{table.name} ADD {column_name} VARCHAR({length}) UNIQUE;
            END
        """
    elif is_postgres_db:
        return f"""
        ALTER TABLE {table.get_queryname()} ADD COLUMN IF NOT EXISTS {column_name} VARCHAR({length}) UNIQUE
        """

def set_tracking_id_where_null_query(table_name, schema_name, primary_key_column, sequence_name, server_id, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False, column_name='tracking_id', primary_key_column_data_type='int', batch_size=99):
    if is_sqlserver_db:
        return f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}' AND COLUMN_NAME = '{column_name}') 
                BEGIN
                    DECLARE @SQL VARCHAR(5000);
                    SET @SQL =  'DECLARE @nextvalue INT; ' -- to set the TrackingId of already existing columns
                    + 'SET @nextvalue = next value for {schema_name}_{table_name}_tracking_id_sequence; '
                    
                    + 'DECLARE @table_id {primary_key_column_data_type}; '

                    +	'SELECT @table_id = min({primary_key_column}) FROM {schema_name}.{table_name} '
                    +	'WHERE tracking_id IS NULL; '

                    +	'while @table_id IS NOT NULL '
                    +	'BEGIN '
                    +		'UPDATE {schema_name}.{table_name} SET tracking_id = ''{server_id}'' + '
                    +		'FORMAT( CURRENT_TIMESTAMP, ''yyyyMMddHHmmss'' ) + '
                    +		'RIGHT ( ''00'' + CAST( @nextvalue AS varchar(2)), 2 ) WHERE {primary_key_column} = @table_id; '

                    +		'SELECT @table_id = min({primary_key_column}) FROM {schema_name}.{table_name} '
                    +		'WHERE {primary_key_column} > @table_id AND tracking_id IS NULL; '
                    
                    +		'SET @nextvalue = next value for {schema_name}_{table_name}_tracking_id_sequence; '
                    +	'END '

                    EXEC(@SQL);
                END
        """

    elif is_postgres_db:
        return f"""
            call set_{schema_name}_{table_name}_tracking_id_where_null({batch_size});

            --DO $$ 
            --DECLARE table_ids RECORD;
            --BEGIN
            --    IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}' AND TABLE_SCHEMA='{schema_name}' AND COLUMN_NAME='{column_name}') THEN 
            --        BEGIN
            --            FOR table_ids IN SELECT {primary_key_column} FROM {schema_name}.{table_name} WHERE tracking_id IS NULL LIMIT {batch_size} LOOP 
            --                UPDATE {schema_name}.{table_name} SET {column_name} = '{server_id}' || to_char( now(), 'yyyyMMddhhmmss' ) 
            --                || LPAD( CAST( nextval('{sequence_name}') AS VARCHAR ), 2, '0' ) 
            --                WHERE {primary_key_column} = table_ids.{primary_key_column};
            --            END LOOP;
            --        END;
            --    END IF;
            --END;
            --$$
        """

def get_set_tracking_id_where_null_query(
    table_name, schema_name, primary_key_columns, 
    sequence_name, server_id, is_postgres_db=False, is_sqlserver_db=False, 
    is_mysql_db=False, column_name='tracking_id'
):
    procedure_name = f"set_{schema_name}_{table_name}_tracking_id_where_null"
    if is_postgres_db:
        return f"""
            CREATE OR REPLACE PROCEDURE {procedure_name}(batch_size int, datetime_string varchar(8))
            LANGUAGE plpgsql AS $$ 
            DECLARE table_ids RECORD;
            BEGIN
                IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}' AND TABLE_SCHEMA='{schema_name}' AND COLUMN_NAME='{column_name}') THEN 
                    BEGIN 
                        FOR table_ids IN SELECT {', '.join( [column.name for column in primary_key_columns] )} FROM {schema_name}.{table_name} WHERE tracking_id IS NULL LIMIT batch_size LOOP 
                            UPDATE {schema_name}.{table_name} SET {column_name} = '{server_id}' || datetime_string 
                            || LPAD( CAST( nextval('{sequence_name}') AS VARCHAR ), 2, '0' ) 
                            WHERE { ' AND '.join( [ f"{column.name}=table_ids.{column.name}" for column in primary_key_columns ] ) };
                        END LOOP;
                    END;
                END IF;
            END;
            $$
        """

def get_column_type_and_precision(column) -> str:
    string = f"{column.name} "
    type = column.data_type

    if type in ['char', 'nchar', 'varchar', 'nvarchar']:
        return f"{string} {type}({column.character_maximum_length})"

    if type in ['decimal']:
        return f"{string} {type}({column.numeric_precision})"

    return f"{string} {type}"

def set_tracking_id_where_null_query_multiple_primary_keys(table, primary_key_columns, server_id, sequence_name, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False, column_name='tracking_id'):
    schema_name = table.schema.name
    table_name = table.name
    table_queryname = table.get_queryname()

    if is_sqlserver_db:
        return f"""
        IF EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = '{column_name}' )
        BEGIN
            DECLARE @SQL VARCHAR(5000);
            SET @SQL = ' DECLARE @nextvalue INT; ' -- next value for the tracking_id sequence
            + 'SET @nextvalue = next value for {sequence_name}; '
            + ' DECLARE @table_id TABLE ( { ', '.join( [ f"{column.name} {column.data_type}" for column in primary_key_columns ] ) } ); '
            + ' INSERT INTO @table_id SELECT { ', '.join( [ column.name for column in primary_key_columns ] ) } FROM {table_queryname} WHERE tracking_id IS NULL;'
            + ' WHILE EXISTS(SELECT 1 FROM @table_id) '
            + ' BEGIN '
                + ' UPDATE {table_queryname} SET tracking_id = ''{server_id}'' +  '
                + ' FORMAT( CURRENT_TIMESTAMP, ''yyyyMMddHHmmss'' ) + '
                + ' RIGHT ( ''00'' + CAST( @nextvalue AS varchar(2) ), 2 ); '

                + ' DELETE FROM @table_id; '
                
                + ' INSERT INTO @table_id SELECT { ', '.join( [ column.name for column in primary_key_columns ] ) } FROM {table_queryname} WHERE tracking_id IS NULL;'
                + ' SET @nextvalue = next value for { sequence_name }; '
            + ' END '

            EXEC(@SQL);
        END
        """

    elif is_postgres_db:
        return f"""
            DO $$ 
            DECLARE table_ids RECORD;
            BEGIN
                IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}' AND TABLE_SCHEMA='{schema_name}' AND COLUMN_NAME='{column_name}') THEN 
                    BEGIN
                        FOR table_ids IN SELECT { ', '.join( [f"{column.name}" for column in primary_key_columns] ) } 
                        FROM {schema_name}.{table_name} WHERE {column_name} IS NULL LOOP 
                            UPDATE {schema_name}.{table_name} SET {column_name} = '{server_id}' || to_char( now(), 'yyyyMMddhhmmss' ) 
                            || LPAD( CAST( nextval('{sequence_name}') AS VARCHAR ), 2, '0' ) 
                            WHERE { " AND ".join( [ f"{column.name} = table_ids.{column.name}" for column in primary_key_columns ] ) };
                        END LOOP;
                    END;
                END IF;
            END;
            $$
        """

def update_datetime_columns_to_now_query(table, column_name='last_updated', is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False, use_timezone=False):
    validate_function( is_sqlserver_db=is_sqlserver_db, is_mysql_db=is_mysql_db, is_postgres_db=is_postgres_db )

    if is_sqlserver_db:
        return f"UPDATE {table.get_queryname()} SET {column_name}=CURRENT_TIMESTAMP"
    elif is_postgres_db:
        return f"""UPDATE {table.get_queryname()} SET {column_name}=NOW() { "AT TIME ZONE 'UTC'" if use_timezone else '' }"""
    else:
        raise NotSupported("We do not support the database you passed yet")

def validate_function(is_postgres_db, is_mysql_db, is_sqlserver_db):
    if not is_postgres_db and not is_mysql_db and not is_sqlserver_db:
        raise ValueError( _("One of the following must be True: is_postgres_db, is_mysql_db or is_sqlserver_db") )
    if (is_postgres_db and is_mysql_db) or (is_mysql_db and is_sqlserver_db):
        raise ValueError( _("Only one of the following must be True: is_postgres_db, is_mysql_db or is_sqlserver_db") )

def create_column_if_not_exists(table, column_name, is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False, data_type="varchar(21)"):
    validate_function( is_sqlserver_db=is_sqlserver_db, is_mysql_db=is_mysql_db, is_postgres_db=is_postgres_db )
    
    if is_sqlserver_db:
        return f"""
        IF NOT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = '{column_name}' )
            BEGIN
                ALTER TABLE {table.schema.name}.{table.name} ADD {column_name} {data_type};
            END
        """

    if is_postgres_db:
        return f"""
        ALTER TABLE {table.get_queryname()} ADD COLUMN IF NOT EXISTS {column_name} {data_type}; 
        """

    if is_mysql_db:
        return ""

def insert_update_delete_trigger_query( 
    table, trigger_name, sequence_name, 
    primary_key_columns, is_postgres_db=False, is_mysql_db=False, 
    is_sqlserver_db=False, tracking_id_exists=True, use_timezone=False 
):
    primary_key_columns = [ f for f in table.column_set.filter(columnconstraint__is_primary_key=True) ]
    primary_key_column_names = [ f.name for f in primary_key_columns ]

    if is_sqlserver_db:
        return f"""
        -- create trigger if not exists but last_updated column exists
            IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = 'last_updated')
            BEGIN
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'TR' AND name = '{trigger_name}')
                BEGIN
                    DECLARE @SQL varchar(5000);
                    
                    SET @SQL = 'CREATE TRIGGER {trigger_name} ON {table.schema.name}.{table.name} ' 
                    + ' FOR INSERT, UPDATE, DELETE AS BEGIN '
                    + ' DECLARE @tn_b INT; '
                    + ' SET @tn_b = TRIGGER_NESTLEVEL(( SELECT object_id FROM sys.triggers WHERE name = ''{trigger_name}'' )); '
                    + ' IF (@tn_b <= 1) '
                    + ' BEGIN '
                        + ' IF EXISTS (SELECT 0 FROM inserted) '  -- insert or update
                        + ' BEGIN '
                            + 'IF EXISTS (SELECT DISTINCT { ', '.join( primary_key_column_names ) } FROM inserted) AND EXISTS (SELECT 0 FROM deleted) '
                            + 'BEGIN '
                                + ' UPDATE {table.get_queryname()} SET last_updated=CURRENT_TIMESTAMP WHERE '
                                + ' { ', '.join( primary_key_columns ) if not tracking_id_exists else 'tracking_id' } IN (SELECT DISTINCT { ', '.join( primary_key_column_names ) if not tracking_id_exists else 'tracking_id' } FROM inserted); '
                            + 'END '

                            + 'IF NOT EXISTS (SELECT 0 FROM deleted) '
                            + 'BEGIN '
                                + ' DECLARE @table_id TABLE ( {', '.join( [ f"{get_column_type_and_precision(column)}" for column in primary_key_columns ] )} );'
                                + ' DECLARE @now_datetime DATETIME2;'
                                + ' DECLARE @nextvalue INT;'
                                + ' DECLARE @offset INT; '

                                + ' SET @nextvalue = next value for {sequence_name}; '
                                + ' SET @offset = 0;'

                                + 'INSERT INTO @table_id SELECT { ', '.join( [ column.name for column in primary_key_columns ] ) } FROM inserted WHERE tracking_id IS NULL '
                                + ' ORDER BY { ', '.join( [ column.name for column in primary_key_columns ] ) } OFFSET @offset ROWS ' 
                                + ' FETCH NEXT 1 ROWS ONLY;'

                                + ' WHILE EXISTS(SELECT 1 FROM @table_id) '
                                + ' BEGIN '
                                    + ' SET @now_datetime = CURRENT_TIMESTAMP;'
                                    + ' UPDATE {table.get_queryname()} SET tracking_id = ''{SERVER_ID}'' +  FORMAT( @now_datetime, ''yyyyMMddHHmmss'' ) + '
                                    + ' RIGHT ( ''00'' + CAST( @nextvalue AS varchar(2) ), 2 ), last_updated = @now_datetime '
                                    + ' WHERE { " AND ".join( [ f"{column} IN (SELECT {column} FROM @table_id)" for column in primary_key_column_names ] ) }'

                                    + ' DELETE FROM @table_id;'
                                    
                                    + ' SET @offset = @offset + 1; '

                                    + 'INSERT INTO @table_id SELECT { ', '.join( [ column.name for column in primary_key_columns ] ) } FROM inserted WHERE tracking_id IS NULL '
                                    + ' ORDER BY { ', '.join( [ column.name for column in primary_key_columns ] ) } OFFSET @offset ROWS ' 
                                    + ' FETCH NEXT 1 ROWS ONLY; '
                                
                                    + ' SET @nextvalue = next value for { sequence_name }; '
                                + ' END'

                        + ' END '
                        + ' ELSE ' -- deletion
                        + ' BEGIN '
                                + 'IF EXISTS (SELECT DISTINCT { ', '.join( primary_key_columns ) if not tracking_id_exists else 'tracking_id' } FROM DELETED) '
                                + 'BEGIN '
                                    + ' DECLARE @table_tracking_id { ' INT ' if not tracking_id_exists else ' VARCHAR(21) ' }; '
                                    + ' SELECT @table_tracking_id = min({ ', '.join( primary_key_columns ) if not tracking_id_exists else 'tracking_id' }) FROM deleted; '
                                    + ' WHILE @table_tracking_id IS NOT NULL '
                                    + ' BEGIN '
                                        + ' INSERT INTO {table.schema.name}_{table.name}_deletion (row_tracking_id, deletion_time) '
                                        + ' VALUES ( @table_tracking_id, CURRENT_TIMESTAMP );'
                                        + ' SELECT @table_tracking_id = min({ ', '.join( primary_key_columns ) if not tracking_id_exists else 'tracking_id' }) FROM deleted WHERE { ', '.join( primary_key_columns ) if not tracking_id_exists else 'tracking_id' } > @table_tracking_id;'
                                    + ' END '
                                + ' END '
                        + ' END '
                    + ' END END END '
                    EXEC (@SQL);
                END
            END
        """
    
    elif is_postgres_db:
        def get_trigger_function_query(function_name: str):
            tracking_id_column = 'tracking_id'
            print("Creating trigger for postgres db")
            return f"""
                CREATE OR REPLACE FUNCTION {function_name}() 
                RETURNS TRIGGER AS $function$ 
                DECLARE table_ids RECORD; 
                BEGIN
                    IF (TG_OP = 'DELETE') THEN 
                        BEGIN
                            INSERT INTO {table.schema.name}_{table.name}_deletion (row_tracking_id, deletion_time) 
                            VALUES ( tables_ids.tracking_id, now() {"AT TIME ZONE 'UTC'" if use_timezone else ''} );
                        END;
                    
                    ELSIF (TG_OP = 'UPDATE') THEN 
                        BEGIN 
                            UPDATE {table.get_queryname()} SET last_updated=CURRENT_TIMESTAMP WHERE 
                            { ' AND '.join([ f"{column}=NEW.{column}" for column in primary_key_column_names]) if not tracking_id_exists else f'{tracking_id_column}=NEW.{tracking_id_column}' };
                        END;

                    ELSIF (TG_OP = 'INSERT') THEN                         
                        UPDATE {table.get_queryname()} SET tracking_id = '{SERVER_ID}' || TO_CHAR( now(), 'yyyyMMddhhmmss' ) 
                        || LPAD( CAST(nextval('{sequence_name}') AS VARCHAR), 2, '0' ) 
                        WHERE { " AND ".join( [ f"{column}=NEW.{column}" for column in primary_key_column_names ] ) };

                    END IF;

                    RETURN NULL;
                END;
                $function$ LANGUAGE plpgsql;
            """
        
        function_name = f"{trigger_name}_function"
        
        return f"""
        DO $$ 
        BEGIN
            IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table.name}' AND TABLE_SCHEMA='{table.schema.name}' AND COLUMN_NAME='last_updated') THEN 
                BEGIN
                    {get_trigger_function_query(function_name)}
                    CREATE OR REPLACE TRIGGER {trigger_name} AFTER INSERT OR UPDATE OR DELETE ON {table.get_queryname()} 
                    FOR EACH ROW 
                    WHEN (pg_trigger_depth() = 0)
                    EXECUTE FUNCTION {function_name}();
                END;
            END IF;
        END;
        $$
        """
    
    else:
        raise NotSupported

def create_deletion_table_query( table, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False ):
    if is_sqlserver_db:
        return f"""
        IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table.schema.name}_{table.name}_deletion') 
        BEGIN
            CREATE TABLE {table.schema.name}_{table.name}_deletion (deletion_id INT IDENTITY PRIMARY KEY, deletion_time DATETIME2(6) DEFAULT CURRENT_TIMESTAMP, row_tracking_id VARCHAR( { len(SERVER_ID) + 16 } ))
        END
        """
    if is_postgres_db:
        return f"""CREATE TABLE IF NOT EXISTS {table.schema.name}_{table.name}_deletion 
        (deletion_id SERIAL PRIMARY KEY, deletion_time timestamp DEFAULT NOW(), 
        row_tracking_id VARCHAR( { len(SERVER_ID) + 16 } ))
        """

def refresh_table( connection, table ):
    if connection:
        cursor = connection.cursor()

        dbms_booleans = get_dbms_booleans(table.schema.database)
        query = None

        if dbms_booleans['is_sqlserver_db']:
            query = f"""
            SELECT C.COLUMN_NAME, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.DATETIME_PRECISION, C.NUMERIC_PRECISION, C.IS_NULLABLE, TC.CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLES T LEFT JOIN 
            INFORMATION_SCHEMA.COLUMNS C ON C.TABLE_NAME = T.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA 
            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
            CU.COLUMN_NAME = C.COLUMN_NAME AND CU.TABLE_NAME = C.TABLE_NAME AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA LEFT JOIN 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME 
            WHERE T.TABLE_TYPE = 'BASE TABLE' AND T.TABLE_NAME = '{table.name}' AND T.TABLE_SCHEMA = '{table.schema.name}' ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME
            """
        
        if query:
            cursor = connection.cursor()
            results = cursor.execute(query)
            columns = [ column[0].lower() for column in cursor.description ]
            
            table_dictionary = {}

            for row in results:
                row_dictionary = dict( zip(columns, row) )
                column_dictionary = table_dictionary.setdefault(
                    row_dictionary['column_name'], {}
                )

                if not column_dictionary:
                    column_dictionary = {
                        'data_type': get_column_datatype(**dbms_booleans, data_type_string=row_dictionary['data_type']),
                        'character_maximum_length': row_dictionary['character_maximum_length'],
                        'datetime_precision': row_dictionary['datetime_precision'],
                        'numeric_precision': row_dictionary['numeric_precision'],
                        'constraint_type': set([row_dictionary['constraint_type']]),
                        'is_nullable': row_dictionary['is_nullable'].lower() == 'yes'
                    }
                else:
                    column_dictionary['constraint_type'].add( row_dictionary['constraint_type'] )
                
                table_dictionary[row_dictionary['column_name']] = column_dictionary

            for column in table_dictionary.keys():
                column_dictionary = table_dictionary[column]

                try:
                    column_record = ferdolt_models.Column.objects.get_or_create(table=table, name=column)[0]

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

                    get_table_foreign_key_references(table, connection=connection)
                except Exception as e:
                    logging.error(f"Error occured: {str(e)}")
                    print(f"Error occured: {str(e)}")
                    raise e

def initialize_database( database_record ):
    logging.debug(f"Initializing database {database_record.__str__()}")
    # print(f"Initializing database {database_record.__str__()}")
    try:
        get_database_details(database_record)
        database_record.refresh_from_db()

        dbms_booleans = get_dbms_booleans(database_record)

        default_schema = ferdolt_models.DatabaseSchema.objects.get_or_create(database=database_record, name=get_default_schema(**dbms_booleans))[0]
        
        connection = get_database_connection(database_record)
        cursor = connection.cursor()
        
        server_id = SERVER_ID

        success_flag = True

        for table in ferdolt_models.Table.objects.filter( Q(schema__database=database_record) & ~Q(name__icontains='_deletion') ):
            tracking_id_exists = False
            primary_key_columns = table.column_set.filter(columnconstraint__is_primary_key=True).distinct()

            # check if there is a tracking_id column in the table
            if 1:
                if primary_key_columns.count() == 1:
                    # create and populate tracking_id column only if there is a single or no primary key column
                    try:
                        logging.info(f"Adding the tracking_id column to the {table.get_queryname()} table in the {database_record.__str__()} database")
                        
                        print(f"Adding the tracking_id column to the {table.get_queryname()} table in the {database_record.__str__()} database")
                        
                        query = get_create_tracking_id_column_query(table, **dbms_booleans)

                        cursor.execute(query)

                        try:
                            logging.info(f"Creating the tracking_id sequence in the {database_record.__str__()} database")
                            print(f"Creating the tracking_id sequence in the {database_record.__str__()} database")

                            sequence_name = f"{table.schema.name.lower()}_{table.name.lower()}_tracking_id_sequence"

                            # create sequence
                            sequence_query = create_sequence_query(f"{sequence_name}", **dbms_booleans)

                            cursor.execute( sequence_query )

                            logging.info(f"Setting the tracking_id where null in the {table.get_queryname()} in the {database_record.__str__()} database")
                            print(f"Setting the tracking_id where null in the {table.get_queryname()} in the {database_record.__str__()} database")

                            results = cursor.execute(f"SELECT COUNT(*) FROM {table.get_queryname()}").fetchone()

                            number_of_rows = results[0]
                            batch_size = 99

                            number_of_iterations, remainder = divmod(number_of_rows, batch_size)

                            number_of_iterations += 0 if remainder == 0 else 1

                            query = set_tracking_id_where_null_query(
                                table.name, table.schema.name, 
                                primary_key_columns.first().name, server_id=server_id, 
                                sequence_name=f"{table.schema.name.lower()}_{table.name.lower()}_tracking_id_sequence", 
                                **dbms_booleans, batch_size=batch_size 
                            )

                            if dbms_booleans['is_sqlserver_db']:
                                number_of_iterations = 1

                            if dbms_booleans['is_postgres_db']:
                                # create the function to populate the tracking_id
                                try:
                                    procedure_name = f"set_{table.schema.name}_{table.name}_tracking_id_where_null"
                                    procedure_query = get_set_tracking_id_where_null_query(table.name, table.schema.name, primary_key_columns, sequence_name=sequence_name, server_id=SERVER_ID, **dbms_booleans)
                                    cursor.execute(procedure_query)

                                    for i in range(number_of_iterations):
                                        try:
                                            date_time_string = dt.datetime.now().strftime('%Y%m%d%H%M%S')
                                            call_procedure_query = f"call {procedure_name}({batch_size}, '{date_time_string}')"
                                            cursor.execute(call_procedure_query)
                                            time.sleep(2)
                                        except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                                            logging.error(f"Error calling the {procedure_name} procedure")
                                            logging.error(f"{call_procedure_query}")
                                            connection.rollback()
                                            successful_flag = False
                                            raise e
                                
                                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                    logging.error(f"Error creating the procedure to populate the tracking_id column")
                                    logging.error(f"Query to create the procedure: {procedure_query}")
                                    connection.rollback()
                                    successful_flag = False
                                    raise e

                            else:
                                # setting the tracking_id where null in batches to avoid integrity errors due to duplicate tracking_ids
                                try:
                                    # set the tracking_id where it is null
                                    cursor.execute( query )
                                    logging.info("tracking_id column populated successfully")
                                except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                                    logging.error(f"Error setting tracking_id where null in the {table.get_queryname()} table in the {database_record.name.lower()} database. Error: {str(e)}")
                                    logging.error(f"Query to set the tracking_id where null: {sequence_query}")
                                    success_flag = False
                                    connection.rollback()
                                    raise e

                        except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                            logging.error(f"Error creating tracking_id_sequence. Error: {e}")
                            logging.error(f"Query to create the sequence: {sequence_query}")
                            connection.rollback()
                            success_flag = False
                            raise e
                        except ( psycopg.errors.UniqueViolation ) as e:
                            logging.error(f"Error setting the tracking_id. Error: {str(e)}")
                            connection.rollback()
                            success_flag = False
                            breakpoint()
                            raise e

                    except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                        logging.error(f"Error adding tracking_id column to the {table.get_queryname()} table in the {database_record.name.lower()} database. Error: {str(e)}")
                        logging.error(f"Query to add the tracking_id column: {query}")
                        success_flag = False
                        connection.rollback()
                        raise e

                elif primary_key_columns.count() > 1:
                    # adding and populating the tracking_id column to the table with multiple primary keys
                    try:
                        logging.info(f"Adding the tracking_id column to the {table.get_queryname()} table with multiple primary keys in the {database_record.__str__()} database")
                        print(f"Adding the tracking_id column to the {table.get_queryname()} table in the {database_record.__str__()} database")
                        query = get_create_tracking_id_column_query(table, **dbms_booleans)
                        cursor.execute(query)

                        try:
                            logging.info(f"Creating the tracking_id sequence in the {database_record.__str__()} database")
                            print(f"Creating the tracking_id sequence in the {database_record.__str__()} database")

                            create_sequence_query_string = create_sequence_query(f"{table.schema.name.lower()}_{table.name.lower()}_tracking_id_sequence", **dbms_booleans)

                            cursor.execute( create_sequence_query_string )
                            
                            sequence_name = f"{table.schema.name.lower()}_{table.name.lower()}_tracking_id_sequence"

                            set_tracking_id_where_null_query_string = set_tracking_id_where_null_query_multiple_primary_keys(
                                table, primary_key_columns, server_id, 
                                sequence_name=sequence_name, **dbms_booleans
                            )

                            try:
                                logging.info(f"Setting the tracking_id where null in the {table.get_queryname()} table (with composite PKs) in the {database_record.__str__()} database")
                                print(f"Setting the tracking_id where null in the {table.get_queryname()} table (with composite PKs) in the {database_record.__str__()} database")

                                logging.info(f"Query to set the tracking_id where null: {set_tracking_id_where_null_query_string}")

                                if not dbms_booleans['is_postgres_db']:
                                    cursor.execute( set_tracking_id_where_null_query_string )
                                else:
                                    procedure_query = get_set_tracking_id_where_null_query(table.name, table.schema.name, primary_key_columns, sequence_name, SERVER_ID, **dbms_booleans)

                                    try:
                                        cursor.execute(procedure_query)
                                        
                                        # get the number of rows in the table
                                        query = f"SELECT COUNT(*) FROM {table.get_queryname()}"
                                        number_of_rows = cursor.execute(query).fetchone()[0]
                                        batch_size = 99

                                        number_of_iterations, remainder = divmod(number_of_rows, batch_size)

                                        number_of_iterations += 0 if remainder == 0 else 1

                                        for i in range( number_of_iterations ):
                                            try:
                                                date_time_string = dt.datetime.now().strftime('%Y%m%d%H%M%S')
                                                call_procedure_query = f"call {procedure_name}({batch_size}, '{date_time_string}')"
                                                cursor.execute(call_procedure_query)
                                                time.sleep(2)
                                            except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                                                logging.error(f"Error calling the {procedure_name} procedure")
                                                logging.error(f"{call_procedure_query}")
                                                connection.rollback()
                                                successful_flag = False
                                                raise e

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error(f"Error creating the procedure to set the tracking_id where null")
                                        logging.error(f"Query to create the procedure: {procedure_query}")
                                        success_flag = False
                                        connection.rollback()
                                        raise e

                            except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                logging.error(f"Error setting the values of the tracking_id column in the {table.get_queryname()} table in the {database_record.name.lower()} database. Error: {str(e)}")
                                logging.error(f"Query to set the values of the tracking_id column: {set_tracking_id_where_null_query_string}")
                                success_flag = False
                                connection.rollback()
                                raise e
                        
                        except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                            logging.error(f"Error creating the tracking_id sequence in the {database_record.name.lower()} database. Error: {str(e)}")
                            logging.error(f"Query to create the sequence: {create_sequence_query_string}")
                            success_flag = False
                            connection.rollback()        
                            raise e

                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                        logging.error(f"Error adding tracking_id column to the {table.get_queryname()} table in the {database_record.name.lower()} database. Error: {str(e)}")
                        logging.error(f"Query to create the tracking_id column: {str(query)}")
                        success_flag = False
                        connection.rollback()
                        raise e
                
                else:
                    raise InvalidDatabaseStructure(f"The {table.__str__()} table in the {database_record.__str__()} does not have a primary key column")

            if 1:
                logging.info(f"""Adding the last_updated column to the {table.get_queryname()} table in the {database_record.__str__()} database""")
                print(f"""Adding the last_updated column to the {table.get_queryname()} table in the {database_record.__str__()} database""")
                query = create_datetime_column_with_default_now_query(table, **dbms_booleans)

                try:
                    cursor.execute(query)

                    try:
                        cursor.execute( update_datetime_columns_to_now_query(table, **dbms_booleans) )
                    except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                        logging.error(f"Error updating last_updated column to now in the {table.get_queryname()} table in the {database_record.name.lower()} database")
                        connection.rollback()
                        success_flag = False
                        raise e

                except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                    logging.error(f"Error adding last_updated column to the {table.get_queryname()} table in the {database_record.name.lower()} database")
                    connection.rollback()
                    success_flag = False
                    raise e

            for constraint in ferdolt_models.ColumnConstraint.objects.filter(is_foreign_key=True, references__isnull=False, column__table=table).distinct():
                # add a new column to the table for the tracking id of the table this foreign key is referencing
                column_name = f"{constraint.column.name}_tracking_id"

                # query to create the a foreign key to the parent's referenced table
                query = create_column_if_not_exists(table, column_name, data_type="varchar(21)", **dbms_booleans)

                try:
                    cursor.execute(query)

                    column = ferdolt_models.Column.objects.get_or_create(name=column_name, table=table, data_type="varchar")
                   
                    constraint.references_tracking_id = column[0]
                    constraint.save()

                except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                    logging.error(f"Error adding the {column_name} column to the {table.get_queryname()} table in the {database_record.name.lower} database. Error: {str(e)}")
                    connection.rollback()
                    success_flag = False
                    raise e

                except (psycopg.IntegrityError, pyodbc.ProgrammingError) as e:
                    logging.error(f"Error adding the {column_name} column to the {table.get_queryname()} table in the {database_record.name.lower} database. Error: {str(e)}")
                    connection.rollback()
                    success_flag = False
                    raise e

            # create and record the deletion table in the local dbms
            try:
                logging.info(f"Creating the deletion table for the {table.__str__()} table in the {database_record.__str__()} database")
                print(f"Creating the deletion table for the {table.__str__()} table in the {database_record.__str__()} database")

                cursor.execute( create_deletion_table_query(table, **dbms_booleans) )

                query = insert_update_delete_trigger_query(table, f"{table.schema.name}_{table.name}_insert_update_delete_trigger", f"{table.schema.name}_{table.name}_tracking_id_sequence", primary_key_columns, **dbms_booleans)

                logging.info(f"Creating the insert, update and delete trigger for the {table.__str__()} table in the {database_record.__str__()} database")
                logging.info(f"Running query: {query}")

                try:
                    cursor.execute( query )
                    logging.info(f"Successfully created the insert, update, delete trigger. Recording the deletion table in the local db")
                    print(f"Successfully created the insert, update, delete trigger. Recording the deletion table in the local db")

                    # create deletion table
                    deletion_table = ( ferdolt_models.Table.objects
                        .get_or_create( name=f'{table.schema.name}_{table.name}_deletion', schema=default_schema ) 
                    )[0]

                    table.deletion_table = deletion_table
                    table.save()
                    
                    logging.info("Refreshing the deletion table to get the different columns")
                    print("Refreshing the deletion table to get the different columns")
                    refresh_table( connection, deletion_table )

                    logging.info("Successfully recorded and refreshed the deletion table. Commiting changes to the target database.")
                    connection.commit()
                
                except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                    logging.error(f"Error creating the insert, update delete trigger for {table.get_queryname()} table in the {database_record.name.lower()}. Error: {str(e)}")
                    logging.error(f"Query to create the trigger: {query}")
                    connection.rollback()
                    success_flag = False
                    raise e
                
                except ( pyodbc.SyntaxError, psycopg.SyntaxError ) as e:
                    logging.error(f"Error creating the insert, update, delete trigger. Error: {str(e)}")
                    logging.error(f"Query to create the trigger: {query}")
                    connection.rollback()
                    success_flag = False

            except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                logging.error(f"Error creating deletion table for {table.get_queryname()} table in the {database_record.name.lower()}. Error: {str(e)}")
                print(f"Error creating deletion table for {table.get_queryname()} table in the {database_record.name.lower()}")
                connection.rollback()
                success_flag = False
                raise e

        if success_flag:
            connection.commit()

    except InvalidDatabaseConnectionParameters as e:
        raise e