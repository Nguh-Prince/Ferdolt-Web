import logging

from cryptography.fernet import Fernet

from django.db.models import Q
from django.db.utils import IntegrityError

import psycopg
from core.exceptions import InvalidDatabaseConnectionParameters, NotSupported

from flux import models as flux_models
from ferdolt import models as ferdolt_models
from ferdolt_web.settings import FERNET_KEY, SERVER_ID

import re
import pyodbc
from django.utils.translation import gettext as _

import datetime as dt
import json

sql_server_regex = re.compile("sql\s*server", re.I)
postgresql_regex = re.compile("postgres", re.I)
deletion_table_regex = re.compile("_deletion$")

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
            return None
    
    if postgresql_regex.search(dbms_name):
        connection_string = f"dbname={database.name.lower()} user={database.get_username} host={database.get_host} "
        
        try:
            # we append the password here instead of above for security reasons as we will be logging the connection string in case of errors
            connection = psycopg.connect(connection_string + f"password={database.get_password}")
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

def get_default_schema(is_postgres_db, is_sqlserver_db, is_mysql_db):
    if is_postgres_db:
        return "default"
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

def get_database_details( database ):
    connection = get_database_connection(database)

    if connection:
        dictionary = get_database_structure_dictionary(database, connection)

        create_database_objects_records_from_structure_dictionary(database, dictionary)
        connection.close()

    else:
        raise InvalidDatabaseConnectionParameters("""Error connecting to the database. Check if the credentials are correct or if the database is running""")

def synchronize_database( connection, database_record, dictionary ):
    cursor = connection.cursor()

    dictionary_key = list( dictionary.keys() )[0] if len(dictionary.keys()) == e else None

    if database_record.name.lower() in dictionary.keys():
        dictionary_key = database_record.name.lower()

    if not dictionary_key:
        logging.error( f"[In core.functions.synchronize_database]. The dictionary has multiple keys and the database's name is not one of them" )
    else:
        dbms_booleans = get_dbms_booleans(database_record)

        item: dict = dictionary[dictionary_key]

        tables = ferdolt_models.Table.objects.filter(
            Q( schema__database=database_record ) & ~Q(name__icontains='_deletion')
        ).order_by('level')

        for table in tables:
            table_name = table.name.lower()
            schema_name = table.schema.name.lower()

            temporary_tables_created = set([])

            # check if this table exists in the synchronization file and if there are any records to apply
            if schema_name in item.keys() and table_name in item[schema_name].keys() and item[schema_name][table_name]:
                table_rows = item[schema_name][table_name]
                table_columns = [
                    f["name"] for f in table.column_set.values("name") if f["name"] in table_rows[0].keys()
                ]
                primary_key_columns = [
                    f["name"] for f in table.column_set.filter(columnconstraint__is_primary_key=True).values("name")
                ]

                temporary_table_name = f"{schema_name}_{table_name}_temporary_table"
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
                            connection.rollback()

                        insert_into_temporary_table_query = f"""
                        INSERT INTO {temporary_table_actual_name} ( { ', '.join( [ column for column in table_columns ] ) } ) VALUES ( { ', '.join( [ '?' if isinstance(cursor, pyodbc.Cursor) else '%s'  for _ in table_columns ] ) } );
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

                        merge_query = None

                        if not deletion_table_regex.search(table_name):
                            merge_query = ""

                            if dbms_booleans["is_sqlserver_db"]:
                                merge_query = f"""
                            merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                {
                                    ' AND '.join(
                                    [ f"t.{column} = s.{column}" for column in primary_key_columns ] )
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
                            if len(primary_key_columns) == 1:
                                if dbms_booleans["is_sqlserver_db"]:
                                    merge_query = f"""
                                    merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                        {
                                            f"t.{primary_key_columns[0]} = s.row_id"
                                        }
                                    ) 
                                    when matched then 
                                    delete
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
                            else:
                                logging.error(f"Could not delete from {table.__str__()} table as it has a composite primary key")
                        select_all_query = f"SELECT * FROM {schema_name}.{table_name}"

                        # execute merge query
                        # merge is used to either insert update or do nothing based on certain conditions
                        try:
                            if merge_query:
                                cursor.execute(merge_query)
                        except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                            logging.error(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
                            cursor.connection.rollback()
                            flag = False
                        except (pyodbc.IntegrityError, psycopg.IntegrityError) as e:
                            logging.error(f"Error executing merge query\n {merge_query}. \n Exception: {str(e)}")
                            cursor.connection.rollback()
                            flag = False
                            

                        if dbms_booleans['is_sqlserver_db']:
                            # set identity_insert off as only one table can have identity_insert on per session
                            # if we don't set it off for this table, no other table will be able to have identity_insert on
                            try:
                                cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} OFF")
                            except pyodbc.ProgrammingError as e:
                                logging.error(f"Error setting identity_insert off for {schema_name}.{table_name} table. Error encountered: {str(e)}")
                                connection.rollback()
                                flag = False

                    except pyodbc.ProgrammingError as e:
                        logging.error(f"Error inserting into temporary table {temporary_table_actual_name}. Error encountered: {str(e)}")
                        cursor.connection.rollback()
                        flag = False
                    
                    except psycopg.ProgrammingError as e:
                        logging.error(f"Error inserting into temporary table {temporary_table_actual_name}. Error encountered: {str(e)}")
                        cursor.connection.rollback()
                        flag = False

                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                    logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                    cursor.connection.rollback()
                    flag = False

def create_sequence_query(sequence_name, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False, data_type='int', start=1, minvalue=1, cycle='true', increment=1):
    if is_sqlserver_db:
        return f"CREATE SEQUENCE {sequence_name} AS {data_type} START WITH {start} INCREMENT BY {increment} MINVALUE {minvalue} { 'CYCLE' if cycle else '' }"

def create_datetime_column_with_default_now_query(table, is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False, column_name='last_updated'):
    """
    returns the query used to create a datetime column with default as the current time in the specified dbms
    """

    if is_sqlserver_db:
        return f"""
            IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = ${table.schema.name}' AND COLUMN_NAME = 'last_updated')
                BEGIN
                    ALTER TABLE {table.schema.name}.{table.name} ADD last_updated DATETIME2(3) DEFAULT CURRENT_TIMESTAMP;
                END
        """

def set_tracking_id_where_null_query(table_name, schema_name, primary_key_column, server_id, is_postgres_db=False, is_sqlserver_db=False, is_mysql_db=False, column_name='tracking_id', primary_key_column_data_type='int'):
    if is_sqlserver_db:
        return f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}' AND COLUMN_NAME = 'tracking_id') 
                BEGIN
                    DECLARE @SQL VARCHAR(5000);
                    SET @SQL =  'DECLARE @nextvalue INT; ' -- to set the TrackingId of already existing columns
                    + 'SET @nextvalue = next value for {schema_name}_{table_name}_tracking_id_sequence; '
                    
                    + 'DECLARE @table_id {primary_key_column_data_type}; '

                    +	'SELECT @table_id = min({primary_key_column}) FROM {schema_name}.{table_name} '
                    +	'WHERE tracking_id IS NULL; '

                    +	'while @table_id IS NOT NULL '
                    +	'BEGIN '
                    +		'UPDATE {schema_name}.{table_name} SET tracking_id = ''${server_id}'' + '
                    +		'FORMAT( CURRENT_TIMESTAMP, ''yyyyMMddHHmmss'' ) + '
                    +		'RIGHT ( ''0000000000'' + CAST( @nextvalue AS varchar(10)), 10 ); '

                    +		'SELECT @table_id = min({primary_key_column}) FROM {schema_name}.{table_name} '
                    +		'WHERE {primary_key_column} > @table_id AND tracking_id IS NULL; '
                    
                    +		'SET @nextvalue = next value for {schema_name}_{table_name}_tracking_id_sequence; '
                    +	'END '

                    EXEC(@SQL);
                END
        """

def update_datetime_columns_to_now_query(table, column_name='last_updated', is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False):
    if is_sqlserver_db:
        return f"UPDATE {table.get_queryname()} SET {column_name}=CURRENT_TIMESTAMP"
    else:
        raise NotSupported

def insert_update_delete_trigger_query( table, trigger_name, is_postgres_db=False, is_mysql_db=False, is_sqlserver_db=False ):
    if is_sqlserver_db:
        return f"""
        -- create trigger if not exists but last_updated column exists
IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.name}' AND TABLE_SCHEMA = '{table.schema.name}' AND COLUMN_NAME = 'last_updated')
BEGIN
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'TR' AND name = '{trigger_name}')
	BEGIN
		DECLARE @SQL nvarchar(1000)
		
		SET @SQL = 'CREATE TRIGGER {trigger_name} ON {table.schema.name}.{table.name} ' 
		+ ' FOR INSERT, UPDATE, DELETE AS BEGIN '
		+ ' DECLARE @tn_b INT; '
		+ ' SET @tn_b = TRIGGER_NESTLEVEL(( SELECT object_id FROM sys.triggers WHERE name = ''{trigger_name}'' )); '
		+ ' IF (@tn_b <= 1) '
		+ ' BEGIN '
			+ ' IF EXISTS (SELECT 0 FROM inserted) '  -- insert or update
			+ ' BEGIN '
				+ 'IF EXISTS (SELECT ${PRIMARYKEYCOLUMN} FROM inserted) '
				+ 'BEGIN '
					+ ' UPDATE {table.schema.name}.{table.name} SET last_updated=CURRENT_TIMESTAMP WHERE ' -- set the value of the last_updated to now
					+ ' ${PRIMARYKEYCOLUMN} IN (SELECT ${PRIMARYKEYCOLUMN} FROM inserted); '
				+ 'END '
			+ ' END '
			+ ' ELSE ' -- deletion
			+ ' BEGIN '
					+ 'IF EXISTS (SELECT ${PRIMARYKEYCOLUMN} FROM DELETED) '
					+ 'BEGIN '
						+ ' DECLARE @table_id INT; '
						+ ' SELECT @table_id = min(${PRIMARYKEYCOLUMN}) FROM deleted; '
						+ ' WHILE @table_id IS NOT NULL '
						+ ' BEGIN '
							+ ' INSERT INTO {table.schema.name}_{table.name}_deletion (row_id, deletion_time) '
							+ ' VALUES ( @table_id, CURRENT_TIMESTAMP );'
							+ ' SELECT @table_id = min(${PRIMARYKEYCOLUMN}) FROM deleted WHERE ${PRIMARYKEYCOLUMN} > @table_id;'
						+ ' END '
					+ ' END '
			+ ' END '
		+ ' END END '

		EXEC (@SQL)
	END
END
        """
    else:
        raise NotSupported

def initialize_database( database_record ):
    try:
        get_database_details(database_record)
        dbms_booleans = get_dbms_booleans
        
        connection = get_database_connection(database_record)
        cursor = connection.cursor()
        
        server_id = SERVER_ID

        for table in ferdolt_models.Table.objects.filter( schema__database=database_record ):
            tracking_id_exists = False

            # check if there is a tracking_id column in the table
            if not table.column_set.filter(name__iexact="tracking_id").exists():
                primary_key_columns = table.column_set.filter(columnconstraint__is_primary_key=True).distinct()

                if primary_key_columns.count() <= 1:
                    # create and populate tracking_id column only if there is a single or no primary key column
                    try:
                        query = f"ALTER TABLE {table.get_queryname()} ADD tracking_id VARCHAR({len(server_id) + 14 + 2}) UNIQUE"

                        cursor.execute(query)
                        try:
                            # create sequence
                            cursor.execute( create_sequence_query(f"{table.schema.name.lower()}_{table.name.lower()}_tracking_id_sequence") )

                            # set the tracking_id where it is null
                            cursor.execute( set_tracking_id_where_null_query(table.name, table.schema.name, primary_key_columns.first().name, server_id, **dbms_booleans ) )

                            tracking_id_exists = True

                        except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                            logging.error(f"Error creating tracking_id_sequence")
                            connection.rollback()
                            raise e                        

                    except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                        logging.error(f"Error adding tracking_id column to the {table.get_queryname()} table in the {database_record.name.lower()} database")
                        connection.rollback()
                        raise e

            if not table.column_set.filter(name__iexact='last_updated').exists():
                query = create_datetime_column_with_default_now_query(table, **dbms_booleans)

                try:
                    cursor.execute(query)
                    cursor.execute( update_datetime_columns_to_now_query(table, **dbms_booleans) )
                except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                    logging.error(f"Error adding last_updated column to the {table.get_queryname()} table in the {database_record.name.lower()} database")
                    connection.rollback()
                    raise e

            # create insert, update, delete trigger
            trigger_name = f"{table.schema.name}_{table.name}_insert_update_delete_trigger"

    except InvalidDatabaseConnectionParameters as e:
        raise e