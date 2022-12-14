from hashlib import sha256
import json
import logging
import os
from time import sleep
import zipfile

from cryptography.fernet import Fernet

from django.core.files import File as DjangoFile
from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone

import psycopg
import pyodbc
from common.functions import hash_file

from core.functions import (
    custom_converter, get_column_dictionary, get_create_temporary_table_query, 
    get_database_connection, get_dbms_booleans, get_temporary_table_name, 
    get_type_and_precision, deletion_table_regex, get_query_placeholder, initialize_database
)

from ferdolt import models as ferdolt_models
from ferdolt_web import settings
from flux.models import File
from flux import models as flux_models
from groups import serializers

from . import models

def extract_from_groupdatabase(
    group_database: models.GroupDatabase, 
    use_time=True, 
    start_time=None, target_databases=None
):
    group: models.Group = group_database.group
    f = Fernet(group.get_fernet_key())

    if use_time:
        if not start_time:
            latest_extraction = (
                models.GroupExtraction.objects
                .order_by('-extraction__time_made')
                .annotate(time_made=F('extraction__time_made')).first()
            )

            if latest_extraction:
                start_time = latest_extraction.time_made
                print(f"Extracting records updated after: {latest_extraction.time_made}")
    
    time_made = timezone.now()

    results = {}

    if not target_databases:
        target_databases = group.groupdatabase_set.filter(Q(can_read=True) & ~Q(id=group_database.id))

    group_dictionary = results.setdefault( group.slug, {} )

    connection = get_database_connection(group_database.database)
    dbms_booleans = get_dbms_booleans(group_database.database)

    query_placeholder = get_query_placeholder(**dbms_booleans)
    if connection:
        cursor = connection.cursor()

        with transaction.atomic():
            for table in group.tables.all():
                # get the tables of this database linked to the group's tables
                actual_database_tables = ferdolt_models.Table.objects.filter( 
                    id__in=table.grouptabletable_set.values("table__id"), 
                    schema__database=group_database.database 
                )
                table_dictionary = {}
                for item in actual_database_tables:
                    table_results = []

                    table_query_name = item.get_queryname()
                    
                    # get the group columns of the grouptable linked to this item's table
                    columns_in_common = ferdolt_models.Column.objects.filter(table=item, 
                        id__in=models.GroupColumnColumn.objects.filter( group_column__group_table=table ).values("column__id")
                    )

                    time_field = item.column_set.filter( Q( name='last_updated' ) | 
                        Q( name='deletion_time' ) 
                    )

                    query = f"""
                    SELECT { ', '.join( [ column.name for column in columns_in_common ] ) } FROM { table_query_name } { f" WHERE { time_field.first().name } >= {query_placeholder}" if start_time and time_field.exists() and use_time else "" }
                    """

                    try:
                        if start_time and time_field.exists() and use_time: 
                            rows = cursor.execute(query, [start_time])
                        else:
                            rows = cursor.execute(query)

                        columns = [ column[0] for column in cursor.description ]
                        
                        for row in rows:
                            row_dictionary = dict( zip( columns, row ) )
                            table_results.append( row_dictionary )

                        if table_results:
                            table_dictionary.setdefault( "rows", table_results )

                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                        logging.error(f"Error occured when extracting from {group_database.database}.{item.schema.name}.{item.name}. Error: {str(e)}")
                        raise e

                    deletion_table = item.deletion_table

                    if deletion_table:
                        table_deletions = []
                        time_field = 'deletion_time'

                        query = f"""
                        SELECT { ', '.join( [ column.name for column in deletion_table.column_set.all() ] ) } 
                        FROM { deletion_table.get_queryname() } {f"WHERE {time_field} >= {query_placeholder}" if start_time and time_field and use_time else ""}
                        """

                        if start_time and time_field and use_time:
                            rows = cursor.execute( query, [start_time] )  
                        else: 
                            rows = cursor.execute(query)

                        columns = [ column[0] for column in cursor.description ]

                        for row in rows:
                            row_dictionary = dict( zip( columns, row ) )
                            table_deletions.append( row_dictionary )

                        if table_results:
                            table_dictionary.setdefault( "deleted_rows", table_deletions )

                if ( "rows" in table_dictionary and len(table_dictionary["rows"]) > 0 ) or ( "deleted_rows" in table_dictionary and len(table_dictionary["deleted_rows"]) > 0):
                    group_dictionary.setdefault( table.name.lower(), table_dictionary )

            if group_dictionary.keys():
                print("There was data to extract from the group database. Saving the data to a file")
                print(f"The data's keys are: {group_dictionary.keys()}")
                base_file_name = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, 
                "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}")

                file_name = base_file_name + ".json"
                zip_file_name = base_file_name + ".zip"

                group_extraction = None

                with open(file_name, "a+") as file:
                    # creating the json file
                    json_string = json.dumps( results, default=custom_converter )
                    token = f.encrypt( bytes( json_string, "utf-8" ) )

                    file.write( token.decode('utf-8') )

                with zipfile.ZipFile(zip_file_name, mode='a') as archive:
                    # zipping the json file
                    archive.write(file_name, os.path.basename(file_name))
                    
                with open( zip_file_name, "rb" ) as __:
                    file = File.objects.create( 
                        file=DjangoFile( __, name=os.path.basename(zip_file_name) ), 
                        size=os.path.getsize(file_name), is_deleted=False, 
                        hash=hash_file(zip_file_name)
                    )

                    extraction = models.Extraction.objects.create(
                        file=file, 
                        start_time=start_time, 
                        time_made=time_made
                    )

                    group_extraction = models.GroupExtraction.objects.create(
                        group=group, 
                        extraction=extraction, 
                        source_database=group_database
                    )

                    extraction_source_database = flux_models.ExtractionSourceDatabase.objects.create(
                        extraction=extraction, database=group_database.database
                    )

                    for database in target_databases:
                        connection = get_database_connection(database.database)

                        # if connection:
                        group_database_synchronization = models.GroupDatabaseSynchronization.objects.create(extraction=group_extraction, 
                            group_database=database, is_applied=False
                        )
                        flux_models.ExtractionTargetDatabase.objects.create(
                            extraction=extraction, database=database.database, is_applied=False
                        )

                        # if connection:


                os.unlink( file_name )
            
            connection.close()

def synchronize_group(group: models.Group, use_primary_keys_for_verification=False):
    for group_database in group.groupdatabase_set.all():
        synchronize_group_database(group_database)
    
def synchronize_group_database(group_database: models.GroupDatabase, use_primary_keys_for_verification=False):
    errors = []
    group = group_database.group
    f = Fernet(group.get_fernet_key())

    synchronized_databases = []
    applied_synchronizations = []
    
    pending_synchronizations = models.GroupDatabaseSynchronization.objects.filter(
        group_database=group_database, is_applied=False
    ).order_by(
        'extraction__extraction__time_made'
    )

    temporary_tables_created = set([])

    database_record = group_database.database

    connection = get_database_connection(database_record)

    dbms_booleans = get_dbms_booleans(database_record)
    
    if connection: 
        cursor = connection.cursor()

        for group_database_synchronization in pending_synchronizations:
            file_path = group_database_synchronization.extraction.extraction.file.file.path
            successful_flag = True

            try:
                zip_file = zipfile.ZipFile(file_path)

                for __ in zip_file.namelist():
                    print(f"Opening the {__} file in the {zip_file} zip_file")

                    content = zip_file.read(__)
                    content = f.decrypt(content)
                    content = content.decode('utf-8')

                    logging.debug("[In groups.functions.synchronize_group_database]")

                    try:
                        dictionary: dict = json.loads(content)
                        dictionary = dictionary[group.slug]

                        group_table_tables = ( models.GroupTableTable.objects
                            .filter(group_table__name__in=dictionary.keys(), 
                                table__schema__database=database_record
                            ) 
                            .order_by('table__level')
                        )

                        for group_table_table in group_table_tables:
                            table = group_table_table.table
                            table_name = table.name.lower()
                            schema_name = table.schema.name.lower()
                            group_table = group_table_table.group_table
                            
                            group_table_name = group_table.name.lower()

                            table_rows = dictionary[group_table_name]['rows']

                            group_table_columns = group_table_table.group_table.columns.filter(
                                name__in=table_rows[0].keys()
                            )
                            
                            flag = True
                            
                            table_columns = [ f.column.name.lower() for f in models.GroupColumnColumn.objects.filter( group_column__in=group_table_columns, column__table=table ) ]
                            
                            primary_key_columns = [
                                f["name"] for f in table.column_set.filter(columnconstraint__is_primary_key=True).values("name")
                            ]

                            temporary_table_name = f"{schema_name}_{table_name}_temporary_table"
                            temporary_table_actual_name = get_temporary_table_name(database_record, temporary_table_name)
                        
                            create_temporary_table_query = get_create_temporary_table_query( 
                            database_record, temporary_table_name,  
                            f"( { ', '.join( [ get_type_and_precision(column, get_column_dictionary(table, column)) for column in table_columns ] ) } )" 
                            )
                            logging.info(f"Running query to create the temporary table. Query: {create_temporary_table_query}")

                            try:
                                if temporary_table_actual_name not in temporary_tables_created:
                                    logging.info(f"Creating the {temporary_table_actual_name} temp table")
                                    
                                    cursor.execute(create_temporary_table_query)
                                    temporary_tables_created.add( temporary_table_actual_name )

                                try:
                                    # emptying the temporary table in case of previous data
                                    try:
                                        cursor.execute(f"DELETE FROM {temporary_table_actual_name}")
                                    except pyodbc.ProgrammingError as e:
                                        logging.error(f"Error deleting from the temporary_table {temporary_table_actual_name}. Error: {str(e)}")
                                        logging.error(f"The temporary tables that have already been created are: ")
                                        logging.error(temporary_tables_created)
                                        successful_flag = False

                                        connection.rollback()
                                    
                                    insert_into_temporary_table_query = f"""
                                    INSERT INTO {temporary_table_actual_name} 
                                    ( { ', '.join( [ column for column in table_columns ] ) } ) VALUES ( { ', '.join( [ '?' if isinstance(cursor, pyodbc.Cursor) else '%s'  for _ in table_columns ] ) } );
                                    """

                                    rows_to_insert = []
                                    use_time = False
                                    
                                    for row in table_rows:
                                        rows_to_insert.append( tuple(
                                            row[f.name.lower() if not isinstance(f, str) else f] for f in table_columns
                                        ) )
                                        if "last_updated" in row.keys():
                                            use_time = True

                                    
                                    cursor.executemany(insert_into_temporary_table_query, rows_to_insert)

                                    # modify the foreign keys in the table
                                    for constraint in ferdolt_models.ColumnConstraint.objects.filter(
                                        column__table=table, is_foreign_key=True, references_tracking_id__isnull=False, 
                                        references__isnull=False
                                    ):
                                        column = constraint.column
                                        referenced_column = constraint.references
                                        referenced_table = referenced_column.table
                                        tracking_id_referencing_column = constraint.references_tracking_id

                                        referenced_table_tracking_id_name = "tracking_id"

                                        query = f"""
                                        UPDATE {temporary_table_actual_name} SET {column.name} = subquery.{referenced_column.name} 
                                        FROM ( SELECT {referenced_column.name}, {referenced_table_tracking_id_name} FROM {referenced_table.get_queryname()} ) subquery 
                                        WHERE subquery.{referenced_table_tracking_id_name}={temporary_table_actual_name}.{tracking_id_referencing_column.name}
                                        """

                                        try:
                                            cursor.execute(query)
                                        except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                                            logging.error(f"Error occured when modifying the foreign keys in the temporary table. Error: {str(e)}")
                                            logging.error(f"Query to execute: {query}")
                                            connection.rollback()
                                            successful_flag = False

                                            raise e

                                    if dbms_booleans['is_sqlserver_db']:
                                        # set identity_insert on to be able to explicitly write values for identity columns
                                        try:
                                            cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} ON")
                                        except pyodbc.ProgrammingError as e:
                                            logging.error(f"Error occured when setting identity_insert on for {schema_name}.{table_name} table")
                                            connection.rollback()
                                            successful_flag = False
                                            raise e

                                    merge_query = None
                                    
                                    tracking_id_column = "tracking_id"

                                    if len(primary_key_columns) == 1:
                                        non_primary_key_columns_list = [ column for column in table_columns if column not in primary_key_columns ]
                                        non_primary_key_columns_list_string = ', '.join(non_primary_key_columns_list)
                                    else:
                                        non_primary_key_columns_list_string = ', '.join( table_columns )

                                    if not deletion_table_regex.search(table_name):
                                        merge_query = ""

                                        if dbms_booleans["is_sqlserver_db"]:
                                            merge_query = f"""
                                                merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                                    {
                                                        ' AND '.join(
                                                            [ f"t.{column}=s.{column}" for column in primary_key_columns ]
                                                        ) if use_primary_keys_for_verification else f"t.{tracking_id_column}=s.{tracking_id_column}"
                                                    }
                                                )
                                                when matched { " and t.last_updated < s.last_updated " if use_time else ' ' } then 
                                                update set {
                                                    ', '.join(
                                                        [ f"{column} = s.{column}" for column in table_columns if column not in primary_key_columns ]
                                                    )
                                                }

                                                when not matched then 
                                                    insert ( { ', '.join( [ column for column in table_columns ] ) } ) 
                                                    values ( { ', '.join( [ f"s.{column}" for column in table_columns ] ) } )
                                                ;
                                            """

                                        elif dbms_booleans['is_postgres_db']:
                                            merge_query = f"""
                                            INSERT INTO {schema_name}.{table_name} AS source ( { non_primary_key_columns_list_string } ) 
                                            (SELECT { non_primary_key_columns_list_string } FROM {temporary_table_actual_name}) 
                                            ON CONFLICT ( { ', '.join( [ column for column in primary_key_columns ] ) if use_primary_keys_for_verification else tracking_id_column } )
                                            DO 
                                                UPDATE SET { ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns if column not in primary_key_columns ) if use_primary_keys_for_verification else ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns if column != tracking_id_column ) } 
                                                WHERE EXCLUDED.last_updated > source.last_updated;
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
                                                ;
                                                """
                                            elif dbms_booleans['is_postgres_db']:
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

                                    try:
                                        if merge_query: 
                                            cursor.execute(merge_query)
                                            connection.commit()
                                            print(f"Successfully synchronized {schema_name}.{table_name}")
                                            group_database_synchronization.is_applied = True
                                            group_database_synchronization.save()
                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
                                        logging.error(f"The temporary tables that have been created are: {temporary_tables_created}")
                                        flag = False
                                        successful_flag = False
                                        connection.rollback()

                                    except (pyodbc.IntegrityError, psycopg.IntegrityError) as e:
                                        logging.error(f"Error executing merge query\n {merge_query}. \n Exception: {str(e)}")
                                        logging.error(f"The temporary tables that have been created are: {temporary_tables_created}")
                                        cursor.connection.rollback()
                                        flag = False
                                        successful_flag = False
                                    
                                    if dbms_booleans['is_sqlserver_db']:
                                        # set identity_insert on to be able to explicitly write values for identity columns
                                        try:
                                            cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} OFF")
                                        except pyodbc.ProgrammingError as e:
                                            logging.error(f"Error occured when setting identity_insert off for {schema_name}.{table_name} table")

                                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                    logging.error(f"Error inserting into the temporary table. Error: {str(e)}")
                                    logging.error(f"Temp table creation query: {create_temporary_table_query}")
                                    print(f"Temp table creation query: {create_temporary_table_query}")
                                    print(f"Query to insert into the temp table: {insert_into_temporary_table_query}")
                                    flag = False
                                    successful_flag = False
                                    connection.rollback()

                            except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}.\nQuery: {create_temporary_table_query}")
                                logging.error(f"Temp table creation query: {create_temporary_table_query}")
                                cursor.connection.rollback()
                                flag = False
                                successful_flag = False

                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                        logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}.\nQuery: {create_temporary_table_query}")
                        logging.error(f"Temp table creation query: {create_temporary_table_query}")
                        cursor.connection.rollback()
                        flag = False
                        successful_flag = False
                    
                    if successful_flag:
                        connection.commit()                           
                        synchronized_databases.append(database_record)

            except json.JSONDecodeError as e:
                successful_flag = False
                logging.error(f"[In groups.functions.synchronize_group_database]. Error parsing json from file for database synchronization. File path: {file_path}")
            except (zipfile.BadZipFile) as e:
                successful_flag = False
                logging.error(f"[In groups.function.synchronize_group_database]. Error opening zip file")

def get_data_type_specification_for_group_column(group_column: models.GroupColumn) -> str:
    if group_column.data_type in ["varchar", "char"]:
        return f"{group_column.data_type}({group_column.character_maximum_length})"
    else:
        return group_column.data_type

def create_non_existing_group_tables_in_group_database(group_database: models.GroupDatabase):
    group = group_database.group

    connection = get_database_connection(group_database.database)

    if connection:
        cursor = connection.cursor()

        for group_table in group.tables.all():
            logging.info(f"Creating the {group_table.name} table in the {group_database.database} database")
            
            group_table_table_query = models.GroupTableTable.objects.filter( group_table=group_table, table__schema__database=group_database.database )

            if not group_table_table_query.exists():
                # create the table in the database and register it locally
                query_to_create_table = f"""
                CREATE TABLE {group_table.name} 
                ( { ', '.join( [ get_data_type_specification_for_group_column( group_column ) for group_column in group_table.columns.all() ] ) } )
                """

                try:
                    cursor.execute(query_to_create_table)
                    # connection.commit()
                    # schema = group_database.database.get_default_schema()

                    # table = ferdolt_models.Table.objects.create(schema=schema, name=group_table.name)

                    # for group_column in group_table.columns.all():
                    #     column = ferdolt_models.Column.objects.create(
                    #         table=table, name=group_column.name, data_type=group_column.data_type
                    #     )

                except (psycopg.ProgrammingError, pyodbc.ProgrammingError) as e:
                    logging.error(f"Error creating the {group_table.name} table in the {group_database.database} database")
                    successful_flag = False
                    connection.rollback()
                    raise e
        
        connection.commit()
        
        initialize_database(group_database.database)

def test_group_extraction(group: models.Group, duration: int=5*60, interval=30):
    time_elapsed = 0
    group_databases = group.groupdatabase_set.all()

    while True:
        if duration is not None and time_elapsed >= duration:
            break

        for group_database in group_databases:
            print(f"Extracting from the {group_database.database} database")
            extract_from_groupdatabase(group_database)

        print(f'Sleeping for {interval} seconds')
        sleep(interval)
        time_elapsed += interval

def test_group_synchronization(group: models.Group, duration: int=5*60, interval=30):
    time_elapsed = 0

    while True:
        if duration is not None and time_elapsed >= duration:
            break

        synchronize_group(group)
        
        print(f'Sleeping for {interval} seconds')
        sleep(interval)
        time_elapsed += interval