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

from core.functions import (
    custom_converter, get_column_dictionary, get_create_temporary_table_query, 
    get_database_connection, get_dbms_booleans, get_temporary_table_name, 
    get_type_and_precision, deletion_table_regex, get_query_placeholder
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

                if table_dictionary.keys():
                    group_dictionary.setdefault( table.name.lower(), table_dictionary )

            if results.keys():
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
                        file=DjangoFile( __, name=os.path.basename(file_name) ), 
                        size=os.path.getsize(file_name), is_deleted=False, 
                        hash=sha256( token ).hexdigest() 
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
                        models.GroupDatabaseSynchronization.objects.create(extraction=group_extraction, 
                            group_database=database, is_applied=False
                        )
                        flux_models.ExtractionTargetDatabase.objects.create(
                            extraction=extraction, database=database.databse, is_applied=False
                        )

                os.unlink( file_name )
            
            connection.close()

def synchronize_group(group: models.Group, use_primary_keys_for_verification=False):
    errors = []
    f = Fernet(group.get_fernet_key())
    
    synchronized_databases = []
    applied_synchronizations = []

    pending_synchronizations = models.GroupDatabaseSynchronization.objects.filter( 
        group_database__group=group, is_applied=False 
    ).annotate(database=F('group_database__database')).order_by('extraction__extraction__time_made')

    for database in pending_synchronizations.values("database").distinct():
        temporary_tables_created = set([])

        database_record = ferdolt_models.Database.objects.get(id=database['database'])

        connection = get_database_connection(database_record)

        dbms_booleans = get_dbms_booleans(database_record)

        if connection: 
            cursor = connection.cursor()

            for group_database_synchronization in pending_synchronizations.filter(database=database['database']):
                # apply the synchronization for this database
                file_path = group_database_synchronization.extraction.extraction.file.file.path
                successful_flag = True

                try:
                    zip_file = zipfile.ZipFile(file_path)

                    for __ in zip_file.namelist():
                        print(f"Opening the {__} file in the {zip_file} zip_file")

                        content = zip_file.read(__)
                        content = f.decrypt( content )
                        content = content.decode('utf-8')

                        logging.debug("[In groups.views.GroupViewSet.synchronize]")

                        try:
                            # the keys of this dictionary are the group's tables
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
                                        INSERT INTO {temporary_table_actual_name} ( { ', '.join( [ column for column in table_columns ] ) } ) VALUES ( { ', '.join( [ '?' if isinstance(cursor, pyodbc.Cursor) else '%s'  for _ in table_columns ] ) } );
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

                                        if dbms_booleans['is_sqlserver_db']:
                                            # set identity_insert on to be able to explicitly write values for identity columns
                                            try:
                                                cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} ON")
                                            except pyodbc.ProgrammingError as e:
                                                logging.error(f"Error occured when setting identity_insert on for {schema_name}.{table_name} table")

                                        merge_query = None
                                        
                                        tracking_id_column = "tracking_id"

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
                                                INSERT INTO {schema_name}.{table_name} ( { ', '.join( [ column for column in table_columns ] ) } ) 
                                                (SELECT { ', '.join( [ column for column in table_columns ] ) } FROM {temporary_table_actual_name}) 
                                                ON CONFLICT ( { ', '.join( [ column for column in primary_key_columns ] ) if use_primary_keys_for_verification else tracking_id_column } )
                                                DO 
                                                    UPDATE SET { ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns if column not in primary_key_columns ) if use_primary_keys_for_verification else ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns) }
                                                """
                                            
                                        else:
                                            if len(primary_key_columns) == 1:
                                                if dbms_booleans["is_postgres_db"]:
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
                                        except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                            logging.error(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
                                            logging.error(f"The temporary tables that have been created are: {temporary_tables_created}")
                                            flag = False
                                            successful_flag = False
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

                                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                    logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}.\nQuery: {create_temporary_table_query}")
                                    logging.error(f"Temp table creation query: {create_temporary_table_query}")
                                    cursor.connection.rollback()
                                    flag = False
                                    successful_flag = False
                                
                                if flag:
                                    connection.commit()                           
                                    synchronized_databases.append(database_record)

                        except json.JSONDecodeError as e:
                            successful_flag = False
                            logging.error( f"[In flux.views.GroupViewSet.synchronize]. Error parsing json from file for database synchronization. File path: {file_path}" )
                            group_database_synchronization.delete()

                    zip_file.close()
                except FileNotFoundError as e:
                    successful_flag = False 
                    logging.error( f"[In flux.GroupViewSet.synchronize]. Error opening file for database synchronization. File path: {file_path}" )
                    # delete synchronization if the file is not found
                    group_database_synchronization.delete()

                if successful_flag:
                    group_database_synchronization.is_applied = True
                    group_database_synchronization.time_applied = timezone.now()

                    applied_synchronizations.append( group_database_synchronization )
                    group_database_synchronization.save()
            
            
            connection.close()

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