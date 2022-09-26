from hashlib import sha256
import json
import logging
import os
import zipfile

from cryptography.fernet import Fernet

from django.core.files import File as DjangoFile
from django.db.models import Q
from django.utils import timezone

import psycopg
import pyodbc

from core.functions import custom_converter, get_database_connection

from ferdolt import models as ferdolt_models
from ferdolt_web import settings
from flux.models import File
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
            latest_extraction = models.GroupExtraction.objects.order_by('-extraction_time_made').first()

            if latest_extraction:
                start_time = latest_extraction.time_made
    
    time_made = timezone.now()

    results = {}

    if not target_databases:
        target_databases = group.groupdatabase_set.filter(can_read=True)

    group_dictionary = results.setdefault( group.slug, {} )

    connection = get_database_connection(group_database.database)

    if connection:
        cursor = connection.cursor()

        for table in group.tables.all():
            # get the tables of this database linked to the group's tables
            actual_database_tables = ferdolt_models.Table.objects.filter( id__in=table.grouptabletable_set.values("table__id"), 
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
                SELECT { ', '.join( [ column.name for column in columns_in_common ] ) } FROM 
                { table_query_name } { f" WHERE { time_field.first().name } >= ?" if start_time and time_field.exists() and use_time else "" }
                """

                try:
                    rows = cursor.execute(query, start_time) if start_time and time_field.exists() and use_time else cursor.execute(query)

                    columns = [ column[0] for column in cursor.description ]
                    
                    for row in rows:
                        row_dictionary = dict( zip( columns, row ) )
                        table_results.append( row_dictionary )

                    if table_results:
                        table_dictionary.setdefault( "rows", table_results )

                except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                    logging.error(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                    raise e

                deletion_table = table.deletion_table
                if deletion_table:
                    table_deletions = []
                    time_field = 'deletion_time'

                    query = f"""
                    SELECT { ', '.join( [ column.name for column in deletion_table.column_set.all() ] ) } 
                    FROM { deletion_table.get_queryname() } {f"WHERE {time_field} >= ?" if start_time and time_field.exists() and use_time else ""}
                    """

                    rows = cursor.execute( query, start_time ) if start_time and time_field and use_time else cursor.execute(query)

                    columns = [ column[0] for column in cursor.description ]

                    for row in rows:
                        row_dictionary = dict( zip( columns, row ) )
                        table_deletions.append( row_dictionary )

                    if table_results:
                        table_dictionary.setdefault( "deleted_rows", table_deletions )

            
            if table_dictionary.keys():
                group_dictionary.setdefault( table.name.lower(), table_dictionary )
                
                base_file_name = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, 
                "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}")

                file_name = base_file_name + ".json"
                zip_file_name = base_file_name + ".zip"

                group_extraction = None

                with open(file_name, "a+") as file:
                    # creating the json file
                    json_string = json.dumps( results, default=custom_converter )
                    token = f.encrypt( bytes( json_string ), "utf-8" )

                    file.write( token.decode('utf-8') )

                with zipfile.ZipFile(zip_file_name, mode='a') as archive:
                    # zipping the json file
                    archive.write(file_name, os.path.basename(file_name))
                    
                with open( zip_file_name, "a+" ) as __:
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

                    for database in target_databases:
                        connection = get_database_connection(database)

                        if connection:
                            models.GroupDatabaseSynchronization.objects.create(extraction=group_extraction, 
                                group_database=database, is_applied=False
                            )

                os.unlink( file_name )
                
def synchronize_group(group: models.Group):
    errors = []
    f = Fernet(group.get_fernet_key())
    
    synchronized_databases = []
    applied_synchronizations = []

    for group_database_synchronization in models.GroupDatabaseSynchronization.objects.filter( 
        group_database__group=group, is_applied=False 
    ).order_by('extraction__extraction__time_made'):
        # apply the synchronization for this database
        file_path = group_database_synchronization.extraction.extraction.file.file.path

        try:
            zip_file = zipfile.ZipFile(file_path)

            for __ in zip_file.namelist():
                content = zip_file.read(__)
                content = f.decrypt( bytes( content, 'utf-8' ) )
                content = content.decode('utf-8')

                logging.debug("[In groups.views.GroupViewSet.synchronize]")

                try:
                    # the keys of this dictionary are the group's tables
                    dictionary: dict = json.loads(content)
                    dictionary = dictionary[group.slug]

                    for group_table_name in dictionary.keys():
                        try:
                            group_table = group.tables.get( name__iexact=group_table_name )
                            # getting the database tables associated to this group_table
                            group_table_tables = group_table.grouptabletable_set.all()
                            group_table_columns = [ group_column for group_column in group_table.columns.all() if group_column.name.lower() in dictionary[group_table_name]['rows'][0] ]

                            temporary_tables_created = set([])

                            for group_table_table in group_table_tables:
                                table = group_table_table.table
                                table_name = table.name.lower()
                                schema_name = table.schema.name.lower()
                                flag = True

                                database_record = table.schema.database
                                connection = get_database_connection( database_record )
                                breakpoint()

                                if connection:
                                    cursor = connection.cursor()
                                    dbms_booleans = get_dbms_booleans(database_record)
                                    
                                    breakpoint()

                                    table_columns = [ f["column__name"].lower() for f in models.GroupColumnColumn.objects.filter( group_column__in=group_table_columns, column__table=table ).values("column__name") ] 
                                    
                                    primary_key_columns = [
                                        f["name"] for f in table.column_set.filter(columnconstraint__is_primary_key=True).values("name")
                                    ]
                                    table_rows = dictionary[group_table_name]['rows']

                                    temporary_table_name = f"{schema_name}_{table_name}_temporary_table"
                                    temporary_table_actual_name = get_temporary_table_name(database_record, temporary_table_name)
                                
                                    create_temporary_table_query = get_create_temporary_table_query( 
                                    database_record, temporary_table_name,  
                                    f"( { ', '.join( [ get_type_and_precision(column, get_column_dictionary(table, column)) for column in table_columns ] ) } )" 
                                    )

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
                                                connection.rollback()
                                            
                                            insert_into_temporary_table_query = f"""
                                            INSERT INTO {temporary_table_actual_name} ( { ', '.join( [ column for column in table_columns ] ) } ) VALUES ( { ', '.join( [ '?' if isinstance(cursor, pyodbc.Cursor) else '%s'  for _ in table_columns ] ) } );
                                            """

                                            rows_to_insert = []
                                            use_time = False

                                            for row in table_rows:
                                                rows_to_insert.append( tuple(
                                                    row[f.name.lower()] for f in group_table_columns
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

                                            if not deletion_table_regex.search(table_name):
                                                merge_query = ""

                                                if dbms_booleans["is_sqlserver_db"]:
                                                    merge_query = f"""
                                                        merge {schema_name}.{table_name} as t USING {temporary_table_actual_name} AS s ON (
                                                            {
                                                                ' AND '.join(
                                                                    [ f"t.{column}=s.{column}" for column in primary_key_columns ]
                                                                )
                                                            }
                                                        )
                                                        when matched { " and t.last_updated < s.last_updated then " if use_time else ' ' } then 
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
                                                    INSERT INTO {schema_name}.{table_name} (SELECT * FROM {temporary_table_actual_name}) 
                                                    ON CONFLICT ( { ', '.join( [ column for column in primary_key_columns ] ) } )
                                                    DO 
                                                        UPDATE SET { ', '.join( f"{column} = EXCLUDED.{column}" for column in table_columns if column not in primary_key_columns ) }
                                                    """
                                                
                                                breakpoint()
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
                                                flag = False
                                            except (pyodbc.IntegrityError, psycopg.IntegrityError) as e:
                                                logging.error(f"Error executing merge query\n {merge_query}. \n Exception: {str(e)}")
                                                cursor.connection.rollback()
                                                flag = False
                                            
                                            if dbms_booleans['is_sqlserver_db']:
                                                # set identity_insert on to be able to explicitly write values for identity columns
                                                try:
                                                    cursor.execute(f"SET IDENTITY_INSERT {schema_name}.{table_name} OFF")
                                                except pyodbc.ProgrammingError as e:
                                                    logging.error(f"Error occured when setting identity_insert off for {schema_name}.{table_name} table")

                                        except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                            logging.error(f"Error inserting into the temporary table")
                                            flag = False

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}.\nQuery: {create_temporary_table_query}")
                                        cursor.connection.rollback()
                                        flag = False
                                    
                                    if flag:
                                        connection.commit()                           
                                        synchronized_databases.append(database_record)
                                    
                                    connection.close()

                                else:
                                    logging.error( _("Invalid connection parameters") )
                                    errors.append(_("Couldn't connect to the %(db_name)s. Please ensure your connection details are correct and your server is running " % {'db_name': database_record.name.lower()}))

                        except models.GroupTable.DoesNotExist as e:
                            logging.error(f"No group table with name {group_table_name} exists in this group")
                            errors.append(_("Table %(table_name)s does not exists in group %(group_name)s" % {'table_name': group_table_name, 'group_name': group.name } ))

                except json.JSONDecodeError as e:
                    logging.error( f"[In flux.views.GroupViewSet.synchronize]. Error parsing json from file for database synchronization. File path: {file_path}" )

            zip_file.close()
        except FileNotFoundError as e:
            flag = False 
            logging.error( f"[In flux.GroupViewSet.synchronize]. Error opening file for database synchronization. File path: {file_path}" )

        group_database_synchronization.is_applied = True
        group_database_synchronization.time_applied = timezone.now()

        applied_synchronizations.append( group_database_synchronization )

