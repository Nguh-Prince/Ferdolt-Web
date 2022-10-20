import json
from hashlib import sha256
import logging
import os
from pathlib import Path
import re
import zipfile

from cryptography.fernet import Fernet

from django.core.files import File as DjangoFile
from django.db.models import Count, Q
from django.http import HttpResponse
from django.utils import timezone
from django.utils.translation import gettext as _

import pyodbc
import psycopg

from rest_framework import permissions as drf_permissions
from rest_framework import status
from rest_framework import viewsets
from rest_framework.decorators import action, parser_classes
from rest_framework.parsers import FileUploadParser

from rest_framework.response import Response
from common.permissions import IsStaff
from common.responses import get_error_response

from common.viewsets import MultipleSerializerViewSet
from core.functions import custom_converter, get_create_temporary_table_query, get_database_connection, get_dbms_booleans, get_temporary_table_name, synchronize_database
from ferdolt_web import settings

from ferdolt_web.settings import FERNET_KEY
from ferdolt import models as ferdolt_models
from flux.models import Extraction, File
from flux.serializers import ExtractionSerializer
from flux.views import get_column_dictionary, get_type_and_precision
from frontend.views import synchronizations
from . import models, serializers

deletion_table_regex = re.compile("_deletion$")

class GroupViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
    serializer_class = serializers.GroupSerializer
    # permission_classes = [ IsStaff, ]

    serializer_classes = {
        'create': serializers.GroupCreationSerializer,
        'extract': serializers.ExtractFromGroupSerializer,
        'link_columns': serializers.LinkColumnsToGroupColumnsSerializer,
        'list': serializers.GroupDisplaySerializer,
        'retrieve': serializers.GroupDetailSerializer,
        'synchronization_group': serializers.SynchronizationGroupSerializer,
        'add_database': serializers.AddDatabaseToGroupSerializer,
        'server_pending_synchronizations': serializers.ServerPendingSynchronizationsSerializer
    }

    def get_queryset(self):
        return models.Group.objects.all()
    
    @action(
        methods=['POST'],
        detail=True
    )
    def extract(self, request, *args, **kwargs):
        # extraction is done from one database but synchronization is done for all the databases
        group = self.get_object()
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        source = serializer.validated_data['source_database']

        validated_data = serializer.validated_data
        f = Fernet(group.get_fernet_key())

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = [source]

            start_time = None

            use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )

            if use_time:
                if 'start_time' in validated_data:
                    start_time = validated_data.pop('start_time')
                else:
                    # get the creation time of the latest extraction
                    latest_extraction_query = models.GroupExtraction.objects.order_by('-extraction__time_made')

                    if latest_extraction_query.exists():
                        start_time = latest_extraction_query.first().extraction.time_made
                    else: 
                        start_time = None

            time_made = timezone.now()

            results = {}

            target_databases = validated_data.pop('target_databases')

            group_dictionary = results.setdefault( group.slug, {} )

            for database in databases:
                database_record = database

                # get the time of the last extraction of this group from this source
                if not start_time and use_time:
                    latest_extraction = Extraction.objects.filter( 
                        groupextraction__group=group, groupextraction__source_database=source
                        ).last()

                    if latest_extraction:
                        start_time = latest_extraction.time_made

                connection = get_database_connection(database_record.database)

                if connection:
                    cursor = connection.cursor()

                    for table in group.tables.all():
                        # get the tables of this database linked to the group's tables
                        actual_database_tables = ferdolt_models.Table.objects.filter( id__in=table.grouptabletable_set.values("table__id"), 
                            schema__database=source.database 
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
                            
                            file_name = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, 
                            "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}.json")

                    group_extraction = None

                    with open( file_name, "a+" ) as __:
                        json_string = json.dumps( results, default=custom_converter )
                        token = f.encrypt( bytes( json_string, 'utf-8' ) )

                        __.write( token.decode('utf-8') )
                        
                        file = File.objects.create( file=DjangoFile( __, name=os.path.basename(file_name) ), size=os.path.getsize(file_name), is_deleted=False, hash=sha256( token ).hexdigest() 
                        )

                        extraction = models.Extraction.objects.create(file=file, start_time=start_time, time_made=time_made)

                        group_extraction = models.GroupExtraction.objects.create(group=group, extraction=extraction, source_database=source)

                        for database in target_databases:
                            connection = get_database_connection(database)

                            if connection:
                                models.GroupDatabaseSynchronization.objects.create(extraction=group_extraction, 
                                    group_database=database, is_applied=False
                                )

                    os.unlink( file_name )
                    
                    serializer = serializers.GroupExtractionSerializer(group_extraction)

                else:
                    return Response( data={'message': _('Error connecting to the %(database)s database. Please ensure that the database server is running and your connection credentials are correct'
                    % {'database': database_record.database})}, status=status.HTTP_400_BAD_REQUEST )
                    
            return Response( data=serializer.data, status=status.HTTP_201_CREATED )

        use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )
    
    @action(
        methods=['GET'],
        detail=True
    )

    @action(
        methods=['POST'],
        detail=True
    )
    def synchronize(self, request, *args, **kwargs):
        errors = []
        group: models.Group = self.get_object()
        f = Fernet(group.get_fernet_key())
        
        synchronized_databases = []
        applied_synchronizations = []

        for group_database_synchronization in models.GroupDatabaseSynchronization.objects.filter( group_database__group=group, is_applied=False ):
            # apply the synchronization for this database
            file_path = group_database_synchronization.extraction.extraction.file.file.path

            try:
                with open( file_path ) as __:
                    content = __.read()
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

                                    if connection:
                                        cursor = connection.cursor()
                                        dbms_booleans = get_dbms_booleans(database_record)

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

            except FileNotFoundError as e:
                flag = False 
                logging.error( f"[In flux.GroupViewSet.synchronize]. Error opening file for database synchronization. File path: {file_path}" )

            group_database_synchronization.is_applied = True
            group_database_synchronization.time_applied = timezone.now()

            applied_synchronizations.append( group_database_synchronization )

        return Response( data=serializers.GroupDatabaseSynchronizationSerializer( applied_synchronizations, many=True ).data )

    @action(
        methods=['GET'],
        detail=True
    )
    def synchronizations(self, request, *args, **kwargs):
        synchronizations = models.GroupDatabaseSynchronization.objects.all()

        return Response( serializers.GroupDatabaseSynchronizationSerializer(synchronizations, many=True).data )

    @action(
        methods=['PATCH'],
        detail=True
    )
    def link_columns(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        created_group_column_columns = []

        for item in serializer.validated_data['data']:
            group_column = item['group_column']
            column = item['column']

            group_column_column = models.GroupColumnColumn.objects.create( column=column, group_column=group_column )
            
            created_group_column_columns.append(group_column_column)

        serializer = serializers.GroupColumnColumnSerializer( created_group_column_columns, many=True )

        return Response( data=serializer.data )

    @action(
        methods=['POST'],
        detail=False
    )
    def synchronization_group(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data

        group = models.Group.objects.create(name=validated_data['group_name'])
        logging.info("Created the group successfully. Adding tables to the group now")

        group_databases = []
        group_tables_names = set([])

        if validated_data['type'] == 'full':
            source_databases_set = set(validated_data['sources'])
            participant_databases_set = set(validated_data['participants'])

            all_databases = source_databases_set.union(participant_databases_set)

            for database in all_databases:
                if database in source_databases_set:
                    logging.info(f"Creating a read/write database using {database}")
                    group_database = models.GroupDatabase.objects.create(
                        group=group, database=database, can_write=True, can_read=True
                    )
                else:
                    logging.info(f"Creating a read-only database using {database}")
                    group_database = models.GroupDatabase.objects.create(group=group, database=database, can_read=True)
                
                group_databases.append(group_database)

            # getting the non-deletion tables from the source databases
            for table in ferdolt_models.Table.objects.filter(
                Q(schema__database__in=source_databases_set) & 
                ~Q(id__in=ferdolt_models.Table.objects.filter(deletion_table__isnull=False)
                .values("deletion_table__id"))
            ):
                group_table_name = f"{table.schema.name}__{table.name}"
                table_columns = table.column_set.all()
                table_database = table.schema.database
                
                # create the group tables based on the tables in the source databases
                # table has not been added to the group, create a new group table and a grouptabletable for the table
                if group_table_name not in group_tables_names and table_database in source_databases_set:
                    logging.info(f"Creating a new group table with name {group_table_name}")

                    group_table = models.GroupTable.objects.create(group=group, name=group_table_name)
                    models.GroupTableTable.objects.create(group_table=group_table, table=table)
                    group_tables_names.add(group_table_name)
                    logging.info(f"Linking the {table} table to the {group_table} table")
                else:
                    # table already exists in the group, create the grouptabletable for the table
                    try:
                        group_table = models.GroupTable.objects.get(name=group_table_name, group=group)
                        models.GroupTableTable.objects.create(group_table=group_table, table=table)
                        logging.info(f"Linking the {table} table to the {group_table} table")
                    except models.GroupTable.DoesNotExist as e:
                        logging.error(f"Error when creating full synchronization group. Error: {str(e)}")

                        if table_database in source_databases_set:
                            return get_error_response()

                # query to get the columns that are in this table but not in the group
                table_columns_query = table_columns.filter( 
                    ~Q(name__in=group_table.columns.values("name")) 
                )

                for column in table_columns:
                    if column in table_columns_query:
                        # create a new groupcolumn
                        group_column = models.GroupColumn.objects.create(
                            name=column.name, group_table=group_table, 
                            data_type=column.data_type, is_nullable=column.is_nullable
                        )
                        logging.info(f"Creating the {column.name} column in the {group_table.name} table in the {group.name} group")
                    else:
                        # get the existing group column from the database
                        try:
                            group_column = models.GroupColumn.objects.get(
                                name=column.name, group_table=group_table
                            )
                        except models.GroupColumn.DoesNotExist as e:
                            logging.error(f"Error when creating full synchronization group. Error: {str(e)}")
                            return get_error_response()
                    
                    # link the group column to the table column
                    models.GroupColumnColumn.objects.create(group_column=group_column, column=column)
                    logging.info("Linked the {column.name} column in the {column.table.__str__()} table to the {group_column.name} column in the {group_table.name} table in {group_table.group} group")
                    print("Linked the {column.name} column in the {column.table.__str__()} table to the {group_column.name} column in the {group_table.name} table in {group_table.group} group")

            # link the table
            for table in ferdolt_models.Table.objects.filter(schema__database__in=participant_databases_set - source_databases_set):
                group_table_name = f"{table.schema.name}__{table.name}"
                table_columns = table.column_set.all()
                table_database = table.schema.database

                if group_table_name in group_tables_names:
                    group_table = models.GroupTable.objects.get(name=group_table_name, group=group)

                    for group_column in group_table.columns:
                        column_query = table_columns.filter(name=group_column.name)

                        if column_query.exists():
                            models.GroupColumnColumn.objects.create(
                                group_column=group_column, column=column_query.first()
                            )
                        else:
                            # create column 
                            pass
            
            return Response( serializers.GroupDetailSerializer(group).data, status=status.HTTP_201_CREATED)
        else:
            pass

    @action(
        methods=["POST"],
        detail=True
    )
    def add_database(self, request, *args, **kwargs):
        group = self.get_object()
        
        serializer = self.get_serializer(request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data

        database = validated_data.pop('database')
        
        validated_data.pop('database_object')

        group_database = models.GroupDatabase.objects.create(
            database=database, **validated_data
        )

        return Response(
            data=serializers.GroupDatabaseSerializer(group_database).data
        )

    @action(
        methods=["GET"],
        detail=True
    )
    def server_pending_synchronizations(self, request, *args, **kwargs):
        group: models.Group = self.get_object()
        
        f = Fernet(group.get_fernet_key())

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data

        # group_server = validated_data['group_server']

        # extractions = models.GroupExtraction.objects.filter(
        #     id__in=models.GroupServerSynchronization.objects.filter(group_server=group_server, is_applied=False)
        # )
        extractions = models.GroupExtraction.objects.all()
        
        serialized_extractions = serializers.SimpleGroupExtractionSerializer(extractions, many=True)

        zip_file_name = os.path.join( 
            settings.BASE_DIR, settings.MEDIA_ROOT, "downloads", "groupservers"
        )
        
        temp_json_file = os.path.join(
            settings.BASE_DIR, settings.MEDIA_ROOT, "temp"
        )
        
        Path(zip_file_name).mkdir(parents=True, exist_ok=True)
        Path(temp_json_file).mkdir(parents=True, exist_ok=True)

        formatted_time = timezone.now().strftime('%Y%m%d%H%M%S')

        zip_file_name = os.path.join(
            zip_file_name, f"{formatted_time}.zip"
        )
        temp_json_file = os.path.join(
            temp_json_file, f"{formatted_time}.json"
        )

        # writing the details of the extractions into a file
        with open(temp_json_file, "w") as __:
            json_string = json.dumps(serialized_extractions.data)
            token = f.encrypt( bytes(json_string, 'utf-8') )

            __.write( token.decode('utf-8') )

        # adding the file with the extraction details to the archive
        with zipfile.ZipFile( zip_file_name, mode='a' ) as archive:
            archive.write(temp_json_file, os.path.basename(temp_json_file))

        for extraction in extractions:
            extraction_file_path = extraction.extraction.file_path

            if not os.path.exists(extraction_file_path):
                extraction.file.is_deleted = True
                extraction.file.save()
            else:
                with zipfile.ZipFile( zip_file_name, mode='a' ) as archive:
                    archive.write(extraction_file_path, os.path.basename(extraction_file_path))

        fh = open(zip_file_name, 'rb')
        
        response = HttpResponse( DjangoFile(fh), content_type='application/zip' )
        response['Content-Disposition'] = 'attachment; filename="%s"' % 'CDX_COMPOSITES_20140626.zip'
        return response


class GroupExtractionViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
    serializer_class = serializers.GroupExtractionSerializer
    permission_classes = [ IsStaff, ]

    group_lookup_key = "parent_lookup_group"

    def get_queryset(self):
        if self.group_lookup_key in self.kwargs:
            self.group = models.Group.objects.filter(id=self.kwargs[self.group_lookup_key]).first()

            return models.GroupExtraction.objects.filter(
                group__id=self.kwargs[self.group_lookup_key]
            )

        return models.GroupExtraction.objects.all()

    @action(methods=['POST'], detail=False)
    @parser_classes( [FileUploadParser, ] )
    def upload(self, request):
        if not self.group_lookup_key in self.kwargs:
            return Response( data={'message': "This method can only be accessed from within a group"}, status=status.HTTP_405_METHOD_NOT_ALLOWED )
        
        file_name = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}" ) + ".zip"

        up_file = request.FILES['file']
        
        with open(file_name) as __:
            for chunk in up_file.chunks:
                __.write(chunk)
        
        return Response( {"message": _("File %(name)s uploaded successfully" % { 'name': up_file.name })} )
