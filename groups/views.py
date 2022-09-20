import json
from hashlib import sha256
import logging
import os
import re

from cryptography.fernet import Fernet

from django.core.files import File as DjangoFile
from django.db.models import Count, Q
from django.utils import timezone
from django.utils.translation import gettext as _

import pyodbc

import psycopg

from rest_framework import permissions as drf_permissions
from rest_framework import status
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from common.permissions import IsStaff

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
    permission_classes = [ IsStaff, ]

    serializer_classes = {
        'create': serializers.GroupCreationSerializer,
        'extract': serializers.ExtractFromGroupSerializer,
        'link_columns': serializers.LinkColumnsToGroupColumnsSerializer,
        'retrieve': serializers.GroupDetailSerializer,
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
                        start_time = latest_extraction_query.first().time_made
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

    @action(
        methods=['GET'],
        detail=True
    )
    def extractions(self, request, *args, **kwargs):
        object = self.get_object()

        extractions = Extraction.objects.filter( groupextraction__group=object )

        return Response( ExtractionSerializer(extractions, many=True).data )

    @action(
        methods=['POST'],
        detail=True
    )
    def synchronize(self, request, *args, **kwargs):
        errors = []
        group = self.get_object()
        f = Fernet(FERNET_KEY)
        
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

                        breakpoint()

                        for group_table_name in dictionary.keys():
                            try:
                                group_table = group.tables.get( name__iexact=group_table_name )
                                breakpoint()

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