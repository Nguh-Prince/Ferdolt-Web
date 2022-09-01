from heapq import merge
import json
import logging
import psycopg
import pyodbc
import re

from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _

from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from core.functions import get_create_temporary_table_query, get_dbms_booleans, get_temporary_table_name

from flux import serializers

from . import models
from .serializers import SynchronizationSerializer

from ferdolt import models as ferdolt_models
from frontend.views import get_database_connection

class FileViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.FileSerializer
    def get_queryset(self):
        return models.File.objects.all()

    def destroy(self, request, *args, **kwargs):
        return Response( data={"message": _("This method is not allowed")}, status=status.HTTP_401_UNAUTHORIZED )

    def update(self, request, *args, **kwargs):
        return Response( data={"message": _("This method is not allowed")}, status=status.HTTP_401_UNAUTHORIZED )

class ExtractionViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.ExtractionSerializer

    def get_queryset(self):
        return models.Extraction.objects.all()

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

deletion_table_regex = re.compile("_deletion$")

class SynchronizationViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.SynchronizationSerializer

    def get_queryset(self):
        return models.Synchronization.objects.all()

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data

        logging.info("[In flux.views.SynchronizationViewSet.create]")

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("synchronizationdatabase_set")

            synchronizations = []
            synchronized_databases = []

            for database in databases:
                database_record = ferdolt_models.Database.objects.filter(id=database['database']['id'])
                
                if database_record.exists():
                    database_record = database_record.first()
                    connection = get_database_connection(database_record)
                    
                    dbms_booleans = get_dbms_booleans(database_record)

                    flag = True

                    if connection:
                        cursor = connection.cursor()

                        # get the list of files (not extracted from this database) that have not been applied on the database
                        unapplied_files = models.File.objects.filter(~Q(id__in=database_record.synchronizationdatabase_set.values("synchronization__file__id") ) & ~Q(id__in=database_record.extractiondatabase_set.values("extraction__file__id")) )

                        temporary_table_created_flag = False

                        for file in unapplied_files:
                            file_path = file.file.path

                            try:
                                with open( file_path ) as _:
                                    content = _.read()
                                    logging.debug("[In flux.views.SynchronizationViewSet.create] reading the unapplied synchronization file")
                                    try:
                                        dictionary: dict = json.loads(content)
                                        
                                        # if there is more than one key in the extraction file check if a key with the database's name exists in the file
                                        dictionary_key = list(dictionary.keys())[0] if len(dictionary.keys()) == 1 else None

                                        if database_record.name.lower() in dictionary.keys():
                                            dictionary_key = database_record.name.lower()

                                        if not dictionary_key:
                                            logging.error( f"[In flux.views.SynchronizationViewSet.create]. The database {database_record.name.lower()} does not exist in the synchronization file. File path: {file_path}" )
                                        else:
                                            item: dict = dictionary[dictionary_key]
                                            
                                            # we order by levels (ascending) in order to avoid integrity errors
                                            # tables with the lowest levels are those with the least number of foreign key relationships
                                            # it's also not just about number but the depth of those relationships i.e. Child -> Parent -> Grand parent -> Adam in this case the child's level is 3
                                            tables = ferdolt_models.Table.objects.filter( Q(schema__database=database_record) & ~Q(name__icontains='_deletion') ).order_by('level')

                                            for table in tables:
                                                table_name = table.name.lower()
                                                schema_name = table.schema.name.lower()
                                                
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
                                                        if not temporary_table_created_flag:
                                                            create_temporary_table_query = get_create_temporary_table_query( database_record, temporary_table_name,  f"( { ', '.join( [ get_type_and_precision(column, get_column_dictionary(table, column)) for column in table_columns ] ) } )" )
                                                            
                                                            cursor.execute(create_temporary_table_query)
                                                            temporary_table_created_flag = True
                                                            breakpoint()

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
                                                            breakpoint()
                                                            rows_to_insert = []

                                                            # getting the list of tables for bulk insert
                                                            for row in table_rows:
                                                                rows_to_insert.append( tuple(
                                                                    row[f] for f in table_columns
                                                                ) )

                                                            cursor.executemany(insert_into_temporary_table_query, rows_to_insert)
                                                            breakpoint()
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

                                                            # execute merge query
                                                            # merge is used to either insert update or do nothing based on certain conditions
                                                            try:
                                                                if merge_query:
                                                                    cursor.execute(merge_query)
                                                                    breakpoint()
                                                            except pyodbc.ProgrammingError as e:
                                                                logging.error(f"Error executing merge query \n{merge_query}. \nException: {str(e)}")
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
                                                            breakpoint()
                                                            cursor.connection.rollback()
                                                            flag = False

                                                    except pyodbc.ProgrammingError as e:
                                                        logging.error(f"Error creating the temporary table {temporary_table_actual_name}. Error: {str(e)}")
                                                        cursor.connection.rollback()
                                                        flag = False

                                            if flag:
                                                synchronized_databases.append(database_record)
                                                connection.commit()

                                            else:
                                                logging.info(f"[In flux.views.SynchronizationViewSet.create] the {schema_name} schema or the {table_name} table are not found in the synchronization file")
                                        
                                    except json.JSONDecodeError as e:
                                        logging.error( f"[In flux.views.SynchronizationViewSet.create]. Error parsing json from file for database synchronization. File path: {file_path}" )

                            except FileNotFoundError as e:
                                flag = False
                                logging.error( f"[In flux.views.SynchronizationViewSet.create]. Error opening file for database synchronization. File path: {file_path}" )

                    else:
                        raise serializers.ValidationError( _("Invalid connection parameters") )
                else:
                    raise serializers.ValidationError( _("No database exists with id %(id)s" % {'id': id}) )    

            if synchronized_databases:
                # record the synchronization
                synchronization = models.Synchronization.objects.create(time_applied=timezone.now(), is_applied=True, file=file)
                
                for database in synchronized_databases:
                    models.SynchronizationDatabase.objects.create(synchronization=synchronization, database=database)

                synchronizations.append(synchronization)

            else:
                logging.error("No synchronizations were applied")
                breakpoint()
        
        return Response( data=SynchronizationSerializer(synchronizations, many=True).data )

    @action(
        methods=["DELETE",], permission_classes=[], detail=False
    )
    def delete_all(self, request, *args, **kwargs):
        user = request.user

        if not user.is_superuser:
            return Response( data={"message": _("You are not authorized to access this resource")}, status=status.HTTP_403_FORBIDDEN )

        all_synchronizations = models.Synchronization.objects.all()
        data = SynchronizationSerializer(all_synchronizations, many=True).data

        all_synchronizations.delete()

        return Response( data=data )