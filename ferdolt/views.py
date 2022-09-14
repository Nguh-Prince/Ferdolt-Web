import json
import re
import zipfile
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from common.permissions import IsStaff
from core.exceptions import InvalidDatabaseConnectionParameters, InvalidDatabaseStructure
from rest_framework import serializers as drf_serializers

import logging
import psycopg
import pyodbc

from cryptography import fernet

from django.db.models import Q
from django.utils.translation import gettext as _
from django.utils import timezone

from common.viewsets import MultipleSerializerViewSet
from core.functions import get_database_connection, get_database_details, get_dbms_booleans, initialize_database, synchronize_database
from ferdolt_web.settings import FERNET_KEY

from flux import models as flux_models
from flux.serializers import ExtractionSerializer

from . import serializers
from . import  permissions

from . import models

class DatabaseManagementSystemViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.DatabaseManagementSystemSerializer

    def get_queryset(self):
        return models.DatabaseManagementSystem.objects.all()


class DatabaseManagementSystemVersionViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.DatabaseManagementSystemVersionSerializer


    def get_queryset(self):
        return models.DatabaseManagementSystemVersion.objects.all()


class DatabaseViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.DatabaseSerializer

    serializer_classes = {
        'retrieve': serializers.DatabaseDetailSerializer,
        'refresh': serializers.DatabaseDetailSerializer,
    }

    def get_queryset(self):
        return models.Database.objects.all()

    @action(
        methods=['GET'], detail=True
    )
    def refresh(self, request, *args, **kwargs):
        database = self.get_object()

        try:
            get_database_details(database)
            serializer = self.get_serializer(database)

            return Response( data=serializer.data )
        except InvalidDatabaseConnectionParameters as e:
            return Response( {'message': _("Error connecting to the database. Check if the credentials are correct or if the database is running")}, status=status.HTTP_400_BAD_REQUEST )

    def create(self, request, *args, **kwargs):
        serializer: serializers.DatabaseSerializer = self.get_serializer( data=request.data )
        serializer.is_valid(raise_exception=True)

        database = serializer.create(serializer.validated_data)
        try:
            get_database_details(database)
        except InvalidDatabaseConnectionParameters as e:
            logging.error(f"Error connecting to the {database.__str__()}.")

        return super().create(request, *args, **kwargs)

    @action(
        methods=["POST"], detail=True
    )
    def initialize(self, request, *args, **kwargs):
        db = self.get_object()

        try:
            initialize_database(db)
        except InvalidDatabaseStructure as e:
            logging.error(f"InvalidDatabaseStructure error raised when initializeing {db.__str__()}")
            return Response(data={'message': _("The target database has one or more target tables without primary key fields. Please add them and try again")}, status=status.HTTP_400_BAD_REQUEST)
        except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
            return Response( data={'message': _("An error occured when trying to initialize the %(database)s database" % {'database': db.__str__()})} )

        return Response(data={'message': _("The %(database)s was initialized successfully." % {'database': db.__str__()})})
    @action(
        methods=["POST"], detail=True
    )
    def synchronize(self, request, *args, **kwargs):
        f = fernet.Fernet(FERNET_KEY)
        database = self.get_object()

        pending_synchronizations = flux_models.ExtractionTargetDatabase.objects.filter(database=database, is_applied=False).order_by('extraction__time_made')

        if not pending_synchronizations.exists():
            return Response(data={'message': _("The %(database_name)s database does not have any pending synchronization" % {'database_name': database.name})})

        connection = get_database_connection(database)

        synchronizations_applied = []
        unapplied_synchronizations = []
        
        temporary_tables_created = set([])

        if connection:
            for synchronization in pending_synchronizations:
                file_path = synchronization.extraction.file.file.path
                file = synchronization.extraction.file

                try:
                    with zipfile.ZipFile(file_path) as zip_file:
                        for file in zip_file.namelist():
                            if file.endswith('.json'):
                                content = zip_file.read(file)
                                if len(content) > 0:
                                    content = f.decrypt( content.encode('utf-8') if not isinstance(content, bytes) else content ) 
                                    content = content.decode('utf-8')

                                    logging.debug("['In ferdolt.views.DatabaseViewSet.synchronize'] reading the unapplied synchronization")

                                    try:
                                        dictionary: dict = json.loads(content)
                                        try:
                                            synchronize_database(connection, database, dictionary, 
                                            temporary_tables_created=temporary_tables_created)
                                            time_applied = timezone.now()
                                            synchronization.time_applied = time_applied
                                            synchronization.is_applied = True

                                            synchronization.save()
                                            
                                            synchronizations_applied.append(synchronization.extraction)
                                        except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                                            logging.error(f"[In ferdolt.views.SynchronizationViewSet.synchronize]. Error synchronizing the database, error: {str(e)}")
                                            unapplied_synchronizations.append({
                                                'synchronization': ExtractionSerializer(synchronization.extraction).data,
                                                "error": _("Couldn't apply the synchronization due to a server error")
                                            })

                                    except json.JSONDecodeError as e:
                                        logging.error( f"[In ferdolt.views.SynchronizationViewSet.synchronize]. Error parsing json from file for database synchronization. File path: {file_path}" )
                                        unapplied_synchronizations.append({
                                            "synchronization": ExtractionSerializer(synchronization.extraction),
                                            "error": _("Couldn't decode json file")
                                        })
                                else:
                                    time_applied = timezone.now()
                                    
                                    synchronization.time_applied = time_applied
                                    synchronization.is_applied = True
                                    synchronization.save()

                                    synchronizations_applied.append(synchronization.extraction)

                except FileNotFoundError as e:
                    logging.error(f"['In ferdolt.views.DatabaseViewSet.synchronize'] couldn't read extraction file {file_path} because it does not exist")
                    unapplied_synchronizations.append({
                        "synchronization": ExtractionSerializer(synchronization.extraction),
                        "error": _("The extraction file has been renamed, moved or deleted")
                    })

                except fernet.InvalidToken as e:
                    logging.error(f"['In ferdolt.views.DatabaseViewSet.synchronize'] couldn't decode extraction file {file_path}")
                    unapplied_synchronizations.append({
                        "synchronization": ExtractionSerializer(synchronization.extraction).data,
                        "error": _("Couldn't decrypt the extraction file")
                    })

            connection.close()
        else:
            return Response(data={'message': _("Error connecting to the %(database_name)s database. Please ensure that your server is running and your credentials are correct" 
                % {'database_name': database.name})}, 
            status=status.HTTP_400_BAD_REQUEST)

        return Response( data={
            'message': _("%(number_applied)d synchronizations applied. %(number_error)d synchronizations unapplied" 
                % {'number_applied': len(synchronizations_applied), 'number_error': len(unapplied_synchronizations)}),
            'data': {
                'applied_synchronizations': ExtractionSerializer(synchronizations_applied, many=True).data,
                'unapplied_synchronizations': unapplied_synchronizations
            }
        } )

    @action(
        methods=["GET"], detail=True
    )
    def extractions(self, request, *args, **kwargs):
        database = self.get_object()

        extractions = flux_models.Extraction.objects.filter(extractionsourcedatabase__database=database)

        return Response( data=ExtractionSerializer(extractions, many=True).data )

class DatabaseSchemaViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.DatabaseSchemaSerializer

    def get_queryset(self):
        return models.DatabaseSchema.objects.all()

class TableViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.TableSerializer
    
    serializer_classes = {
        'insert_data': serializers.TableInsertSerializer,
        'update_data': serializers.TableUpdateSerializer,
        'delete_data': serializers.TableDeleteSerializer
    }

    def get_queryset(self):
        return models.Table.objects.all()

    @action(detail=True, methods=["GET"], permission_classes=[permissions.DummyPermission])
    def records(self, request, *args, **kwargs):
        table: models.Table = self.get_object()

        connection = get_database_connection(table.schema.database)

        if connection:
            cursor = connection.cursor()

            table_primary_key_columns = [ f.name for f in table.column_set.filter( 
                    columnconstraint__is_primary_key=True
                ) 
            ]

            query = f"""
            SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } 
            FROM {table.schema.name}.{table.name} { ' ORDER BY ' if table_primary_key_columns else '' } 
            { ', '.join( [ f for f in table_primary_key_columns ] ) }
            """

            results = cursor.execute(query)
            columns = [ column[0] for column in cursor.description ]

            records = []

            for row in results:
                row_dictionary = dict( zip( columns, row ) )
                records.append(row_dictionary)

            connection.close()
        else:
            breakpoint()
            return Response( data={'message': """Error connecting to the database. 
            Please check your credentials and ensure your database is running"""}, 
            status=status.HTTP_400_BAD_REQUEST )

        return Response(data=records)

    @action(detail=False, methods=['POST'], permission_classes=[permissions.DummyPermission])
    def insert_data(self, request, *args, **kwargs):
        time = timezone.now()

        serializer = serializers.TableInsertSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        database = serializer.validated_data.pop('database')
        table = serializer.validated_data.pop('table')
        atomic = serializer.validated_data.pop('atomic')

        connection = get_database_connection(database)

        if not connection:
            raise drf_serializers.ValidationError( _("Error connecting to the database. Invalid connection parameters") )

        cursor = connection.cursor()

        table_column_set = table.column_set.filter(Q(columnconstraint__is_primary_key=False) | Q(columnconstraint__isnull=True)).values("name")

        error_occured_flag = False
        records_inserted = []

        for record in serializer.validated_data['data']:
            columns_in_common = [ column['name'] for column in table_column_set if column['name'] in record.keys() ]
            
            values_to_insert = tuple( record[f] for f in columns_in_common )
            breakpoint()
            query = f"""
            INSERT INTO {table.schema.name}.{table.name} ( { ', '.join( columns_in_common ) } ) VALUES ( { ', '.join( [ '?' for _ in columns_in_common] ) } )
            """

            try:
                cursor.execute(query, values_to_insert)

                if not atomic: # commit this single change
                    connection.commit()
                    records_inserted.append(record)

            except pyodbc.ProgrammingError as e:
                connection.rollback()
                logging.error( _("Error inserting %(values)s in %(table)s. Exception caught: %(message)s" % { 'values': str(values_to_insert), 
                'table': f"{table.schema.database.name}.{table.schema.name}.{table.name}", 'message': str(e) } ) )
                error_occured_flag = False

        if atomic and not error_occured_flag:
            connection.commit()
        elif atomic and error_occured_flag:
            logging.error( _("An error occured, the changes will not be committed") )
            return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )

        return Response(data={"records_inserted": records_inserted})

    @action(detail=False, methods=['PATCH', 'PUT'], permission_classes=[permissions.DummyPermission])
    def update_data(self, request, *args, **kwargs):
        time = timezone.now()

        serializer = serializers.TableUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        database = serializer.validated_data.pop('database')
        table: models.Table = serializer.validated_data.pop('table')
        atomic = serializer.validated_data.pop('atomic')

        connection = get_database_connection(database)

        if not connection:
            raise drf_serializers.ValidationError( _("Error connecting to the database. Invalid connection parameters") )

        cursor = connection.cursor()

        table_column_set = table.column_set.values("name")
        table_primary_key_set = table.column_set.filter( columnconstraint__is_primary_key=True ).values("name")
        
        dbms_booleans = get_dbms_booleans(database)

        error_occured_flag = False
        records_updated = []

        for record in serializer.validated_data['data']:
            reference_columns = set([ column['name'] for column in table_column_set if column['name'] in record['current'].keys() ])
            update_columns = [ column['name'] for column in table_column_set if column['name'] in record['update'].keys() ]

            values_to_insert = tuple( [ record['update'][f] for f in update_columns ] + 
                [ record['current'][f] for f in reference_columns ] 
            )

            query = f"""
            UPDATE {table.get_queryname()} SET { ', '.join( [ f"{column}=?" for column in update_columns ] ) } 
            WHERE { ', '.join( [ f"{column}=?" for column in reference_columns ] ) }
            """

            try:
                if dbms_booleans["is_sqlserver_db"]:
                    try:
                        cursor.execute( f"SET IDENTITY_INSERT {table.get_queryname()} ON" )
                    except pyodbc.ProgrammingError as e:
                        logging.error( (f"Error setting identity insert on for {table.get_queryname()} table. Error: {str(e)}") )
                        connection.rollback()
                    
                cursor.execute( query, values_to_insert )

                if not atomic: 
                    connection.commit()

                if dbms_booleans["is_sqlserver_db"]:
                    try:
                        cursor.execute( f"SET IDENTITY_INSERT {table.get_queryname()} OFF" )
                    except pyodbc.ProgrammingError as e:
                        logging.error( (f"Error setting identity insert off for {table.get_queryname()} table. Error: {str(e)}") )
                        connection.rollback()

            except ( pyodbc.ProgrammingError, psycopg.ProgrammingError ) as e:
                logging.error(f"Error execuing update query. Query: {query}")
                return Response( data={'message': "Error updating records"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR )
            except (pyodbc.Error, psycopg.Error) as e:
                breakpoint()
                logging.error(f"Error execuing update query. Query: {query}")
                return Response( data={'message': "Error updating records"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR )

        if atomic and not error_occured_flag:
            connection.commit()
        elif atomic and error_occured_flag:
            logging.error( _("An error occured, the changes will not be committed") )
            return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )

        return Response( data=records_updated )

    @action(detail=False, methods=['DELETE'], permission_classes=[permissions.DummyPermission])
    def delete_data(self, request, *args, **kwargs):
        serializer = serializers.TableDeleteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        database = serializer.validated_data.pop('database')
        table = serializer.validated_data.pop('table')
        atomic = serializer.validated_data.pop('atomic')

        connection = get_database_connection(database)

        if not connection:
            raise drf_serializers.ValidationError( _("Error connecting to the database. Invalid connection parameters") )

        cursor = connection.cursor()

        table_column_set = table.column_set.values("name")

        error_occured_flag = False
        records_deleted = []

        for record in serializer.validated_data['data']:
            columns_in_common = [ column['name'] for column in table_column_set if column['name'].lower() in record.keys() ]
            # values_to_insert = tuple( value for key in columns_in_common for value in record[key] )
            values_to_insert = tuple( record[key] for key in columns_in_common )

            query = f"""
            DELETE FROM {table.schema.name}.{table.name} WHERE 
            { " AND ".join( f"{key}=?" for key in columns_in_common ) }
            """

            breakpoint()

            try:
                cursor.execute(query, values_to_insert)
                if not atomic:
                    connection.commit()
                records_deleted.append(record)
            except pyodbc.ProgrammingError as e:
                if atomic: 
                    logging.error( _("An error occured, the changes will not be committed") )
                    return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )
                error_occured_flag = True
            except pyodbc.Error as e:
                if atomic: 
                    logging.error( _("An error occured, the changes will not be committed") )
                    return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )
                
                error_occured_flag = True

        if atomic and not error_occured_flag:
            connection.commit()
        elif atomic and error_occured_flag:
            logging.error( _("An error occured, the changes will not be committed") )
            return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )

        return Response( records_deleted )

    @action(detail=False, methods=['GET'], permission_classes=[permissions.DummyPermission])
    def read_data(self, request, *args, **kwargs):
        serializer = serializers.TableRecordsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        database = serializer.validated_data.pop('database')
        table = serializer.validated_data.pop('table')

        connection = get_database_connection(database)
        cursor = connection.cursor()

        query = f"SELECT * FROM {table.schema.name}.{table.name}"

        results = []
        rows = cursor.execute(query)
        columns = [ column[0] for column in cursor.description ]

        for row in rows:
            results.append( dict( zip(columns, row) ) )

        return Response(data=results)

class ColumnViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.ColumnSerializer

    def get_queryset(self):
        return models.Column.objects.all()

class ColumnConstraintViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.ColumnConstraintSerializer

    def get_queryset(self):
        return models.ColumnConstraint.objects.all()

class ServerViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.ServerSerializer

    def get_queryset(self):
        return models.Server.objects.all()

