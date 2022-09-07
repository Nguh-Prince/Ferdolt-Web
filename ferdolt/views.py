import re
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from core.exceptions import InvalidDatabaseConnectionParameters
from rest_framework import serializers as drf_serializers

import logging
import psycopg
import pyodbc

from django.db.models import Q
from django.utils.translation import gettext as _
from django.utils import timezone

from common.viewsets import MultipleSerializerViewSet
from core.functions import get_database_connection, get_database_details, get_dbms_booleans

from . import serializers
from . import  permissions

from . import models

class DatabaseManagementSystemViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DatabaseManagementSystemSerializer

    def get_queryset(self):
        return models.DatabaseManagementSystem.objects.all()


class DatabaseManagementSystemVersionViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DatabaseManagementSystemVersionSerializer


    def get_queryset(self):
        return models.DatabaseManagementSystemVersion.objects.all()


class DatabaseViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
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

class DatabaseSchemaViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DatabaseSchemaSerializer

    def get_queryset(self):
        return models.DatabaseSchema.objects.all()

class TableViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
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
            return Response( data={'message': "Error connecting to the database. Please check your credentials and ensure your database is running"}, 
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
    serializer_class = serializers.ColumnSerializer

    def get_queryset(self):
        return models.Column.objects.all()

class ColumnConstraintViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.ColumnConstraintSerializer

    def get_queryset(self):
        return models.ColumnConstraint.objects.all()

class ServerViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.ServerSerializer

    def get_queryset(self):
        return models.Server.objects.all()

