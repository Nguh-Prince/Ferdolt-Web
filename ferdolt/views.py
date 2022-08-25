from unittest import result
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from frontend import views
from rest_framework import serializers as drf_serializers

import logging
import pyodbc

from django.utils.translation import gettext as _
from django.utils import timezone

from frontend.views import columns, get_database_connection

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


class DatabaseViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DatabaseSerializer

    def get_queryset(self):
        return models.Database.objects.all()

class DatabaseSchemaViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DatabaseSchemaSerializer

    def get_queryset(self):
        return models.DatabaseSchema.objects.all()

class TableViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.TableSerializer

    def get_queryset(self):
        return models.Table.objects.all()

    @action(detail=True, methods=["GET"], permission_classes=[permissions.DummyPermission])
    def records(self, request, *args, **kwargs):
        table: models.Table = self.get_object()

        connection = get_database_connection(table.schema.database)
        cursor = connection.cursor()

        query = f"""
        SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table.schema.name}.{table.name}
        """

        results = cursor.execute(query)
        columns = [ column[0] for column in cursor.description ]

        records = []

        for row in results:
            row_dictionary = dict( zip( columns, row ) )
            records.append(row_dictionary)

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

        table_column_set = table.column_set.values("name")

        error_occured_flag = False
        records_inserted = []

        for record in serializer.validated_data['data']:
            columns_in_common = [ column['name'] for column in table_column_set if column['name'] in record.keys() ]
            
            values_to_insert = tuple( record[f] for f in columns_in_common )

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
        table = serializer.validated_data.pop('table')
        atomic = serializer.validated_data.pop('atomic')
        reference_columns = serializer.validated_data.pop('reference_columns')

        connection = get_database_connection(database)

        if not connection:
            raise drf_serializers.ValidationError( _("Error connecting to the database. Invalid connection parameters") )

        cursor = connection.cursor()

        table_column_set = table.column_set.values("name")
        table_primary_key_set = table.column_set.filter( columnconstraint__is_primary_key=True ).values("name")

        error_occured_flag = False
        records_updated = []

        for record in serializer.validated_data['data']:
            reference_columns = [ column['name'] for column in table_primary_key_set ] if not reference_columns else reference_columns
            columns_in_common = [ column['name'] for column in table_column_set if column['name'] in record.keys() and column['name'] not in reference_columns ]
            
            values_to_insert = tuple( record[f] for f in columns_in_common + reference_columns )

            query = f"""
            UPDATE {table.schema.name}.{table.name} SET { ', '.join( [ f"{column}=?" for column in columns_in_common ] ) } 
            WHERE { ', '.join( [ f"{column}=?" for column in reference_columns ] ) }
            """
            try:
                cursor.execute( query, values_to_insert )
            except pyodbc.ProgrammingError as e:
                connection.rollback()
                logging.error( _("Error inserting %(values)s in %(table)s. Exception caught: %(message)s" % { 'values': str(values_to_insert), 
                'table': f"{table.schema.database.name}.{table.schema.name}.{table.name}", 'message': str(e) } ) )
                error_occured_flag = False
                if atomic:
                    logging.error( _("An error occured, the changes will not be committed. Error: %(error)s" % { 'error': str(e) }) )
                    return Response( data = _("An error occured, the changes will not be committed"), status=status.HTTP_400_BAD_REQUEST )

            records_updated.append(record)

            if not atomic: 
                connection.commit()
        
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
            values_to_insert = tuple( value for key in columns_in_common for value in record[key] )

            query = f"""
            DELETE FROM {table.schema.name}.{table.name} WHERE { " OR ".join( f"{key}=?" for key in columns_in_common for __ in record[key] ) }
            """

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

