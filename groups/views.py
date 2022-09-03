import json
import logging
import os
from tracemalloc import start

from cryptography.fernet import Fernet

from django.core.files import File as DjangoFile
from django.db.models import Count, Q
from django.utils import timezone

import pyodbc

import psycopg

from rest_framework import viewsets
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from common.viewsets import MultipleSerializerViewSet
from core.functions import custom_converter, get_database_connection
from ferdolt_web import settings

from ferdolt_web.settings import FERNET_KEY
from ferdolt import models as ferdolt_models
from flux.models import Extraction, File
from . import models, serializers

class GroupViewSet(viewsets.ModelViewSet, MultipleSerializerViewSet):
    serializer_class = serializers.GroupSerializer

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
        group = self.get_object()
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        source = serializer.validated_data['source_database']

        validated_data = serializer.validated_data
        f = Fernet(FERNET_KEY)

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = [source]

            start_time = None

            use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )

            if 'start_time' in validated_data and use_time:
                start_time = validated_data.pop('start_time')

            time_made = timezone.now()

            results = {}

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
                        # get the groupcolumns linked to the columns of tables in the source database
                        actual_database_tables = models.GroupColumnColumn.objects.filter( 
                            group_column__group_table=table, column__table__schema__database=source.database
                        ).values(
                            "column__table__name", "column__table__schema__name", "column__table__id"
                        ).annotate(Count("column__table__name"))
                        
                        actual_database_tables = ferdolt_models.Table.objects.filter(
                            schema__database=source.database, id__in=models.GroupColumnColumn.objects
                                .filter(group_column__group_table=table)
                                .values("column__table__id")
                            ).distinct()
                        
                        table_dictionary = {}

                        breakpoint()

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
                                    table_dictionary.setdefault( item.name.lower(), table_results )

                            except pyodbc.ProgrammingError as e:
                                logging.error(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                                raise e
                            except psycopg.ProgrammingError as e:
                                logging.error(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                                raise e

                        if table_dictionary.keys():
                            group_dictionary.setdefault( table.name.lower(), table_dictionary )
                            
                            file_name = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, 
                            "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}.json")

                            group_extraction = None

                            with open( file_name, "a+" ) as __:
                                json_string = json.dumps( results, default=custom_converter )
                                token = f.encrypt( bytes( json_string, 'utf-8' ) )

                                __.write( token.decode('utf-8') )
                                
                                file = File.objects.create( file=DjangoFile( __, name=os.path.basename(file_name) ), size=os.path.getsize(file_name), is_deleted=False )

                                extraction = models.Extraction.objects.create(file=file, start_time=start_time, time_made=time_made)

                                group_extraction = models.GroupExtraction.objects.create(group=group, extraction=extraction, source_database=source)              

                            os.unlink( file_name )
            serializer = serializers.GroupExtractionSerializer(group_extraction)

            return Response( data=serializer.data, status=status.HTTP_201_CREATED )

    @action(
        methods=['POST'],
        detail=True
    )
    def synchronize(self, request, *args, **kwargs):
        pass

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