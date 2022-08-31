import json
import logging
import os
import datetime as dt

import pyodbc

from rest_framework import serializers, status
from rest_framework.response import Response

from . import models
from django.core.files import File
from django.db.models import Q
from django.utils.translation import gettext as _
from django.utils import timezone

from frontend.views import get_database_connection

from ferdolt import models as ferdolt_models

from ferdolt_web import settings

from core.functions import custom_converter, extract_raw

class FluxDatabaseSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(source="database.id")
    name = serializers.CharField(source='database.name', required=False)
    host = serializers.CharField(source='database.host', required=False)
    port = serializers.CharField(source='database.port', required=False)
    
    class FluxSchemaSerializer(serializers.ModelSerializer):
        class FluxTableSerializer(serializers.ModelSerializer):
            class FluxColumnSerializer(serializers.ModelSerializer):
                class Meta:
                    model = ferdolt_models.Column
                    fields = ( "id", "name", )

            columns = FluxColumnSerializer(many=True, required=False, allow_null=True, source="column_set")

            class Meta:
                model = ferdolt_models.Table
                fields = ("id", "name", "columns")

        tables = FluxTableSerializer(many=True, required=False, allow_null=True, source="table_set")

        class Meta:
            model = ferdolt_models.DatabaseSchema
            fields = ( "id", "name", "tables" )
            
    schemas = FluxSchemaSerializer(many=True, required=False, allow_null=True, source='database.databaseschema_set')

    class Meta:
        model = models.ExtractionDatabase
        fields = ("id", "name", "host", "port", "schemas")

class FileSerializer(serializers.ModelSerializer):
    file_name = serializers.CharField(source="file.name", read_only=True)
    file_url = serializers.CharField(source='file.url', read_only=True)
    file_size = serializers.FloatField(source='file.size', read_only=True)

    class Meta:
        model = models.File
        fields = ("id", "file_name", "file_url", "file_size")

class ExtractionSerializer(serializers.ModelSerializer):
    databases = FluxDatabaseSerializer(many=True, source='extractiondatabase_set')
    use_pentaho = serializers.BooleanField(required=False, allow_null=True, write_only=True)
    file_name = serializers.CharField(source="file.file.name", read_only=True)
    file_url = serializers.CharField(source='file.file.url', read_only=True)
    file_size = serializers.FloatField(source='file.file.size', read_only=True)
    file_id = serializers.IntegerField(source='file.id', read_only=True)
    use_time = serializers.BooleanField(default=True, required=False)

    class Meta:
        model = models.Extraction
        fields = ("id", "start_time", "time_made", "databases", "use_pentaho", "file_id", "file_name", "file_url", "file_size", "use_time")
        extra_kwargs = {
            "time_made": {"read_only": True}
        }

    def validate_databases(self, value):
        if not isinstance(value, list):
            raise serializers.ValidationError(
                _("Expected a list for the databases key, got a %(type)s instead" % { 'type': type(value) } )
            )
        
        if value == []:
            raise serializers.ValidationError(
                _("The databases must have at least one item")
            )

        return value

    def create(self, validated_data):
        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("extractiondatabase_set")

            start_time = None
            
            # set use_time to False if it is passed as False else True
            use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )

            if 'start_time' in validated_data and use_time:
                start_time = validated_data.pop("start_time")


            time_made = timezone.now()

            results = {}
            database_records = []

            for database in databases:
                database_record = ferdolt_models.Database.objects.filter(id=database['database']['id'])
                
                if database_record.exists():
                    database_record = database_record.first()
                    
                    database_records.append(database_record)

                    database_dictionary = results.setdefault(database_record.name, {})
                    
                    # get the last time data was extracted from this database if no start time was provided
                    if not start_time:
                        latest_extraction: self.Meta.model = self.Meta.model.objects.filter(extractiondatabase__database=database_record).last()

                        if latest_extraction:
                            start_time = latest_extraction.time_made

                    connection = get_database_connection(database_record)

                    if connection:
                        cursor = connection.cursor()
                        
                        schemas = None

                        if 'databaseschema_set' in database['database'] and database['database']['databaseschema_set']:
                            schemas = database['database']['databaseschema_set']
                        else:
                            schemas = FluxDatabaseSerializer.FluxSchemaSerializer( database_record.databaseschema_set.all(), many=True ).data

                        for schema in schemas:
                            tables = None

                            schema_tables_key = 'table_set'
                            if schema_tables_key in schema and schema[schema_tables_key]:
                                tables = schema[schema_tables_key]
                            else:
                                tables = FluxDatabaseSerializer.FluxSchemaSerializer.FluxTableSerializer( ferdolt_models.Table.objects.filter( schema__database=database_record, schema__name=schema['name'] ), many=True ).data

                            for _table in tables:
                                table = ferdolt_models.Table.objects.get(schema__name=schema['name'], schema__database=database_record, name=_table['name'])

                                schema_dictionary = database_dictionary.setdefault(schema['name'], {})
                                table_results = schema_dictionary.setdefault( table.name, [] )

                                time_field = table.column_set.filter( Q(name='last_updated') | Q(name="deletion_time") )
        
                                query = f"""
                                SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table.schema.name}.{table.name} 
                                { f"WHERE { time_field.first().name } >= ?" if start_time and time_field.exists() and use_time else "" }
                                """
                                breakpoint()
                                try:
                                    rows = cursor.execute(query, start_time) if start_time and time_field.exists() and use_time else cursor.execute(query)

                                    columns = [ column[0] for column in cursor.description ]

                                    for row in rows:
                                        row_dictionary = dict( zip( columns, row ) )
                                        table_results.append(row_dictionary)
                                except pyodbc.ProgrammingError as e:
                                    print(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                                    raise e
                      
                    else:
                        raise serializers.ValidationError( _("Invalid connection parameters") )
                else:
                    raise serializers.ValidationError( _("No database exists with id %(id)s" % {'id': id}) )

            filename = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}.json" )

            with open( filename, "a+" ) as _:
                _.write( json.dumps( results, default=custom_converter ) )
                file = models.File.objects.create( file=File( _, name=os.path.basename( filename ) ), size=os.path.getsize(filename), is_deleted=False )

                extraction = models.Extraction.objects.create(file=file, start_time=start_time, time_made=time_made)

                for database in database_records:
                    models.ExtractionDatabase.objects.create( database=database, extraction=extraction )

            return extraction

        else:
            # use pentaho to extract
            os.system( f"{settings.PATH_TO_KITCHEN} { os.path.join(settings.BASE_DIR, 'pentaho_files') }" )


class SynchronizationSerializer(serializers.ModelSerializer):
    databases = FluxDatabaseSerializer(source="synchronizationdatabase_set", many=True)
    use_pentaho = serializers.BooleanField(required=False, allow_null=True, write_only=True)
    
    file_id = serializers.IntegerField(source='file.id', read_only=True)
    file_name = serializers.CharField(source="file.file.name", read_only=True)
    file_url = serializers.CharField(source='file.file.url', read_only=True)
    file_size = serializers.FloatField(source='file.file.size', read_only=True)
    
    class Meta:
        model = models.Synchronization
        fields = ("id", "databases", "use_pentaho", "file_id", "file_name", "file_url", "file_size")
        pass

    def validate_databases(self, value):
        if not isinstance(value, list):
            raise serializers.ValidationError(
                _("Expected a list for the databases key, got a %(type)s instead" % { 'type': type(value) } )
            )
        
        if value == []:
            raise serializers.ValidationError(
                _("The databases must have at least one item")
            )

        return value
