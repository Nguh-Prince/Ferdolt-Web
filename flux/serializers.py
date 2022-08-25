import json
import os
import datetime as dt

from rest_framework import serializers
from rest_framework.response import Response

from . import models
from django.core.files import File
from django.utils.translation import gettext as _
from django.utils import timezone

from frontend.views import get_database_connection

from ferdolt import models as ferdolt_models

from ferdolt_web import settings

def custom_converter(object):
    if isinstance(object, dt.datetime):
        return object.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if isinstance(object, dt.date):
        return object.strftime("%Y-%m-%d")

    if isinstance(object, dt.time):
        return object.strftime("%H:%M:%S.%f")
    
    return object.__str__()

class ExtractionSerializer(serializers.ModelSerializer):
    class ExtractionDatabaseSerializer(serializers.ModelSerializer):
        id = serializers.IntegerField(source="database.id")
        name = serializers.CharField(source='database.name', required=False)
        host = serializers.CharField(source='database.host', required=False)
        port = serializers.CharField(source='database.port', required=False)

        class Meta:
            model = models.ExtractionDatabase
            fields = ("id", "name", "host", "port")
    
    databases = ExtractionDatabaseSerializer(many=True, source='extractiondatabase_set')
    use_pentaho = serializers.BooleanField(required=False, allow_null=True, write_only=True)

    class Meta:
        model = models.Extraction
        fields = ("start_time", "databases", "use_pentaho")

    def create(self, validated_data):
        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("extractiondatabase_set")
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
                    
                    # get the last time data was extracted from this database
                    if not start_time:
                        latest_extraction: self.Meta.model = self.Meta.model.objects.filter(extractiondatabase__database=database_record).last()

                        if latest_extraction:
                            start_time = latest_extraction.time_made

                    connection = get_database_connection(database_record)

                    if connection:
                        cursor = connection.cursor()

                        for table in ferdolt_models.Table.objects.filter(schema__database=database_record):
                            schema_dictionary = database_dictionary.setdefault(table.schema.name, {})
                            table_results = schema_dictionary.setdefault(table.name, [])

                            query = f"""
                            SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table.schema.name}.{table.name} 
                            { "WHERE last_updated >= ?" if start_time else "" }
                            """

                            rows = cursor.execute(query, start_time) if start_time else cursor.execute(query)

                            columns = [ column[0] for column in cursor.description ]

                            for row in rows:
                                row_dictionary = dict( zip( columns, row ) )
                                table_results.append(row_dictionary)
                        
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

