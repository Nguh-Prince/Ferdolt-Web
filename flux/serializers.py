<<<<<<< HEAD
from collections import OrderedDict
import json
import logging
import os
import datetime as dt
from sqlite3 import ProgrammingError
import zipfile

import psycopg
import pyodbc

from cryptography.fernet import Fernet

from django.core.files import File
from django.db.models import Q
from django.utils.translation import gettext as _
from django.utils import timezone

from rest_framework import serializers, status
from rest_framework.response import Response

from core.functions import custom_converter, extract_raw, synchronize_database
from . import models
from frontend.views import get_database_connection
from ferdolt import models as ferdolt_models
from ferdolt_web import settings

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
        model = models.ExtractionSourceDatabase
        fields = ("id", "name", "host", "port", "schemas")

class FileSerializer(serializers.ModelSerializer):
    file_name = serializers.CharField(source="file.name", read_only=True)
    file_url = serializers.CharField(source='file.url', read_only=True)
    file_size = serializers.FloatField(source='file.size', read_only=True)

    class Meta:
        model = models.File
        fields = ("id", "file_name", "file_url", "file_size")

class ExtractionSerializer(serializers.ModelSerializer):
    class ExtractionSourceDatabaseSerializer(serializers.ModelSerializer):
        
        class ExtractionSourceDatabaseSchemaSerializer(serializers.ModelSerializer):
            class ExtractionSourceTableSerializer(serializers.ModelSerializer):
                class Meta:
                    model = models.ExtractionSourceTable
                    fields = ("id", "table")
            
            tables = ExtractionSourceTableSerializer(many=True, required=False, source='extractionsourcetable_set')

            def validate(self, attrs):
                validation_errors = []

                schema = attrs['schema']
                tables_key = 'extractionsourcetable_set'

                if tables_key in attrs and attrs[tables_key]:
                    tables = attrs[tables_key]

                    for table in tables:
                        table = table['table']

                        if table not in schema.table_set.all():
                            validation_errors.append(
                                serializers.ValidationError( _("No table with id %(id)d exists in the schema with id %(db_id)d" 
                            % { 'id': table.id, 'db_id': schema.id }) )
                            )

                else:
                    tables = []

                    for table in schema.table_set.all():
                        ordered_dict = OrderedDict()
                        ordered_dict['table'] = table

                        tables.append(table)
                    attrs[tables_key] = tables

                return attrs

            class Meta:
                model = models.ExtractionSourceDatabaseSchema
                fields = ("id", "schema", "tables")

        schemas = ExtractionSourceDatabaseSchemaSerializer(many=True, required=False, source='extractionsourcedatabaseschema_set')

        schemas_key = 'extractionsourcedatabaseschema_set'
        schema_tables_key = 'extractionsourcetable_set'

        class Meta:
            model = models.ExtractionSourceDatabase
            fields = ( "id", "database", "schemas" )

        def validate(self, attrs):
            validation_errors = []

            database = attrs['database']
            schemas_key = self.schemas_key
            tables_key = self.schema_tables_key

            if schemas_key in attrs and attrs[schemas_key]:
                schemas = attrs[schemas_key]
                for schema in schemas:
                    if schema['schema'] not in database.databaseschema_set.all():
                        schema = schema['schema']
                        validation_errors.append(
                            serializers.ValidationError( _("No schema with id %(id)d exists in the database with id %(db_id)d" 
                            % { 'id': schema.id, 'db_id': database.id }) )
                        )
                    else:
                        if tables_key not in schema:
                            tables = []
                            
                            for table in schema.table_set.all():
                                ordered_dict = OrderedDict()
                                ordered_dict['table'] = table
                                
                                tables.append(ordered_dict)
                            
                            schema[tables_key] = tables

            else:
                # select all the schemas from the database
                schemas = []

                for schema in database.databaseschema_set.all():
                    ordered_dict = OrderedDict()
                    ordered_dict['schema'] = schema

                    ordered_dict[self.schema_tables_key] = []

                    # select all the tables from the schema
                    for table in schema.table_set.all():
                        table_ordered_dict = OrderedDict()
                        table_ordered_dict['table'] = table

                        ordered_dict[self.schema_tables_key].append(table_ordered_dict)

                    schemas.append( ordered_dict )
                
                attrs[schemas_key] = schemas

            if validation_errors:
                raise serializers.ValidationError(validation_errors)

            return attrs

    class ExtractionTargetDatabaseSerializer(serializers.ModelSerializer):
        database_id = serializers.IntegerField(source='database.id')
        name = serializers.CharField(source='database.name')
        port = serializers.CharField(source='database.port')
        host = serializers.CharField(source='database.host')

        class Meta:
            model = models.ExtractionTargetDatabase
            fields = ("id", "database_id", "name", "host", "port", "is_applied")

    databases = ExtractionSourceDatabaseSerializer(many=True, source='extractionsourcedatabase_set')
    target_databases = serializers.ListField( child=serializers.IntegerField(), write_only=True ) # list of ids
    synchronize_immediately = serializers.BooleanField(required=False, default=True, write_only=True)

    use_pentaho = serializers.BooleanField(required=False, allow_null=True, write_only=True)
    file_name = serializers.CharField(source="file.file.name", read_only=True)
    file_url = serializers.CharField(source='file.file.url', read_only=True)
    file_size = serializers.FloatField(source='file.file.size', read_only=True)
    file_id = serializers.IntegerField(source='file.id', read_only=True)
    use_time = serializers.BooleanField(default=True, required=False)

    targets = ExtractionTargetDatabaseSerializer(many=True, source='extractiontargetdatabase_set', read_only=True)

    def validate_target_databases(self, items):
        validation_errors = []
        if not items:
            raise serializers.ValidationError( _("The target_databases item must be a list with at least one item") )

        for index, item in enumerate(items):
            try:
                database = ferdolt_models.Database.objects.get(id=item)
                items[index] = database
            except ferdolt_models.Database.DoesNotExist as e:
                validation_errors.append(
                    serializers.ValidationError( _("Error on index %(index)d of the target_databases list. No databases exists with id %(id)d" 
                    % {'index': index+1, 'id': item}) )
                )
        
        if validation_errors:
            raise serializers.ValidationError(validation_errors)

        return items

    def validate(self, attrs):
        if 'synchronize_immediately' not in attrs:
            attrs['synchronize_immediately'] = True

        return attrs

    class Meta:
        model = models.Extraction
        fields = ("id", "start_time", "time_made", "databases", "use_pentaho", "file_id", "file_name", "file_url", "file_size", "use_time", "target_databases", 'synchronize_immediately', "targets")
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
        f = Fernet(settings.FERNET_KEY)

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("extractionsourcedatabase_set")
            start_time = None
            
            # set use_time to False if it is passed as False else True
            # use_time is set to False if the user doesn't want to select rows from the tables based on time i.e. WHERE last_updated > start_time
            use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )

            if 'start_time' in validated_data and use_time:
                start_time = validated_data.pop("start_time")

            time_made = timezone.now()

            results = {}
            database_records = []

            for database in databases:
                database_record = database['database']
                
                if database_record:
                    database_dictionary = results.setdefault(database_record.name, {})

                    connection = get_database_connection(database_record)

                    if connection:
                        cursor = connection.cursor()
                        database_records.append(database)
                        
                        schemas = None
                        schemas_key = 'extractionsourcedatabaseschema_set'

                        if schemas_key in database and database[schemas_key]:
                            schemas = database[schemas_key]

                        for schema in schemas:
                            tables = None
                            _schema = schema['schema']

                            schema_tables_key = 'extractionsourcetable_set'

                            if schema_tables_key in schema and schema[schema_tables_key]:
                                tables = schema[schema_tables_key]

                                for _table in tables:
                                    table: ferdolt_models.Table = _table['table']
                                    deletion_table = table.deletion_table
                                    
                                    table_query_name = table.get_queryname()
                                    deletion_table_query_name = deletion_table.get_queryname()

                                    schema_dictionary = database_dictionary.setdefault(_schema.name, {})
                                    table_dictionary = schema_dictionary.setdefault( table.name, {} )

                                    table_rows = table_dictionary.setdefault( "rows", [] )
                                    table_deleted_rows = table_dictionary.setdefault("deletions", [])

                                    time_field = table.column_set.filter( Q(name='last_updated') | Q(name="deletion_time") )
                                    deletion_time_field = "deletion_time"
            
                                    query = f"""
                                    SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table_query_name} 
                                    { f"WHERE { time_field.first().name } >= ?" if start_time and time_field.exists() and use_time else "" }
                                    """
                                    deletion_query = f"""
                                    SELECT { ', '.join( [column.name for column in deletion_table.column_set.all()] ) } FROM { deletion_table_query_name } 
                                    { f"WHERE { deletion_time_field } >= ?" if start_time and deletion_time_field and use_time else "" }
                                    """
                                    try:
                                        rows = cursor.execute(query, start_time) if start_time and time_field.exists() and use_time else cursor.execute(query)

                                        columns = [ column[0] for column in cursor.description ]

                                        for row in rows:
                                            row_dictionary = dict( zip( columns, row ) )
                                            table_rows.append(row_dictionary)

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                                        raise e

                                    try:
                                        deleted_rows = (cursor.execute( deletion_query, start_time ) 
                                            if start_time and deletion_time_field and use_time
                                            else cursor.execute(deletion_query)
                                        )

                                        columns = [ column[0] for column in cursor.description ]
                                        for row in deleted_rows:
                                            row_dictionary = dict( zip( columns, row ) )
                                            table_deleted_rows.append( row_dictionary )

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error( f"Erorr occured when extracting the deleted rows from the deletion table of {table.__str__()} (Deletion table: {deletion_table.__str__()}, Error: {str(e)})" )
                                        raise e
                        
                    else:
                        raise serializers.ValidationError( _("Invalid connection parameters") )
                else:
                    raise serializers.ValidationError( _("No database exists with id %(id)s" % {'id': id}) )

            # for database in databases:
            #     database_record = database['database']

            #     if database_record:
            #         database_dictionary = results.setdefault(database_record.name, {})

            #         connection get_database_connection(database_record)

            #         if connection:
            #             cursor = connection.cursor()
            #             database_records.append(database)

            #             schemas = None
            #             schemas_key = 'extractionsourcedatabaseschema_set'

            #             if schemas_key in database and database[schemas_key]:
            #                 schemas = database[schemas_key]

            #             for schema in schemas:
            #                 tables = None
            #                 _schema = schema['schema']

            #                 schema_tables_key = 'extractionsourcetable_set'

            #                 if schema_tables_key in schema and schema[schema_tables_key]:
            #                     tables = schema[schema_tables_key]

            #                     for _table in tables:
            #                         table: ferdolt_models.Table = _table['table']
            #                         deletion_table = table.deletion_table

            #                         table_query_name = table.get_queryname()
            #                         deletion_table_query_name = deletion_table.get_queryname()

            #                         schema_dictionary = database_dictionary.setdefault(_schema.name, {})
            #                         table_dictionary = schema_dictionary.setdefault( table.name, {} )

            #                         table_rows = table_dictionary.setdefault( "rows", [] )
            #                         table_deleted_rows = table_dictionary.setdefault("deletions", [])

            #                         time_field = table.column_set.filter( Q(name='last_updated') | Q(name="deletion_time") )
            #                         deletion_time_field = "deletion_time"

            # get the last extraction that included this table
            if database_records:
                base_filename = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}" )

                filename = base_filename + ".json"
                zip_filename = base_filename + ".zip"

                with open( filename, "a+" ) as file:
                    json_string = json.dumps( results, default=custom_converter )
                    token = f.encrypt( bytes(json_string, 'utf-8') )

                    file.write( token.decode('utf-8') )
                
                # zipping the json file
                with zipfile.ZipFile(zip_filename, mode='a') as archive:
                    archive.write(filename, os.path.basename(filename))
                    
                with open( zip_filename, "rb" ) as __:
                    file = models.File.objects.create( 
                        file=File( __, name=os.path.basename( zip_filename ) ), 
                        size=os.path.getsize(zip_filename), is_deleted=False )

                    extraction = models.Extraction.objects.create(file=file, start_time=start_time, time_made=time_made)

                    # record source info (database, schema, table)
                    for database in database_records:
                        # create extraction source databases, source schemas and source tables
                        source_database = models.ExtractionSourceDatabase.objects.create( database=database['database'], extraction=extraction )

                        for schema in database[self.ExtractionSourceDatabaseSerializer.schemas_key]:
                            source_schema = models.ExtractionSourceDatabaseSchema.objects.create(extraction_database=source_database, schema=schema['schema'])

                            for table in schema[self.ExtractionSourceDatabaseSerializer.schema_tables_key]:
                                models.ExtractionSourceTable.objects.create(extraction_database_schema=source_schema, table=table['table'])

                    # record target databases
                    if 'target_databases' in validated_data and validated_data['target_databases']:
                        for database in validated_data['target_databases']:
                            synchronized_flag = False

                            try:
                                connection = get_database_connection(database)

                                if connection:
                                    synchronize_database(connection, database, results)
                                    flag = True
                                else: 
                                    flag = False

                            except TypeError as e:
                                raise e
                            # except Exception as e:
                            #     logging.error(f"The {database.__str__()} database was not synchronized successfully")
                            #     logging.error(f"The exception raised: {str(e)}")
                            #     flag = False
                            #     raise e

                            models.ExtractionTargetDatabase.objects.create(extraction=extraction, database=database, is_applied=synchronized_flag)

                os.unlink( filename )
                os.unlink( zip_filename )

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
=======
from collections import OrderedDict
import json
import logging
import os
import datetime as dt
from sqlite3 import ProgrammingError
import zipfile

import psycopg
import pyodbc

from cryptography.fernet import Fernet

from django.core.files import File
from django.db.models import Q
from django.utils.translation import gettext as _
from django.utils import timezone

from rest_framework import serializers, status
from rest_framework.response import Response

from core.functions import custom_converter, extract_raw, synchronize_database
from . import models
from frontend.views import get_database_connection
from ferdolt import models as ferdolt_models
from ferdolt_web import settings

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
        model = models.ExtractionSourceDatabase
        fields = ("id", "name", "host", "port", "schemas")

class FileSerializer(serializers.ModelSerializer):
    file_name = serializers.CharField(source="file.name", read_only=True)
    file_url = serializers.CharField(source='file.url', read_only=True)
    file_size = serializers.FloatField(source='file.size', read_only=True)

    class Meta:
        model = models.File
        fields = ("id", "file_name", "file_url", "file_size")

class ExtractionSerializer(serializers.ModelSerializer):
    class ExtractionSourceDatabaseSerializer(serializers.ModelSerializer):
        
        class ExtractionSourceDatabaseSchemaSerializer(serializers.ModelSerializer):
            class ExtractionSourceTableSerializer(serializers.ModelSerializer):
                class Meta:
                    model = models.ExtractionSourceTable
                    fields = ("id", "table")
            
            tables = ExtractionSourceTableSerializer(many=True, required=False, source='extractionsourcetable_set')

            def validate(self, attrs):
                validation_errors = []

                schema = attrs['schema']
                tables_key = 'extractionsourcetable_set'

                if tables_key in attrs and attrs[tables_key]:
                    tables = attrs[tables_key]

                    for table in tables:
                        table = table['table']

                        if table not in schema.table_set.all():
                            validation_errors.append(
                                serializers.ValidationError( _("No table with id %(id)d exists in the schema with id %(db_id)d" 
                            % { 'id': table.id, 'db_id': schema.id }) )
                            )

                else:
                    tables = []

                    for table in schema.table_set.all():
                        ordered_dict = OrderedDict()
                        ordered_dict['table'] = table

                        tables.append(table)
                    attrs[tables_key] = tables

                return attrs

            class Meta:
                model = models.ExtractionSourceDatabaseSchema
                fields = ("id", "schema", "tables")

        schemas = ExtractionSourceDatabaseSchemaSerializer(many=True, required=False, source='extractionsourcedatabaseschema_set')

        schemas_key = 'extractionsourcedatabaseschema_set'
        schema_tables_key = 'extractionsourcetable_set'

        class Meta:
            model = models.ExtractionSourceDatabase
            fields = ( "id", "database", "schemas" )

        def validate(self, attrs):
            validation_errors = []

            database = attrs['database']
            schemas_key = self.schemas_key
            tables_key = self.schema_tables_key

            if schemas_key in attrs and attrs[schemas_key]:
                schemas = attrs[schemas_key]
                for schema in schemas:
                    if schema['schema'] not in database.databaseschema_set.all():
                        schema = schema['schema']
                        validation_errors.append(
                            serializers.ValidationError( _("No schema with id %(id)d exists in the database with id %(db_id)d" 
                            % { 'id': schema.id, 'db_id': database.id }) )
                        )
                    else:
                        if tables_key not in schema:
                            tables = []
                            
                            for table in schema.table_set.all():
                                ordered_dict = OrderedDict()
                                ordered_dict['table'] = table
                                
                                tables.append(ordered_dict)
                            
                            schema[tables_key] = tables

            else:
                # select all the schemas from the database
                schemas = []

                for schema in database.databaseschema_set.all():
                    ordered_dict = OrderedDict()
                    ordered_dict['schema'] = schema

                    ordered_dict[self.schema_tables_key] = []

                    # select all the tables from the schema
                    for table in schema.table_set.all():
                        table_ordered_dict = OrderedDict()
                        table_ordered_dict['table'] = table

                        ordered_dict[self.schema_tables_key].append(table_ordered_dict)

                    schemas.append( ordered_dict )
                
                attrs[schemas_key] = schemas

            if validation_errors:
                raise serializers.ValidationError(validation_errors)

            return attrs

    class ExtractionTargetDatabaseSerializer(serializers.ModelSerializer):
        database_id = serializers.IntegerField(source='database.id')
        name = serializers.CharField(source='database.name')
        port = serializers.CharField(source='database.port')
        host = serializers.CharField(source='database.host')

        class Meta:
            model = models.ExtractionTargetDatabase
            fields = ("id", "database_id", "name", "host", "port", "is_applied")

    databases = ExtractionSourceDatabaseSerializer(many=True, source='extractionsourcedatabase_set')
    target_databases = serializers.ListField( child=serializers.IntegerField(), write_only=True ) # list of ids
    synchronize_immediately = serializers.BooleanField(required=False, default=True, write_only=True)

    use_pentaho = serializers.BooleanField(required=False, allow_null=True, write_only=True)
    file_name = serializers.CharField(source="file.file.name", read_only=True)
    file_url = serializers.CharField(source='file.file.url', read_only=True)
    file_size = serializers.FloatField(source='file.file.size', read_only=True)
    file_id = serializers.IntegerField(source='file.id', read_only=True)
    use_time = serializers.BooleanField(default=True, required=False)

    targets = ExtractionTargetDatabaseSerializer(many=True, source='extractiontargetdatabase_set', read_only=True)

    def validate_target_databases(self, items):
        validation_errors = []
        if not items:
            raise serializers.ValidationError( _("The target_databases item must be a list with at least one item") )

        for index, item in enumerate(items):
            try:
                database = ferdolt_models.Database.objects.get(id=item)
                items[index] = database
            except ferdolt_models.Database.DoesNotExist as e:
                validation_errors.append(
                    serializers.ValidationError( _("Error on index %(index)d of the target_databases list. No databases exists with id %(id)d" 
                    % {'index': index+1, 'id': item}) )
                )
        
        if validation_errors:
            raise serializers.ValidationError(validation_errors)

        return items

    def validate(self, attrs):
        if 'synchronize_immediately' not in attrs:
            attrs['synchronize_immediately'] = True

        return attrs

    class Meta:
        model = models.Extraction
        fields = ("id", "start_time", "time_made", "databases", "use_pentaho", "file_id", "file_name", "file_url", "file_size", "use_time", "target_databases", 'synchronize_immediately', "targets")
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
        f = Fernet(settings.FERNET_KEY)

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("extractionsourcedatabase_set")
            start_time = None
            
            # set use_time to False if it is passed as False else True
            # use_time is set to False if the user doesn't want to select rows from the tables based on time i.e. WHERE last_updated > start_time
            use_time = not ( 'use_time' in validated_data and not validated_data['use_time'] )

            if 'start_time' in validated_data and use_time:
                start_time = validated_data.pop("start_time")

            time_made = timezone.now()

            results = {}
            database_records = []

            for database in databases:
                database_record = database['database']
                
                if database_record:
                    database_dictionary = results.setdefault(database_record.name, {})

                    connection = get_database_connection(database_record)

                    if connection:
                        cursor = connection.cursor()
                        database_records.append(database)
                        
                        schemas = None
                        schemas_key = 'extractionsourcedatabaseschema_set'

                        if schemas_key in database and database[schemas_key]:
                            schemas = database[schemas_key]

                        for schema in schemas:
                            tables = None
                            _schema = schema['schema']

                            schema_tables_key = 'extractionsourcetable_set'

                            if schema_tables_key in schema and schema[schema_tables_key]:
                                tables = schema[schema_tables_key]

                                for _table in tables:
                                    table: ferdolt_models.Table = _table['table']
                                    deletion_table = table.deletion_table
                                    
                                    table_query_name = table.get_queryname()
                                    deletion_table_query_name = deletion_table.get_queryname()

                                    schema_dictionary = database_dictionary.setdefault(_schema.name, {})
                                    table_dictionary = schema_dictionary.setdefault( table.name, {} )

                                    table_rows = table_dictionary.setdefault( "rows", [] )
                                    table_deleted_rows = table_dictionary.setdefault("deletions", [])

                                    time_field = table.column_set.filter( Q(name='last_updated') | Q(name="deletion_time") )
                                    deletion_time_field = "deletion_time"
            
                                    query = f"""
                                    SELECT { ', '.join( [ column.name for column in table.column_set.all() ] ) } FROM {table_query_name} 
                                    { f"WHERE { time_field.first().name } >= ?" if start_time and time_field.exists() and use_time else "" }
                                    """
                                    deletion_query = f"""
                                    SELECT { ', '.join( [column.name for column in deletion_table.column_set.all()] ) } FROM { deletion_table_query_name } 
                                    { f"WHERE { deletion_time_field } >= ?" if start_time and deletion_time_field and use_time else "" }
                                    """
                                    try:
                                        rows = cursor.execute(query, start_time) if start_time and time_field.exists() and use_time else cursor.execute(query)

                                        columns = [ column[0] for column in cursor.description ]

                                        for row in rows:
                                            row_dictionary = dict( zip( columns, row ) )
                                            table_rows.append(row_dictionary)

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error(f"Error occured when extracting from {database}.{table.schema.name}.{table.name}. Error: {str(e)}")
                                        raise e

                                    try:
                                        deleted_rows = (cursor.execute( deletion_query, start_time ) 
                                            if start_time and deletion_time_field and use_time
                                            else cursor.execute(deletion_query)
                                        )

                                        columns = [ column[0] for column in cursor.description ]
                                        for row in deleted_rows:
                                            row_dictionary = dict( zip( columns, row ) )
                                            table_deleted_rows.append( row_dictionary )

                                    except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                                        logging.error( f"Erorr occured when extracting the deleted rows from the deletion table of {table.__str__()} (Deletion table: {deletion_table.__str__()}, Error: {str(e)})" )
                                        raise e
                        
                    else:
                        raise serializers.ValidationError( _("Invalid connection parameters") )
                else:
                    raise serializers.ValidationError( _("No database exists with id %(id)s" % {'id': id}) )

            if database_records:
                base_filename = os.path.join( settings.BASE_DIR, settings.MEDIA_ROOT, "extractions", f"{timezone.now().strftime('%Y%m%d%H%M%S')}" )

                filename = base_filename + ".json"
                zip_filename = base_filename + ".zip"

                with open( filename, "a+" ) as file:
                    json_string = json.dumps( results, default=custom_converter )
                    token = f.encrypt( bytes(json_string, 'utf-8') )

                    file.write( token.decode('utf-8') )
                
                # zipping the json file
                with zipfile.ZipFile(zip_filename, mode='a') as archive:
                    archive.write(filename, os.path.basename(filename))
                    
                with open( zip_filename, "rb" ) as __:
                    file = models.File.objects.create( 
                        file=File( __, name=os.path.basename( zip_filename ) ), 
                        size=os.path.getsize(zip_filename), is_deleted=False )

                    extraction = models.Extraction.objects.create(file=file, start_time=start_time, time_made=time_made)

                    # record source info (database, schema, table)
                    for database in database_records:
                        # create extraction source databases, source schemas and source tables
                        source_database = models.ExtractionSourceDatabase.objects.create( database=database['database'], extraction=extraction )

                        for schema in database[self.ExtractionSourceDatabaseSerializer.schemas_key]:
                            source_schema = models.ExtractionSourceDatabaseSchema.objects.create(extraction_database=source_database, schema=schema['schema'])

                            for table in schema[self.ExtractionSourceDatabaseSerializer.schema_tables_key]:
                                models.ExtractionSourceTable.objects.create(extraction_database_schema=source_schema, table=table['table'])

                    # record target databases
                    if 'target_databases' in validated_data and validated_data['target_databases']:
                        for database in validated_data['target_databases']:
                            synchronized_flag = False

                            try:
                                connection = get_database_connection(database)

                                if connection:
                                    synchronize_database(connection, database, results)
                                    flag = True
                                else: 
                                    flag = False

                            except TypeError as e:
                                raise e
                            # except Exception as e:
                            #     logging.error(f"The {database.__str__()} database was not synchronized successfully")
                            #     logging.error(f"The exception raised: {str(e)}")
                            #     flag = False
                            #     raise e

                            models.ExtractionTargetDatabase.objects.create(extraction=extraction, database=database, is_applied=synchronized_flag)

                os.unlink( filename )
                os.unlink( zip_filename )

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
>>>>>>> 52e1a53dc5de9b95f1f3b424b2c812b1237a5c0d
