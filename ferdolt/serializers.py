import logging
from rest_framework import serializers
import sqlite3
from django.utils.translation import gettext as _

import psycopg
import pyodbc

from core.functions import encrypt, initialize_database

from ferdolt_web import settings
from frontend.views import get_database_connection

from . import models

class DatabaseManagementSystemSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.DatabaseManagementSystem
        fields = ("id", "name", )
        extra_kwargs = {
                "name": {"validators": []}
            }
    
    def validate_name(self, value):
        check_query = self.Meta.model.objects.filter(name=value)

        if check_query.exists() and not (
            isinstance(self.parent, DatabaseManagementSystemVersionSerializer) and 
            (
                self.field_name == "dbms_object"
            )
        ):
            raise serializers.ValidationError(
                _("A database with this name already exists")
            )
        
        return value

class DatabaseManagementSystemVersionSerializer(serializers.ModelSerializer):
    dbms_object = DatabaseManagementSystemSerializer(source='dbms')

    class Meta:
        model = models.DatabaseManagementSystemVersion
        fields = ( "dbms", "version_number", "dbms_object" )

class DatabaseSerializer(serializers.ModelSerializer):
    class DatabaseSchemas(serializers.ModelSerializer):
        class Meta:
            model = models.DatabaseSchema
            fields = ( "name", )
    
    schemas = DatabaseSchemas(source='databaseschema_set', many=True, read_only=True)
    version = DatabaseManagementSystemVersionSerializer(source='dbms_version')
    username = serializers.CharField(write_only=True)
    password = serializers.CharField(write_only=True)
    host = serializers.CharField(write_only=True)
    port = serializers.CharField(write_only=True)

    clear_username = serializers.CharField(read_only=True, source='get_username')
    clear_password = serializers.CharField(read_only=True, source='get_password')
    clear_host = serializers.CharField(read_only=True, source='get_host')
    clear_port = serializers.CharField(read_only=True, source='get_port')

    class Meta: 
        model = models.Database
        fields = ( "id", "name", "username", "password", 
        'host', 'port', 'schemas', 'version', 'instance_name', "clear_username", "clear_password", 
        'clear_host', 'clear_port', 'is_initialized', 'provides_successful_connection')
        extra_kwargs = {
            'is_initialized': {'read_only': True},
            'provides_successful_connection': {'read_only': True}
        }

    def validate_username(self, data):
        data = encrypt(data)[1]
        return data

    def validate_password(self, data):
        data = encrypt(data)[1]
        return data

    def validate_host(self, data):
        data = encrypt(data)[1]
        return data

    def validate_port(self, data):
        data = encrypt(data)[1]
        return data

    def validate(self, attrs):
        query = Database.objects.filter(name=attrs['name'])

        clear_host = decrypt(attrs['host'])[1]
        clear_port = decrypt(attrs['port'])[1]

        for database in query:
            if (database.get_host == clear_host 
                and database.get_port == clear_port 
            ):
                raise serializers.ValidationError(_("A database already exists with the name %(name)s running on %(host)s:%(port)s" % 
                    {
                        'name': attrs['name'], 'host': clear_host, 'port': clear_port
                    }
                ))

    def create(self, validated_data) -> models.Database:
        version = validated_data.pop("dbms_version")

        query = models.DatabaseManagementSystemVersion.objects.filter(version_number=version['version_number'], dbms__name=version['dbms']['name'])
        other_query = None

        if query.exists():
            version = query.first()
        else:
            other_query = models.DatabaseManagementSystem.objects.filter(name=version['dbms']['name'])

            if other_query.exists():
                version = models.DatabaseManagementSystemVersion(version_number=version['version_number'], dbms=other_query.first())
                version.save()

        instance = self.Meta.model(dbms_version=version, **validated_data)
        instance.save()

        connection = get_database_connection(instance)

        if connection:
            instance.provides_successful_connection = True
            try:
                initialize_database(instance)
                instance.is_initialized = True
                instance.save()
            except (pyodbc.ProgrammingError, psycopg.ProgrammingError) as e:
                logging.error(f"Error when initializing the {instance.__str__()} database. Error: {e}")

            instance.save()

        return instance

class DatabaseDetailSerializer(DatabaseSerializer):
    class DatabaseSchemas(serializers.ModelSerializer):
        class DatabaseTables(serializers.ModelSerializer):
            class DatabaseTableColumns(serializers.ModelSerializer):
                class Meta:
                    model = models.Column
                    fields = ( "id", "name", )
            
            columns = DatabaseTableColumns(many=True, allow_null=True, required=False, source="column_set")
            class Meta:
                model = models.Table
                fields = ("name", "columns")

        tables = DatabaseTables(many=True, allow_null=True, required=False, source="table_set")
        class Meta:
            model = models.DatabaseSchema
            fields = ( "name", "tables" )

    schemas = DatabaseSchemas(source='databaseschema_set', many=True, read_only=True)
    username = serializers.CharField(source='get_username')
    password = serializers.CharField(source='get_password')
    port = serializers.CharField(source='get_port')
    host = serializers.CharField(source='get_host')
    
    class Meta: 
        model = models.Database
        fields = ( "id", "name", "username", "password", 'host', 'port', 'schemas', 'version', 'instance_name' )

class DatabaseSchemaSerializer(serializers.ModelSerializer):
    class SchemaTables(serializers.ModelSerializer):
        class Meta:
            model = models.Table
            fields = (
                "name"
            )

    tables = SchemaTables(source="table_set", read_only=True, many=True)
    class Meta:
        model = models.DatabaseSchema
        fields = ( "database", "name", "tables" )

class TableSerializer(serializers.ModelSerializer):

    class TableSchemaSerializer(serializers.ModelSerializer):
        
        class TableDatabaseSerializer(serializers.ModelSerializer):
            version = DatabaseManagementSystemVersionSerializer(source='dbms_version')
            class Meta:
                model = models.Database
                fields = ("id", "name", "version")
        database = TableDatabaseSerializer(read_only=True)
        
        class Meta:
            model = models.DatabaseSchema
            fields = ("id", "name", "database")
    
    schema_object = TableSchemaSerializer(source='schema', read_only=True)

    class TableColumns(serializers.ModelSerializer):
        class Meta:
            model = models.Column
            fields = "__all__"
    
    columns = TableColumns(source='column_set', many=True, read_only=True)

    class Meta:
        model = models.Table
        fields = ( "id", "schema", "schema_object", "name", "columns" )

class ColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Column
        fields = ( "id", "table", "name", "data_type", "datetime_precision", "character_maximum_length", "numeric_precision" )

class ColumnConstraintSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ColumnConstraint
        fields = ("column", "is_primary_key", "is_foreign_key")

class ServerSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Server
        fields = ( "name", "address", "port", "location" )

class TableRecordsSerializer(serializers.Serializer):
    database = serializers.CharField(max_length=150)
    schema = serializers.CharField(max_length=150)
    table = serializers.CharField(max_length=150)

    def validate_database(self, value):
        query = models.Database.objects.filter(name=value)

        if not query.exists():
            raise serializers.ValidationError( _("No database exists with name %(name)s " % { 'name': value }) )
        
        database = query.first()

        connection = get_database_connection(query.first())

        if not connection:
            raise serializers.ValidationError( _("Error connecting to the %(database)s database" % {'database': database.__str__() }) )
        else:
            connection.close()

        return database

    
    def validate(self, attrs):
        database = attrs.get("database")

        try:
            attrs['schema'] = database.databaseschema_set.get(name=attrs['schema'])
            attrs['table'] = attrs['schema'].table_set.get(name=attrs['table'])

        except models.DatabaseSchema.DoesNotExist as schema_exception:
            raise serializers.ValidationError( _("The %(schema_name)s schema does not exist in the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name } ) )
        except models.Table.DoesNotExist as table_exception:
            raise serializers.ValidationError( _("The %(table_name)s table does not exist in the %(schema_name)s schema of the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name, 'table_name': attrs['table'] } ) )

        return super().validate(attrs)


class TableInsertSerializer(serializers.Serializer):
    # serializer for DML operations on the table
    database = serializers.CharField(max_length=150)
    schema = serializers.CharField(max_length=150)
    table = serializers.CharField(max_length=150)
    data = serializers.ListField(allow_empty=False)
    atomic = serializers.BooleanField(required=False) # set to True if all the records have to be added or none of them are

    def validate_database(self, value):
        query = models.Database.objects.filter(name__iexact=value)

        if not query.exists():
            raise serializers.ValidationError( _("No database exists with name %(name)s " % { 'name': value }) )
        
        database = query.first()

        connection = get_database_connection(query.first())

        if not connection:
            raise serializers.ValidationError( _("Error connecting to the %(database)s database" % {'database': database.__str__() }) )

        return database

    def validate(self, attrs):
        database = attrs.get("database")
        validation_errors = []

        try:
            attrs['schema'] = database.databaseschema_set.get(name=attrs['schema'])
            attrs['table'] = attrs['schema'].table_set.get(name=attrs['table'])

            attrs['atomic'] = 'atomic' in attrs and attrs['atomic']

            table_column_set = attrs['table'].column_set.all()

            table_columns = set([ f['name'] for f in table_column_set.values("name") ])
            table_primary_key_columns =  [ f['name'] for f in table_column_set.filter(columnconstraint__is_primary_key=True).values("name") ]
            not_null_columns = set( [ f.name.lower() for f in table_column_set.filter(is_nullable=False) ] )
            
            for i in range( len( attrs['data'] ) ):
                if not isinstance( attrs['data'][i], dict ):
                    validation_errors.append(
                        serializers.ValidationError( _("Invalid type. Expected a dictionary on item %(index)d of the list, got a %(type)s instead" 
                    % { 'index': i+1, 'type': type( attrs['data'][i] ) }) )
                    )
            
                columns_in_common = table_columns & attrs['data'][i].keys()
                attrs['columns_in_common'] = columns_in_common

                # at least one key in the dictionary must correspond to a column in the target table
                if not columns_in_common:
                    validation_errors.append(
                        serializers.ValidationError( _("Error on item %(index)d, at least one of the keys in the dictionary must be in %(set)s set" 
                        % { 'set': str(columns_in_common), 'index': i+1}) )
                    )
                
                not_null_columns_not_found = not_null_columns - columns_in_common

                if not_null_columns_not_found:
                    validation_errors.append(
                        serializers.ValidationError( _("Error on item %(index)d, the following non null columns were missing %(set)s" % {
                            'index': i+1, 'set': str( not_null_columns_not_found )
                        }) )
                    )

            if validation_errors:
                raise serializers.ValidationError(validation_errors)

        except models.DatabaseSchema.DoesNotExist as schema_exception:
            raise serializers.ValidationError( _("The %(schema_name)s schema does not exist in the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name } ) )
        except models.Table.DoesNotExist as table_exception:
            raise serializers.ValidationError( _("The %(table_name)s table does not exist in the %(schema_name)s schema of the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name, 'table_name': attrs['table'] } ) )

        return super().validate(attrs)

class TableUpdateSerializer(TableInsertSerializer):
    # reference_columns = serializers.ListField(allow_empty=True, child=serializers.CharField()) 
    # these are the columns to use for the WHERE clause in the update, if empty the primary key columns will be used instead
    def validate(self, attrs):
        database = attrs.get("database")
        validation_errors = []

        try:
            attrs['schema'] = database.databaseschema_set.get(name=attrs['schema'])
            attrs['table'] = attrs['schema'].table_set.get(name=attrs['table'])

            attrs['atomic'] = 'atomic' in attrs and attrs['atomic']

            table_columns = set([ f['name'] for f in attrs['table'].column_set.values("name") ])
            table_primary_key_columns =  [ f['name'] for f in attrs['table'].column_set.filter(columnconstraint__is_primary_key=True).values("name") ]

            for i in range( len( attrs['data'] ) ):
                if not isinstance( attrs['data'][i], dict ):
                    validation_errors.append(
                        serializers.ValidationError( _("Invalid type. Expected a dictionary on item %(index)d of the list, got a %(type)s instead" 
                    % { 'index': i+1, 'type': type( attrs['data'][i] ) }) )
                    )
            
                columns_in_common = table_columns & attrs['data'][i]['current'].keys()
                
                attrs['columns_in_common'] = columns_in_common

                update_columns_in_common = table_columns & attrs['data'][i]['update'].keys()
                attrs['update_columns_in_common'] = update_columns_in_common

                # at least one key in the dictionary must correspond to a column in the target table
                if not columns_in_common:
                    validation_errors.append(
                        serializers.ValidationError( _("At least one of the keys in the current dictionary must be in the %(set)s set" 
                        % { 'set': str(table_columns) }) )
                    )
                
                if not update_columns_in_common:
                    validation_errors.append(
                        serializers.ValidationError( _("At least one of the keys in the update dictionary must be in the %(set)s set" % { 'set': str(table_columns) } ) )
                    )

            if validation_errors:
                raise serializers.ValidationError(validation_errors)

        except models.DatabaseSchema.DoesNotExist as schema_exception:
            raise serializers.ValidationError( _("The %(schema_name)s schema does not exist in the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name } ) )
        except models.Table.DoesNotExist as table_exception:
            raise serializers.ValidationError( _("The %(table_name)s table does not exist in the %(schema_name)s schema of the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name, 'table_name': attrs['table'] } ) )

        return attrs

class TableDeleteSerializer(serializers.Serializer):
    database = serializers.CharField(max_length=150)
    schema = serializers.CharField(max_length=150)
    table = serializers.CharField(max_length=150)
    data = serializers.ListField(allow_empty=False)
    atomic = serializers.BooleanField(required=False) # set to True if all the records have to be deleted or none of them are

    def validate_database(self, value):
        query = models.Database.objects.filter(name=value)

        if not query.exists():
            raise serializers.ValidationError( _("No database exists with name %(name)s " % { 'name': value }) )
        
        database = query.first()

        connection = get_database_connection(query.first())

        if not connection:
            raise serializers.ValidationError( _("Error connecting to the %(database)s database" % {'database': database.__str__() }) )

        return database

    def validate(self, attrs):
        database = attrs.get("database")

        try:
            attrs['schema'] = database.databaseschema_set.get(name=attrs['schema'])
            attrs['table'] = attrs['schema'].table_set.get(name=attrs['table'])

            attrs['atomic'] = 'atomic' in attrs and attrs['atomic']

        except models.DatabaseSchema.DoesNotExist as schema_exception:
            raise serializers.ValidationError( _("The %(schema_name)s schema does not exist in the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name } ) )
        except models.Table.DoesNotExist as table_exception:
            raise serializers.ValidationError( _("The %(table_name)s table does not exist in the %(schema_name)s schema of the %(database_name)s database" 
            % { 'schema_name': attrs['schema'], 'database_name': database.name, 'table_name': attrs['table'] } ) )

        return super().validate(attrs)
