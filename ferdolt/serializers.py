import logging

from django.contrib.auth.models import User
from django.utils.translation import gettext as _

from rest_framework import serializers
from rest_framework.authtoken.models import Token

import psycopg
import pyodbc

from common.functions import is_valid_hostname
from core.functions import decrypt, encrypt, initialize_database

from ferdolt_web import settings
from frontend.views import get_database_connection
from groups import models as groups_models

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
    version_object = DatabaseManagementSystemVersionSerializer(source='dbms_version', read_only=True)
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
        'host', 'port', 'schemas', 'version_object', 'dbms_version', 'instance_name', "clear_username", "clear_password", 
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
        if is_valid_hostname(data):
            data = encrypt(data)[1]
            return data
        else:
            raise serializers.ValidationError(
                _("%(ip)s is not a valid host name or ip address" % { 'ip': data })
            )

    def validate_port(self, data):
        data = encrypt(data)[1]
        return data

    def validate(self, attrs):
        query = models.Database.objects.filter(name=attrs['name'])

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

        return attrs

    def create(self, validated_data) -> models.Database:
        logging.info("In ferdolt.serializers.DatabaseSerializer, adding database")
        version = validated_data.pop("dbms_version")

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
                    fields = ( "id", "name", "data_type", "character_maximum_length", "datetime_precision", "numeric_precision")
            
            columns = DatabaseTableColumns(many=True, allow_null=True, required=False, source="column_set")
            class Meta:
                model = models.Table
                fields = ("id", "name", "columns")

        tables = DatabaseTables(many=True, allow_null=True, required=False, source="table_set")
        class Meta:
            model = models.DatabaseSchema
            fields = ( "name", "tables" )

    schemas = DatabaseSchemas(source='databaseschema_set', many=True, read_only=True)
    username = serializers.CharField(source='get_username')
    password = serializers.CharField(source='get_password')
    port = serializers.CharField(source='get_port')
    host = serializers.CharField(source='get_host')
    version_object = DatabaseManagementSystemVersionSerializer(source='dbms_version', read_only=True)
    
    class Meta: 
        model = models.Database
        fields = ( "id", "name", "username", 
        "password", 'host', 'port', 'schemas', 
        'dbms_version', 'instance_name', 'version_object' )

class UpdateDatabaseSerializer(serializers.Serializer):
    username = serializers.CharField(write_only=True)
    password = serializers.CharField(write_only=True)
    host = serializers.CharField(write_only=True)
    port = serializers.CharField(write_only=True)
    name = serializers.CharField(write_only=True)

    def validate_username(self, data):
        data = encrypt(data)[1]
        return data

    def validate_password(self, data):
        data = encrypt(data)[1]
        return data

    def validate_host(self, data):
        if is_valid_hostname(data):
            data = encrypt(data)[1]
            return data
        else:
            raise serializers.ValidationError(
                _("%(ip)s is not a valid host name or ip address" % { 'ip': data })
            )

    def validate_port(self, data):
        data = encrypt(data)[1]
        return data

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
    class ServerUserSerializer(serializers.ModelSerializer):
        auth_token = serializers.SerializerMethodField()

        class Meta:
            model = User
            fields = ("id", "username", "password", "auth_token")
            extra_kwargs = {
                "password": {"write_only": True},
                "username": { "validators": [] }
            }

        def get_auth_token(self, obj):
            token = Token.objects.get_or_create(user=obj)
            return token[0].key

    user = ServerUserSerializer(required=False, allow_null=True)
    request = serializers.IntegerField(allow_null=True, required=False, write_only=True)

    def validate_request(self, data) -> models.CreateServerRequest:
        query = models.CreateServerRequest.objects.filter(id=data)

        if not query.exists():
            raise serializers.ValidationError(_("No server request exists with id '%(id)d'" % { 'id': data }))
        else:
            request: models.CreateServerRequest = query.first()

            if request.is_accepted is not None:
                raise serializers.ValidationError(_("This request has already been treated. A new server cannot be created from it"))
            
            return request

    class Meta:
        model = models.Server
        fields = ( "id", "name", "address", "port", "location", "server_id", "user", "request" )
        extra_kwargs = {
            "server_id": {"read_only": True},
            "port": {"required": False, "allow_null": True},
            "address": {"required": False, "allow_null": True}
        }

    def validate_user(self, data):
        if User.objects.filter(username=data['username']).exists():
            raise serializers.ValidationError(_("The username %(name)s has already been taken" % {'name': data['username']}))
        else:
            user = User.objects.create(username=data['username'])
            user.set_password(data['password'])

            return user

    def create(self, validated_data):
        user = validated_data.pop("user") if "user" in validated_data else None
        request = validated_data.pop("request") if "request" in validated_data else None

        server = models.Server.objects.create(**validated_data, user=user)

        if request is not None:
            request.server = server
            request.save()

        return server

class DeleteServersSerializer(serializers.Serializer):
    servers = serializers.ListField(child=serializers.IntegerField())

    def validate_servers(self, data):
        validation_errors = []
        for index, server_id in enumerate(data):
            server = models.Server.objects.filter(id=server_id).first()

            if not server:
                validation_errors.append(
                    serializers.ValidationError( _("Error on item at position %(position)d on the servers list. No server exists with id %(server_id)d" % { 'position': index+1, 'server_id': server_id }) )
                )
            else:
                data[index] = server

        if len(validation_errors) > 0:
            raise serializers.ValidationError( validation_errors )

        return data

class AddServersToGroupsSerializer(DeleteServersSerializer):
    groups = serializers.ListField(child=serializers.IntegerField())

    def validate_groups(self, data):
        validation_errors = []

        for index, group_id in enumerate(data):
            group = groups_models.Group.objects.filter(id=group_id)

            if not group:
                validation_errors.append(
                    serializers.ValidationError( _("Error on item at position %(position)d on the groups list. No group exists with id %(group_id)d" % { 'position': index+1, 'group_id': group_id }) )
                )
            else:
                data[index] = group

        if len(validation_errors) > 0:
            raise serializers.ValidationError( validation_errors )

        return data

class CreateServerRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.CreateServerRequest
        fields = (
            "time_made", "code", "server_created", 
            "is_accepted", "fernet_key", "name", "location"
        )

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
