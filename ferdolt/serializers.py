from rest_framework import serializers
from django.utils.translation import gettext as _

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
            fields = ( "name" )
    
    schemas = DatabaseSchemas(source='schema_set', many=True, read_only=True)
    version = DatabaseManagementSystemVersionSerializer(source='dbms_version')

    class Meta: 
        model = models.Database
        fields = ( "id", "name", "username", "password", 'host', 'port', 'schemas', 'version', 'instance_name' )

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

        return instance

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
        fields = ( "table", "name", "data_type", "datetime_precision", "character_maximum_length", "numeric_precision" )

class ColumnConstraintSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ColumnConstraint
        fields = ("column", "is_primary_key", "is_foreign_key")

class ServerSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Server
        fields = ( "name", "address", "port", "location" )

class TableInsertSerializer(serializers.Serializer):
    # serializer for DML operations on the table
    database = serializers.CharField(max_length=150)
    schema = serializers.CharField(max_length=150)
    table = serializers.CharField(max_length=150)
    data = serializers.ListField(allow_empty=False)
    atomic = serializers.BooleanField(required=False) # set to True if all the records have to be added or none of them are

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

class TableUpdateSerializer(TableInsertSerializer):
    reference_columns = serializers.ListField(allow_empty=True, child=serializers.CharField()) 
    # these are the columns to use for the WHERE clause in the update, if empty the primary key columns will be used instead
    def validate(self, attrs):
        breakpoint()
        return super().validate(attrs)

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
