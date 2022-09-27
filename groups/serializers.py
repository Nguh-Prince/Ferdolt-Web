from typing import Iterable

from django.utils.translation import gettext as _

from rest_framework import serializers
from core.functions import decrypt

from ferdolt.models import Column, Database
from ferdolt.serializers import ColumnSerializer, DatabaseSerializer
from . import models

class GroupColumnConstraintSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.GroupColumnConstraint
        fields = ( "id", "is_unique", "is_foreign_key", "is_primary_key", "references" )

class GroupColumnSerializer(serializers.ModelSerializer):
    constraints = GroupColumnConstraintSerializer(source='groupcolumnconstraint_set', many=True, required=False)

    columns = ColumnSerializer(many=True, required=False)

    class Meta:
        model = models.GroupColumn
        fields = ( "id", "name", "is_required", "data_type", "constraints", "columns" )

class GroupTableSerializer(serializers.ModelSerializer):
    columns = GroupColumnSerializer(many=True)

    class Meta:
        model = models.GroupTable
        fields = ( "id", "name", "columns" )

class GroupCreationSerializer(serializers.ModelSerializer):
    class DatabaseSerializer(serializers.ModelSerializer):
        class Meta:
            model = Database
            fields = ("id", "name", "host", "port")

    databases = DatabaseSerializer(many=True, required=False, allow_null=True)
    tables = GroupTableSerializer(many=True)

    class Meta:
        model = models.Group
        fields = ("name", "databases", "tables")

    def validate_databases(self, databases):
        database_records = []

        if not isinstance(databases, Iterable):
            raise serializers.ValidationError( _("Expected an iterable for databases, got a %(type)s instead" % {'type': type(databases)}) )

        for index, database in enumerate(databases):
            try:
                database_record = Database.objects.get( name=database['name'], host=database['host'], port=database['port'] )
                database_records.append(database_record)
            except Database.DoesNotExist as e:
                raise serializers.ValidationError( _("The database on index %(index)d of the list does not exist" % {'index': index}) )

        return database_records

    def create(self, validated_data):
        databases = validated_data.pop('databases')
        tables = validated_data.pop('tables')

        group = models.Group.objects.create(**validated_data)

        for _table in tables:
            columns = _table.pop('columns')

            table = models.GroupTable.objects.create(**_table, group=group)

            for _column in columns:
                try:
                    constraints = _column.pop('groupcolumnconstraint_set')
                except KeyError as e:
                    constraints = ()

                column = models.GroupColumn.objects.create(**_column, group_table=table)

                for _constraint in constraints:
                    models.GroupColumnConstraint.objects.create(**_constraint, column=column)

        for _database in databases:
            models.GroupDatabase.objects.create( group=group, database=_database )

        return group

class GroupDetailSerializer(serializers.ModelSerializer):
    class GroupDatabaseSerializer(serializers.ModelSerializer):
        database_id = serializers.CharField(source='database.id')
        database_name = serializers.CharField(source='database.name')
        database_host = serializers.CharField(source='database.get_host')
        database_port = serializers.CharField(source='database.get_port')

        class Meta:
            model = models.GroupDatabase
            fields = ( "id", "database_id", "database_name", "database_host", "database_port" )
    
    databases = GroupDatabaseSerializer(many=True, required=False, source='groupdatabase_set')
    tables = GroupTableSerializer(many=True)

    class Meta:
        model = models.Group
        fields = ("id", "name", "databases", "tables")

class GroupDatabaseSerializer(serializers.ModelSerializer):
    database_name = serializers.CharField( source="database.name", required=False )
    database_host = serializers.CharField( source="database.host", required=False )
    database_port = serializers.CharField( source="database.port", required=False )

    class Meta:
        model = models.GroupDatabase
        fields = ("id", "database_id", "database_name", "database_host", "database_port")


class ExtractFromGroupSerializer(serializers.ModelSerializer):
    use_time = serializers.BooleanField(default=True, required=False)

    class Meta:
        model = models.GroupExtraction
        fields = ( "source_database", "use_time")

    def validate_source_database(self, data):
        if not data.can_write:
            raise serializers.ValidationError( _("This database is not allowed to extract into this group") )

        return data

    def validate(self, attrs):
        group = attrs['source_database'].group

        if 'use_time' not in attrs:
            attrs['use_time'] = True
        
        # set the target_databases to all the databases in the group if no target_databases are passed
        if 'target_databases' not in attrs or attrs['target_databases'] is None:
            attrs['target_databases'] = group.groupdatabase_set.all()

        return super().validate(attrs)

class GroupExtractionSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.GroupExtraction
        fields = ( "id", "extraction", "group" ) 

class GroupDatabaseSynchronizationSerializer(serializers.ModelSerializer):
    group_database = GroupDatabaseSerializer()

    class Meta:
        model = models.GroupDatabaseSynchronization
        fields = ("id", "group_database", "extraction", "is_applied", "time_applied")

class GroupSerializer(serializers.ModelSerializer):
    class GroupDatabaseSimpleSerializer(serializers.ModelSerializer):
        class Meta:
            model = Database
            fields = ( "id", "name", "host" )

    databases = GroupDatabaseSimpleSerializer(many=True)

    class Meta:
        model = models.Group
        fields = ( "id", "name", "tables", "databases" )

class GroupDisplaySerializer(GroupSerializer):
    class GroupTableSimpleSerializer(serializers.ModelSerializer):
        class Meta:
            model = models.GroupTable
            fields = ( "id", "name", "column_count" )
    
    tables = GroupTableSerializer(many=True)    

class LinkColumnsToGroupColumnsSerializer(serializers.Serializer):
    class LinkColumnToGroupColumnSerializer(serializers.Serializer):
        group_column = serializers.IntegerField()
        column = serializers.IntegerField()

        def validate_group_column(self, data):
            try:
                group_column = models.GroupColumn.objects.get(id=data)
                return group_column
            except models.GroupColumn.DoesNotExist as e:
                raise serializers.ValidationError( _("No group exists with the id %(id)d" % { 'id': data } ) )

        def validate_column(self, data):
            try:
                column = Column.objects.get(id=data)
                return column
            except Column.DoesNotExist as e:
                raise serializers.ValidationError( _("No column exists with id %(id)d" % { 'id': data }) )

        def validate(self, attrs):
            column: Column = attrs['column']
            group_column: models.GroupColumn = attrs['group_column']

            if column not in Column.objects.filter( table__schema__database__id__in=group_column.group_table.group.groupdatabase_set.values('database__id') ):
                raise serializers.ValidationError( _("The column is not from a database that has been added to the %(group_name)s group" % { 'group_name': group_column.group_table.group.name }) )

            return super().validate(attrs)

    data = serializers.ListField(child=LinkColumnToGroupColumnSerializer())

class GroupColumnColumnSerializer(serializers.ModelSerializer):
    column = ColumnSerializer()
    group_column = GroupColumnSerializer()

    class Meta:
        model = models.GroupColumnColumn
        fields = ( 'id', 'column', 'group_column' )

class SynchronizationGroupSerializer(serializers.Serializer):
    synchronization_type_choices = (
        ("full", _("Full synchronization")),
        # ("partial", _("Partial synchronization"))
    )
    type = serializers.ChoiceField(choices=synchronization_type_choices)
    
    # if this is not provided then all the databases can receive and provide data
    sources = serializers.ListField(child=serializers.IntegerField(), required=False)
    participants = serializers.ListField(child=serializers.IntegerField())

    def validate_list_of_database_ids(self, list_of_database_ids):
        temporary_list = []

        for index, item in enumerate(list_of_database_ids):
            try:
                database = Database.objects.get(id=item)
                temporary_list.append(database)
            except Database.DoesNotExist as e:
                raise serializers.ValidationError(
                    _("Error on index %(index)d: No database exists with id %(id)d" % 
                        {'index': index+1, 'id': item}
                    )
                )
        return temporary_list

    def validate_sources(self, data):
        return self.validate_list_of_database_ids(data)

    def validate_participants(self, data):
        return self.validate_list_of_database_ids(data)

