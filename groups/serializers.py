from rest_framework import serializers

from . import models

class GroupColumnConstraintSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.GroupColumnConstraint
        fields = ( "column", "is_unique", "is_foreign_key", "is_primary_key", "references" )

class GroupDatabaseSerializer(serializers.ModelSerializer):
    class GroupDatabaseTables(serializers.ModelSerializer):
        class Meta:
            model = models.GroupTable
            fields = ("id", "name", "table",)
    
    tables = GroupDatabaseTables(source='table_set', read_only=True, allow_null=True)
    class Meta:
        model = models.GroupDatabase
        fields = ("id", "name", "database", "is_writeable", "is_readable", "tables")

class GroupDatabaseCreationSerializer(serializers.ModelSerializer):
    class GroupDatabaseTables(serializers.ModelSerializer):
        
        class GroupTableColumns(serializers.ModelSerializer):
        
            class GroupTableColumnConstraints(serializers.ModelSerializer):
                class Meta: 
                    model = models.GroupColumnConstraint
                    fields = ( "is_unique", "is_foreign_key", "is_primary_key" )
        
            class Meta:
                model = models.GroupColumn
                fields = ("id", "name", "column", "is_required", "constraints")
        
        class Meta:
            model = models.GroupTable
            fields = ("id", "name", "table", "columns")
    
    tables = GroupDatabaseTables(source='table_set', read_only=True, allow_null=True)
    class Meta:
        model = models.GroupDatabase
        fields = ("id", "name", "database", "is_writeable", "is_readable", "tables")

