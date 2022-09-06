from django.db import models
from django.db.models import Q
from django.utils.translation import gettext as _

import string
import random

class DatabaseManagementSystem(models.Model):
    name = models.CharField(max_length=50, unique=True, null=False)

    class Meta:
        verbose_name = _("Database management system")
        verbose_name_plural = _("Database management systems")

    def __str__(self) -> str:
        return self.name

class DatabaseManagementSystemVersion(models.Model):
    dbms: DatabaseManagementSystem = models.ForeignKey(DatabaseManagementSystem, on_delete=models.PROTECT)
    version_number = models.CharField(max_length=25)

    class Meta:
        verbose_name = _("Database management system version")
        verbose_name_plural = _("Database management system versions")

    def __str__(self):
        return f"{self.dbms.name} v{self.version_number}"

class Database(models.Model):
    dbms_version: DatabaseManagementSystemVersion = models.ForeignKey(DatabaseManagementSystemVersion, on_delete=models.PROTECT)
    name = models.CharField(max_length=100)
    username = models.CharField(max_length=150)
    password = models.TextField()
    instance_name = models.CharField(max_length=100, null=True, blank=True)
    host = models.CharField(max_length=150, default="localhost")
    port = models.IntegerField(default=1433);

    class Meta:
        unique_together = [
            ["dbms_version", "name", "host", "port"]
        ]
        verbose_name = _("Database")
        verbose_name_plural = _("Databases")

    def __str__(self):
        return f"{self.dbms_version.__str__()} ({self.name})"

    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        
        return super().save(*args, **kwargs)

class DatabaseSchema(models.Model):
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)

    class Meta:
        verbose_name = _("Database schema")
        verbose_name_plural = _("Database schemas")
        unique_together = [
            [ "database", "name" ]
        ]

    def __str__(self):
        return f"{self.database.name}.{self.name}"
    
    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        
        return super().save(*args, **kwargs)

class Table(models.Model):
    # validations
    # on insert, non-nullable columns must be present
    schema = models.ForeignKey(DatabaseSchema, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    level = models.IntegerField(default=0) # this level is the order in which items should be added to tables to avoid integrity errors 
    # starts with 0 (these are the parent tables with no external foreign keys)

    class Meta:
        verbose_name = _("Table")
        verbose_name_plural = _("Tables")
        unique_together = [
            [ "schema", "name" ]
        ]
        ordering = [ "schema__database__name", "schema__name", "name" ]

    def __str__(self):
        return f"{self.schema.database.name}.{self.schema.name}.{self.name}"
    
    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        
        return super().save(*args, **kwargs)

    def get_level(self):
        referenced_tables = Table.objects.filter( 
        id__in=ColumnConstraint.objects.filter( 
            Q( references__isnull=False ) & Q( column__table=self ) & ~Q( references__table=self ) )
            .values("references__table__id") 
        )

        level = 0

        if not referenced_tables.exists():
            return 0
        else:
            for referenced_table in referenced_tables:
                level = max( level, referenced_table.get_level() )

        return level + 1

    def set_level(self):
        level = self.get_level()
        self.level = level
        self.save()

    def get_queryname(self) -> str:
        """
        Gets the schema name and table name or just the table name in cases where the schema is null to be used for query e.g. schema.table
        """
        if self.schema.name:
            return f"{self.schema.name}.{self.name}"
        else:
            return self.name

class Column(models.Model):
    table: Table = models.ForeignKey(Table, on_delete=models.CASCADE)
    name: str = models.CharField(max_length=100)
    data_type: str = models.CharField(max_length=50)
    datetime_precision = models.IntegerField(null=True, blank=True)
    character_maximum_length: str = models.IntegerField(null=True, blank=True)
    numeric_precision: int = models.IntegerField(null=True, blank=True)
    is_nullable: bool = models.BooleanField(default=True)

    class Meta:
        verbose_name = _("Column")
        verbose_name_plural = _("Columns")
        unique_together = [
            ["table", "name"]
        ]
        ordering = [
            "table__schema__database__name", "table__schema__name", "table__name", "name"
        ]

    def __str__(self):
        return f"{self.table.__str__()} {self.name}"

    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        
        return super().save(*args, **kwargs)

class ColumnConstraint(models.Model):
    column: Column = models.ForeignKey(Column, on_delete=models.CASCADE)
    is_primary_key: bool = models.BooleanField(default=False)
    is_foreign_key: bool = models.BooleanField(default=False)
    references: Column = models.ForeignKey(Column, null=True, on_delete=models.SET_NULL, related_name='references', blank=True)

    class Meta:
        ordering = [ "column__table__schema__database__name", "column__table__schema__name", "column__table__name", "column__name", "is_primary_key", "is_foreign_key" ]

    def __str__(self):
        string = ""
        if self.is_primary_key and not self.is_foreign_key:
            string = f"PK({ self.column.name }) on { self.column.table.__str__() }"
        elif self.is_foreign_key and not self.is_primary_key:
            string = f"FK({ self.column.name }) on { self.column.table.__str__() } { f'references {self.references.__str__()}' if self.references else '' }"
        return string

    def save(self, *args, **kwargs):
        # modify the level of the table if need be
        self.column.table.set_level()
        super().save(*args, **kwargs)

def generate_random_string(length, include_uppercase=True, include_lowercase=False, include_digits=True, include_symbols=False, symbol_set:str=''):
        if not include_uppercase and not include_lowercase and not include_digits and not include_symbols:
          raise ValueError("At least one of the following must be true; include_uppercase, include_lowercase, include_digits, include_symbols")

        string_set = ''
        string_set += string.ascii_uppercase if include_uppercase else ''
        string_set += string.ascii_lowercase if include_lowercase else ''
        string_set += string.digits if include_digits else ''
        
        if include_symbols:
            symbol_set = symbol_set if symbol_set else '~`!@#$%^&*()_-=+/?.>,<\|\\]}[{;:\'"'
            string_set += symbol_set

        return  ''.join(random.choices( string_set, k=length ))

def generate_server_id(length=15):
    server_id = None

    while True:
        server_id = generate_random_string(length)

        if not Server.objects.filter(server_id=server_id).exists():
            break

    return server_id

class Server(models.Model):
    ID_MAX_LENGTH = 15
    name = models.CharField(max_length=50, unique=True)
    location = models.TextField(null=True, blank=True)
    server_id = models.CharField(max_length=ID_MAX_LENGTH, unique=True, default=generate_server_id)
    address = models.CharField(max_length=150)
    port = models.IntegerField(null=True, blank=True)

    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        
        return super().save(*args, **kwargs)