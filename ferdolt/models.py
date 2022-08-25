from django.db import models
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
    instance_name = models.CharField(max_length=100, null=True)
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

class Table(models.Model):
    schema = models.ForeignKey(DatabaseSchema, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)

    class Meta:
        verbose_name = _("Table")
        verbose_name_plural = _("Tables")
        unique_together = [
            [ "schema", "name" ]
        ]
        ordering = [ "schema__database__name", "schema__name", "name" ]

    def __str__(self):
        return f"{self.schema.database.name}.{self.schema.name}.{self.name}"

class Column(models.Model):
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    data_type = models.CharField(max_length=50)
    datetime_precision = models.IntegerField(null=True)
    character_maximum_length = models.IntegerField(null=True)
    numeric_precision = models.IntegerField(null=True)
    is_nullable = models.BooleanField(default=True)

    class Meta:
        verbose_name = _("Column")
        verbose_name_plural = _("Columns")
        unique_together = [
            ["table", "name"]
        ]

    def __str__(self):
        return f"{self.table.__str__()} {self.name}"

class ColumnConstraint(models.Model):
    column = models.ForeignKey(Column, on_delete=models.CASCADE)
    is_primary_key = models.BooleanField(default=False)
    is_foreign_key = models.BooleanField(default=False)

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
    location = models.TextField(null=True)
    server_id = models.CharField(max_length=ID_MAX_LENGTH, unique=True, default=generate_server_id)
    address = models.CharField(max_length=150)
    port = models.IntegerField(null=True)