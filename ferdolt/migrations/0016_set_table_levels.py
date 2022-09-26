# Generated by Django 4.1 on 2022-08-30 17:14

import logging
from django.db import migrations
from django.db.models import Q

def get_table_level(apps, table):
    Table = apps.get_model("ferdolt", "Table")
    ColumnConstraint = apps.get_model("ferdolt", "ColumnConstraint")

    level = 0

    referenced_tables = Table.objects.filter( 
        id__in=ColumnConstraint.objects.filter( 
            Q( references__isnull=False ) & Q( column__table=table ) & ~Q( references__table=table ) )
            .values("references__table__id") 
        )

    if not referenced_tables.exists():
        return 0
    else:
        for referenced_table in referenced_tables:
            level = max( level, get_table_level( apps, referenced_table ) )
    
    return level + 1

def set_table_levels(apps, schema_editor):
    Table = apps.get_model("ferdolt", "Table")

    for table in Table.objects.all():
        logging.info(f"Setting the level for table: {table.__str__()}")
        level = get_table_level(apps, table)
        table.level = level
        table.save()

class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0015_alter_column_options_alter_columnconstraint_options_and_more'),
    ]

    operations = [
        migrations.RunPython( set_table_levels, reverse_code=migrations.RunPython.noop )
    ]
