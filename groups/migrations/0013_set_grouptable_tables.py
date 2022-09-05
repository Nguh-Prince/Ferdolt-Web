# Generated by Django 4.1 on 2022-09-05 07:45

import logging
from django.db import migrations

def set_grouptable_tables(apps, schema_editor):
    GroupTable = apps.get_model("groups", "GroupTable")
    Table = apps.get_model("ferdolt", "Table")
    GroupColumnColumn = apps.get_model("groups", "GroupColumnColumn")
    
    for group_table in GroupTable.objects.all():
        logging.info(f"Setting the table for group_table: {group_table}")
        table = Table.objects.filter( 
            column__id__in=GroupColumnColumn.objects
            .filter( group_column__group_table=group_table )
            .values("column__id")
        ).first()

        group_table.table = table
        group_table.save()


class Migration(migrations.Migration):

    dependencies = [
        ('groups', '0012_grouptable_table_and_more'),
    ]

    operations = [
        migrations.RunPython( set_grouptable_tables
        , reverse_code=migrations.RunPython.noop )
    ]
