# Generated by Django 4.1 on 2022-10-12 17:04

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0004_create_default_server_and_set_database_to_default_server'),
        ('groups', '0001_initial'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='grouptabletable',
            unique_together={('group_table', 'table')},
        ),
    ]