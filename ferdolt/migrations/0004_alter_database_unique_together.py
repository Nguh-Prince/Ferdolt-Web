# Generated by Django 4.1 on 2022-08-23 13:47

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0003_database_host_database_port'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='database',
            unique_together={('dbms_version', 'name', 'host', 'port')},
        ),
    ]
