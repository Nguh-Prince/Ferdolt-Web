# Generated by Django 4.1 on 2022-09-22 08:27

from django.db import migrations, models
import groups.models


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0025_alter_database_port_alter_historicaldatabase_port'),
        ('groups', '0019_generate_group_fernet_keys'),
    ]

    operations = [
        migrations.AlterField(
            model_name='group',
            name='fernet_key',
            field=models.TextField(default=groups.models.generate_unique_fernet_key, null=True, unique=True),
        ),
        migrations.AlterField(
            model_name='group',
            name='name',
            field=models.CharField(default=groups.models.generate_unique_group_name, max_length=50, unique=True),
        ),
        migrations.AlterUniqueTogether(
            name='groupdatabase',
            unique_together={('database', 'group')},
        ),
        migrations.AlterUniqueTogether(
            name='grouptable',
            unique_together={('group', 'name')},
        ),
    ]
