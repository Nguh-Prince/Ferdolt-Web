# Generated by Django 4.1 on 2022-09-02 11:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('groups', '0004_groupcolumn_data_type_alter_groupdatabase_database_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='groupcolumn',
            name='is_nullable',
            field=models.BooleanField(default=True),
        ),
    ]
