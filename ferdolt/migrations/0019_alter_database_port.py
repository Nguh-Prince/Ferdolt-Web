# Generated by Django 4.1 on 2022-09-07 09:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0018_database_time_added'),
    ]

    operations = [
        migrations.AlterField(
            model_name='database',
            name='port',
            field=models.CharField(default='1433', max_length=5),
        ),
    ]
