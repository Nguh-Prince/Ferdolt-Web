# Generated by Django 4.1 on 2022-09-07 07:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0009_synchronizationdatabase_is_synchronized'),
    ]

    operations = [
        migrations.AlterField(
            model_name='synchronizationdatabase',
            name='is_synchronized',
            field=models.BooleanField(default=False),
        ),
    ]
