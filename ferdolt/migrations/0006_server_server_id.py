# Generated by Django 4.1 on 2022-08-24 10:17

from django.db import migrations, models
import ferdolt.models


class Migration(migrations.Migration):
    atomic: bool = False

    dependencies = [
        ('ferdolt', '0005_server'),
    ]

    operations = [
        migrations.AddField(
            model_name='server',
            name='server_id',
            field=models.CharField(max_length=15, null=True),
        ),
    ]
