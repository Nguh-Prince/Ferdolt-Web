# Generated by Django 4.1 on 2022-10-15 15:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0005_historicalserver_user_server_user'),
    ]

    operations = [
        migrations.AddField(
            model_name='databasemanagementsystem',
            name='codename',
            field=models.CharField(blank=True, max_length=12, null=True, unique=True),
        ),
    ]