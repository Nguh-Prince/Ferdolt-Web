# Generated by Django 4.1.3 on 2022-11-14 15:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0008_createserverrequest'),
    ]

    operations = [
        migrations.AddField(
            model_name='createserverrequest',
            name='fernet_key',
            field=models.TextField(default=1),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='createserverrequest',
            name='location',
            field=models.TextField(default=1),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='createserverrequest',
            name='name',
            field=models.CharField(default=1, max_length=50),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='createserverrequest',
            name='notification_email_address',
            field=models.EmailField(default=1, max_length=254),
            preserve_default=False,
        ),
    ]