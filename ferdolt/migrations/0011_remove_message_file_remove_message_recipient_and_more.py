# Generated by Django 4.1 on 2022-08-24 13:35

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0010_file_synchronization_message_extraction'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='message',
            name='file',
        ),
        migrations.RemoveField(
            model_name='message',
            name='recipient',
        ),
        migrations.RemoveField(
            model_name='synchronization',
            name='file',
        ),
        migrations.RemoveField(
            model_name='synchronization',
            name='source',
        ),
        migrations.DeleteModel(
            name='Extraction',
        ),
        migrations.DeleteModel(
            name='File',
        ),
        migrations.DeleteModel(
            name='Message',
        ),
        migrations.DeleteModel(
            name='Synchronization',
        ),
    ]
