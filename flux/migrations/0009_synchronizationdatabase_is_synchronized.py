# Generated by Django 4.1 on 2022-09-07 07:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0008_set_file_hashes'),
    ]

    operations = [
        migrations.AddField(
            model_name='synchronizationdatabase',
            name='is_synchronized',
            field=models.BooleanField(default=True),
            preserve_default=False,
        ),
    ]