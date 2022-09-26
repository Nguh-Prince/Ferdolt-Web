# Generated by Django 4.1 on 2022-08-24 12:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0008_auto_20220824_1117'),
    ]

    operations = [
        migrations.AddField(
            model_name='server',
            name='address',
            field=models.CharField(default='localhost', max_length=150),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='server',
            name='port',
            field=models.IntegerField(null=True),
        ),
        migrations.AlterField(
            model_name='server',
            name='location',
            field=models.TextField(null=True),
        ),
    ]
