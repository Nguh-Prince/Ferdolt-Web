# Generated by Django 4.1 on 2022-10-12 17:04

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='file',
            name='password',
            field=models.TextField(null=True),
        ),
        migrations.AddField(
            model_name='historicalfile',
            name='password',
            field=models.TextField(null=True),
        ),
        migrations.AlterField(
            model_name='extraction',
            name='file',
            field=models.OneToOneField(on_delete=django.db.models.deletion.PROTECT, to='flux.file'),
        ),
    ]
