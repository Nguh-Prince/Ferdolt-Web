# Generated by Django 4.1 on 2022-10-19 22:48

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0007_create_default_dbms_versions'),
        ('groups', '0006_groupextraction_code'),
    ]

    operations = [
        migrations.AddField(
            model_name='group',
            name='created_by',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='ferdolt.server'),
        ),
    ]
