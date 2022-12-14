# Generated by Django 4.1 on 2022-10-19 22:49

from django.db import migrations

from common.functions import generate_random_string

def set_groupextraction_codes(apps, schema_editor):
    GroupExtraction = apps.get_model("groups", "GroupExtraction")

    for extraction in GroupExtraction.objects.filter(code__isnull=True):
        code = generate_random_string(10)
        extraction.code = code
        extraction.save()
        
class Migration(migrations.Migration):

    dependencies = [
        ('groups', '0007_group_created_by'),
    ]

    operations = [
        migrations.RunPython(
            set_groupextraction_codes, reverse_code=migrations.RunPython.noop
        )
    ]
