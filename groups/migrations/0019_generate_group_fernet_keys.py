import logging
from cryptography.fernet import Fernet

from django.db import migrations

from groups.models import generate_unique_fernet_key

def generate_group_fernet_keys(apps, schema_editor):
    Group = apps.get_model("groups", "Group")

    for group in Group.objects.all():
        logging.info(f"Generating the fernet key for the {group.__str__()} group")

        key = generate_unique_fernet_key()

        group.fernet_key = key
        group.save()

class Migration(migrations.Migration):

    dependencies = [
        ('groups', '0018_group_fernet_key'),
    ]

    operations = [
        migrations.RunPython(generate_group_fernet_keys, 
        reverse_code=migrations.RunPython.noop)
    ]
