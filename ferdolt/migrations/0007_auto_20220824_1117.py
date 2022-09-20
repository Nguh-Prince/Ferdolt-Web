from django.db import migrations

from ferdolt.models import generate_server_id

def generate_server_server_id(apps, schema_editor):
    Server = apps.get_model("ferdolt", "Server")

    for row in Server.objects.all():
        row.server_id = generate_server_id()
        row.save(update_fields=['server_id'])

class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0006_server_server_id'),
    ]

    operations = [
        migrations.RunPython( generate_server_server_id, reverse_code=migrations.RunPython.noop ),
    ]
