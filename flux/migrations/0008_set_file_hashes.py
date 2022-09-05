from hashlib import sha256
import logging

from django.db import migrations

def set_file_hashes(apps, schema_editor):
    File = apps.get_model("flux", "File")
    
    for file in File.objects.all():
        file_path = file.file.path

        try:
            with open( file_path ) as __:
                content = __.read()
                hash = sha256( content.encode('utf-8') ).hexdigest()

                file.hash = hash
                file.save()
                logging.info(f"Successfully set has for {file_path}")
        except FileNotFoundError:
            logging.error(f"Could not set hash for {file_path} cause it does not exist")

class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0007_file_hash'),
    ]

    operations = [
        migrations.RunPython( set_file_hashes, 
        reverse_code=migrations.RunPython.noop )
    ]
