import logging
from os import unlink
from os.path import basename, dirname, exists, getsize, join
import re
import zipfile

from django.core.files import File as DjangoFile
from django.db import migrations

def zip_existing_files(apps, schema_editor):
    File = apps.get_model("flux", "File")
    file_extension_regex = re.compile("(.*)(\.(.*))$", re.I)

    for file in File.objects.all():
        logging.info(f"Zipping the file found at {file.file.path}")
        print(f"Zipping the file found at {file.file.path}")

        file_path = file.file.path
        file_basename = basename(file_path)

        file_name = file_extension_regex.search(basename(file_path)).group(1)
        dir_name = dirname(file_path)

        if exists(file_path):
            # create a zip file
            zip_filename = join( dir_name, file_name + ".zip" )
            
            with zipfile.ZipFile( zip_filename, mode='a' ) as archive:
                logging.info(f"Creating zip file {zip_filename}")
                print(f"Creating zip file {zip_filename}")
                archive.write(file_path, file_basename)

            with open( zip_filename, "rb" ) as __:
                old_file = file.file
                file.file = DjangoFile( __, name=basename(zip_filename))
                file.size = getsize(zip_filename)

                if old_file.path != zip_filename:
                    print("Deleting old_file...")
                    # unlink(old_file.path)
                
                file.save()
        
        else:
            file.is_deleted = True
            file.save()
            logging.info(f"The {file_path} file does not exist")


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0014_extractiontargetdatabase_time_applied'),
    ]

    operations = [
        migrations.RunPython(zip_existing_files, reverse_code=migrations.RunPython.noop)
    ]
