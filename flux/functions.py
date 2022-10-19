from os import path

from . import models

from common.functions import hash_file

def set_hashes_from_files_in_queryset(queryset):
    for file in queryset:
        file_path = file.file.path

        if path.exists(file_path):
            hash = hash_file(file_path)
            file.hash = hash
            file.save()   

def set_file_hashes():
    queryset = all_files = models.File.objects.all()
    set_hashes_from_files_in_queryset(queryset)