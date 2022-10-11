from huey.contrib.djhuey import task

from core import functions as core_functions

from django.db import transaction

from . import models

@task()
def initialize_database(database_id):
    database = models.Database.objects.filter(id=database_id).first()

    if database:
        with transaction.atomic():
            core_functions.initialize_database(database)

            database.is_initialized = True
            database.save()

