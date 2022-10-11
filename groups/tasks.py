from huey import crontab
from huey.contrib.djhuey import periodic_task, task

from groups import functions

from . import models

@periodic_task(crontab(minute='*/5'))
def extract_from_groups():
    for group_database in models.GroupDatabase.objects.all():
        functions.extract_from_groupdatabase(group_database)
        pass
    pass

@periodic_task(crontab(minute='*/10'))
def synchronize_groups():
    for group in models.Group.objects.all():
        functions.synchronize_group(group)