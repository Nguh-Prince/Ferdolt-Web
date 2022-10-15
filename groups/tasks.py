from huey import crontab
from huey.contrib.djhuey import periodic_task, task

from groups import functions

from . import models

@periodic_task(crontab(minute='*/1'))
def extract_from_groups():
    for group_database in models.GroupDatabase.objects.all():
        functions.extract_from_groupdatabase(group_database)

@periodic_task(crontab(minute='*/2'))
def synchronize_groups():
    for group in models.Group.objects.all():
        functions.synchronize_group(group)