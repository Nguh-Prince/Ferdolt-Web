import logging
from huey import crontab
from huey.contrib.djhuey import periodic_task, task
from core.functions import get_database_connection

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

@task()
def create_missing_tables_and_columns_in_group_databases(group_id):
    group = models.Group.objects.filter( id=group_id )

    # looping through the databases that can synchronize with this group
    for group_database in group.groupdatabase_set.filter(can_read=True):
        connection = get_database_connection(group_database.database)

        if connection:
            cursor = connection.cursor()

            for table in group.tables.all():
                group_table_table_query = models.GroupTableTable.objects.filter( group_table=table, table__database=group_database.database )
                if not group_table_table_query.exists():
                    # create the table in that database with the columns and data types
                    query = f"""
                    CREATE TABLE {table.name} ()
                    """