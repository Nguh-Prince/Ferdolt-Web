from huey import crontab
from huey.contrib.djhuey import periodic_task, task

@task()
def count_beans(number):
    print("-- counted %s beans -- " % number)
    return 'Counted %s beans' % number

@periodic_task(crontab(minute='*/1'))
def every_minute():
    print('Every 1 minute, this will be printed by the consumer')