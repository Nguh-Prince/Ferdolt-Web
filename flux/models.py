from django.db import models

from ferdolt import models as ferdolt_models

class File(models.Model):
    file = models.FileField(upload_to="extractions")
    size = models.FloatField(null=True)
    is_deleted = models.BooleanField(default=False)
    last_modified_time = models.DateTimeField(null=True)

class Extraction(models.Model):
    time_made = models.DateTimeField()
    file = models.ForeignKey(File, on_delete=models.PROTECT)
     
    # this time is used to query the target database 
    # i.e. SELECT * FROM table WHERE last_updated > start_time
    start_time = models.DateTimeField(null=True)

class ExtractionDatabase(models.Model):
    extraction = models.ForeignKey(Extraction, on_delete=models.CASCADE)
    database = models.ForeignKey(ferdolt_models.Database, on_delete=models.CASCADE)

class Synchronization(models.Model):
    time_received = models.DateTimeField(auto_now_add=True)
    time_applied = models.DateTimeField()
    source = models.ForeignKey(ferdolt_models.Server, on_delete=models.SET_NULL, null=True)
    is_applied = models.BooleanField(default=False)
    file = models.ForeignKey(File, on_delete=models.PROTECT)

class SynchronizationDatabase(models.Model):
    synchronization = models.ForeignKey(Synchronization, on_delete=models.CASCADE)
    database = models.ForeignKey(ferdolt_models.Database, on_delete=models.CASCADE)

class Message(models.Model):
    time_sent = models.DateTimeField(auto_now_add=True)
    recipient = models.ForeignKey(ferdolt_models.Server, on_delete=models.CASCADE)
    file = models.ForeignKey(File, on_delete=models.CASCADE)