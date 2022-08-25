from django.contrib import admin

from . import models

admin.site.register(models.Extraction)
admin.site.register(models.ExtractionDatabase)
admin.site.register(models.File)
admin.site.register(models.Message)
admin.site.register(models.Synchronization)