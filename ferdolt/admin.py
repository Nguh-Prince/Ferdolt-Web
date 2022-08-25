from django.contrib import admin

from . import models

admin.site.register(models.Column)
admin.site.register(models.ColumnConstraint)
admin.site.register(models.Database)
admin.site.register(models.DatabaseManagementSystem)
admin.site.register(models.DatabaseManagementSystemVersion)
admin.site.register(models.DatabaseSchema)
admin.site.register(models.Server)
admin.site.register(models.Table)