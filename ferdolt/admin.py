from django.contrib import admin

from simple_history.admin import SimpleHistoryAdmin

from . import models

admin.site.register(models.Column, SimpleHistoryAdmin)
admin.site.register(models.ColumnConstraint, SimpleHistoryAdmin)
admin.site.register(models.Database, SimpleHistoryAdmin)
admin.site.register(models.DatabaseManagementSystem, SimpleHistoryAdmin)
admin.site.register(models.DatabaseManagementSystemVersion, SimpleHistoryAdmin)
admin.site.register(models.DatabaseSchema, SimpleHistoryAdmin)
admin.site.register(models.Server, SimpleHistoryAdmin)
admin.site.register(models.Table, SimpleHistoryAdmin)