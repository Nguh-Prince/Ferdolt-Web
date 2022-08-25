from django.db import models
from django.utils.translation import gettext as _
from django.core.exceptions import ValidationError

from ferdolt import models as ferdolt_models

class GroupDatabase(models.Model):
    name = models.CharField(max_length=150)
    database = models.ForeignKey(ferdolt_models.Database, on_delete=models.PROTECT)
    is_writeable = models.BooleanField(default=True) # set this to False if this server does not create data related to this group
    
    is_readable = models.BooleanField(default=True) # set this to False if this server does not integrate data from this group

    def clean(self) -> None:
        if not self.is_writeable and not self.is_readable:
            raise ValidationError(
             _("The group must be writeable or readable") 
            )
        return super().clean()

    class Meta:
        verbose_name = _("Group database")
        verbose_name_plural = _("Group databases")

class GroupTable(models.Model):
    name = models.CharField(max_length=100)
    table = models.ForeignKey( ferdolt_models.Table, on_delete=models.PROTECT )

    class Meta:
        verbose_name = _("Group table")
        verbose_name_plural = _("Group tables")

class GroupColumn(models.Model):
    name = models.CharField(max_length=150)
    group_table = models.ForeignKey(GroupTable, on_delete=models.CASCADE)
    column = models.ForeignKey(ferdolt_models.Column, on_delete=models.PROTECT)
    is_required = models.BooleanField(default=False)