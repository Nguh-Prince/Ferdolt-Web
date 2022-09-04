from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _

from slugify import slugify

from ferdolt import models as ferdolt_models
from flux.models import Extraction, Synchronization

class Group(models.Model):
    name = models.CharField(unique=True, max_length=50)
    slug = models.CharField(max_length=50, null=False)

    @property
    def databases(self):
        return ferdolt_models.Database.objects.filter(id__in=self.groupdatabase_set.values("database__id"))

    def save(self, *args, **kwargs):
        self.slug = slugify(self.name)

        return super().save(*args, **kwargs)

class GroupDatabase(models.Model):
    database = models.ForeignKey(ferdolt_models.Database, on_delete=models.CASCADE, null=True)
    is_writeable = models.BooleanField(default=True) # set this to False if this server does not create data related to this group
    is_readable = models.BooleanField(default=True) # set this to False if this server does not integrate data from this group
    group = models.ForeignKey(Group, on_delete=models.CASCADE)

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
    group = models.ForeignKey(Group, on_delete=models.CASCADE, related_name='tables')

    class Meta:
        verbose_name = _("Group table")
        verbose_name_plural = _("Group tables")

    @property
    def column_count(self) -> int:
        return self.groupcolumn_set.count()

class GroupColumn(models.Model):
    name = models.CharField(max_length=150)
    group_table = models.ForeignKey(GroupTable, on_delete=models.CASCADE, related_name='columns')
    is_required = models.BooleanField(default=False)
    data_type = models.CharField(max_length=50)
    is_nullable = models.BooleanField(default=True)

class GroupColumnColumn(models.Model):
    group_column = models.ForeignKey(GroupColumn, on_delete=models.CASCADE)
    column = models.ForeignKey( ferdolt_models.Column, on_delete=models.CASCADE )

class GroupColumnConstraint(models.Model):
    column = models.ForeignKey(GroupColumn, on_delete=models.CASCADE)
    is_unique = models.BooleanField(default=False)
    is_foreign_key = models.BooleanField(default=False)
    is_primary_key = models.BooleanField(default=False)
    references = models.ForeignKey(GroupColumn, on_delete=models.SET_NULL, 
    null=True, blank=True, related_name='references')

class GroupExtraction(models.Model):
    extraction = models.ForeignKey(Extraction, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    source_database = models.ForeignKey(GroupDatabase, on_delete=models.SET_NULL, null=True)

class GroupDatabaseSynchronization(models.Model):
    group_database = models.ForeignKey(GroupDatabase, on_delete=models.CASCADE)
    extraction = models.ForeignKey( GroupExtraction, on_delete=models.CASCADE )
    is_applied = models.BooleanField( default=False )
    time_applied = models.DateTimeField( null=True )

    class Meta:
        unique_together = [
            ["group_database", "extraction"]
        ]

    def save(self, *args, **kwargs):
        if self.is_applied and not self.time_applied:
            self.time_applied = timezone.now()

        return super().save(*args, **kwargs)