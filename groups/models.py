from email.policy import default
import logging
from random import choices
import string

from cryptography import fernet

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _

from slugify import slugify

from core.functions import encrypt, decrypt

from ferdolt import models as ferdolt_models
from flux.models import Extraction, Synchronization

from huey.contrib import djhuey as huey

def generate_unique_fernet_key():
    while True:
        generated_key_is_unique = True

        key = fernet.Fernet.generate_key()

        for group in Group.objects.all():
            try:
                group_key = decrypt(group.fernet_key)[0]

                if group_key == key:
                    generated_key_is_unique = False

            except fernet.InvalidToken as e:
                logging.info(f"[In groups.models.generate_key] fernet.InvalidToken error raised when decrypting the key for the {group.__str__()} group")

        if generated_key_is_unique:
            return encrypt(key)[1]
    
def generate_fernet_key():
    return encrypt( fernet.Fernet.generate_key() )[1]

def generate_unique_group_name(length=15):
    print("Calling generate_unique_group_name function")
    while True:
        name = choices( string.ascii_lowercase + string.digits, k=length )

        if not Group.objects.filter(name=name).exists():
            return ''.join(name)

class Group(models.Model):
    name = models.CharField(unique=True, max_length=50, default=generate_unique_group_name)
    slug = models.CharField(max_length=50, null=False)
    fernet_key = models.TextField(unique=True, null=True, default=generate_unique_fernet_key)
    # set this to True if objects that are missing in group member's databases should be created
    # e.g. a group member is lacking a column or table
    create_missing_objects_from_sources = models.BooleanField(default=False) 
    is_active = models.BooleanField(default=True)

    @property
    def databases(self):
        return ferdolt_models.Database.objects.filter(id__in=self.groupdatabase_set.values("database__id"))

    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        self.slug = slugify(self.name)
        return super().save(*args, **kwargs)

    def get_fernet_key(self):
        return decrypt(self.fernet_key)[1]

DEFAULT_EXTRACTION_FREQUENCY = 1
DEFAULT_SYNCHRONIZATION_FREQUENCY = 1

class GroupServer(models.Model):
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    server = models.ForeignKey(ferdolt_models.Server, on_delete=models.CASCADE, null=True)

class GroupDatabase(models.Model):
    database = models.ForeignKey(ferdolt_models.Database, on_delete=models.CASCADE, null=True)
    can_write = models.BooleanField(default=True) # set this to False if this server does not create data related to this group
    can_read = models.BooleanField(default=True) # set this to False if this server does not integrate data from this group
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    extraction_frequency = models.BigIntegerField(null=True) # how often data should be extracted from this database in minutes 
    synchronization_frequency = models.BigIntegerField(null=True) # how often this database should be synchronized with data from the group in minutes

    def clean(self) -> None:
        if not self.can_write and not self.can_read:
            raise ValidationError(
             _("The group must be writeable or readable") 
            )
        return super().clean()

    def save(self, *args, **kwargs) -> None:
        if self.can_write and not self.extraction_frequency:
            self.extraction_frequency = 1
        
        if self.can_read and not self.synchronization_frequency:
            self.synchronization_frequency = 1

        if 'force_insert' in kwargs:
            super().save(*args, **kwargs)
            # create GroupDatabaseSynchronizations for every GroupExtraction of this group
            for extraction in self.groupextraction_set.filter(~Q(groupdatabasesynchronization__group_database=self)):
                GroupDatabaseSynchronization.objects.create(group_database=self, extraction=extraction)

    class Meta:
        verbose_name = _("Group database")
        verbose_name_plural = _("Group databases")

        unique_together = ["database", "group"]

class GroupTable(models.Model):
    name = models.CharField(max_length=100)
    group = models.ForeignKey(Group, on_delete=models.CASCADE, related_name='tables')

    class Meta:
        verbose_name = _("Group table")
        verbose_name_plural = _("Group tables")
        unique_together = [
            ["group", "name"]
        ]

    @property
    def column_count(self) -> int:
        return self.groupcolumn_set.count()

    def save(self, *args, **kwargs):
        self.name = self.name.lower()

        super().save(*args, **kwargs)

class GroupTableTable(models.Model):
    group_table = models.ForeignKey(GroupTable, on_delete=models.CASCADE)
    table = models.ForeignKey( ferdolt_models.Table, on_delete=models.CASCADE )

    class Meta:
        unique_together = [
            ["group_table", "table"]
        ]

class GroupColumn(models.Model):
    data_types = (
        ("datetime", _("Datetime")),
        ("date", _("Date")),
        ("int", _("Integer")),
        ("float", _("Float")),
        ("double", _("Double")),
        ("char", _("Character"))
    )

    name = models.CharField(max_length=150)
    group_table = models.ForeignKey(GroupTable, on_delete=models.CASCADE, related_name='columns')
    is_required = models.BooleanField(default=False)
    data_type = models.CharField(max_length=50, choices=data_types)
    is_nullable = models.BooleanField(default=True)
    character_maximum_length = models.IntegerField(null=True, blank=True)

    class Meta:
        unique_together = [
            ["group_table", "name"]
        ]

    def save(self, *args, **kwargs):
        self.name = self.name.lower()
        super().save(*args, **kwargs)

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
    source_server = models.ForeignKey(GroupServer, on_delete=models.SET_NULL, null=True)

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

class GroupServerSynchronization(models.Model):
    group_server = models.ForeignKey(GroupServer, on_delete=models.CASCADE)
    extraction = models.ForeignKey(Group, on_delete=models.CASCADE)
    is_applied = models.BooleanField(default=False)
    time_applied = models.DateTimeField(null=True)
