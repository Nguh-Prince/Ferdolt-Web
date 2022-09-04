# Generated by Django 4.1 on 2022-09-03 20:30

from django.db import migrations

from slugify import slugify

def set_group_slugs(apps, schema_editor):
    Group = apps.get_model("groups", "Group")

    for group in Group.objects.all():
        group.slug = slugify( group.name, separator='_' )
        group.save()

class Migration(migrations.Migration):

    dependencies = [
        ('groups', '0007_group_slug_alter_groupcolumn_group_table_and_more'),
    ]

    operations = [
        migrations.RunPython( set_group_slugs, reverse_code=migrations.RunPython.noop )
    ]