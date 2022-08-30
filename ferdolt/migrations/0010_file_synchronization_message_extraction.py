# Generated by Django 4.1 on 2022-08-24 13:17

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0009_server_address_server_port_alter_server_location'),
    ]

    operations = [
        migrations.CreateModel(
            name='File',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file', models.FileField(upload_to='')),
                ('size', models.FloatField(null=True)),
                ('is_deleted', models.BooleanField()),
                ('last_modified_time', models.DateTimeField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Synchronization',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_received', models.DateTimeField(auto_now_add=True)),
                ('time_applied', models.DateTimeField()),
                ('is_applied', models.BooleanField(default=False)),
                ('file', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='ferdolt.file')),
                ('source', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='ferdolt.server')),
            ],
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_sent', models.DateTimeField(auto_now_add=True)),
                ('file', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ferdolt.file')),
                ('recipient', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ferdolt.server')),
            ],
        ),
        migrations.CreateModel(
            name='Extraction',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_made', models.DateTimeField(auto_now=True)),
                ('file', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='ferdolt.file')),
            ],
        ),
    ]