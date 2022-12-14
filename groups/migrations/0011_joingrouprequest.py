# Generated by Django 4.1.3 on 2022-11-14 15:31

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0008_createserverrequest'),
        ('groups', '0010_groupserver_can_read_groupserver_can_write'),
    ]

    operations = [
        migrations.CreateModel(
            name='JoinGroupRequest',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_made', models.DateTimeField()),
                ('is_accepted', models.BooleanField(null=True)),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='groups.group')),
                ('source_server', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='joingrouprequests', to='ferdolt.server')),
                ('target_server', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ferdolt.server')),
            ],
        ),
    ]
