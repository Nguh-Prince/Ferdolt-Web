<<<<<<< HEAD
# Generated by Django 4.1 on 2022-09-15 17:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0016_historicalsynchronization_historicalmessage_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='extraction',
            name='end_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='historicalextraction',
            name='end_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.CreateModel(
            name='ExtractionSynchronizationErrors',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_recorded', models.DateTimeField(auto_now_add=True)),
                ('error_message', models.TextField()),
                ('target', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='flux.extractiontargetdatabase')),
            ],
        ),
    ]
=======
# Generated by Django 4.1 on 2022-09-15 17:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0016_historicalsynchronization_historicalmessage_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='extraction',
            name='end_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='historicalextraction',
            name='end_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.CreateModel(
            name='ExtractionSynchronizationErrors',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_recorded', models.DateTimeField(auto_now_add=True)),
                ('error_message', models.TextField()),
                ('target', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='flux.extractiontargetdatabase')),
            ],
        ),
    ]
>>>>>>> 52e1a53dc5de9b95f1f3b424b2c812b1237a5c0d
