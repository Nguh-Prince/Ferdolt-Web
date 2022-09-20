# Generated by Django 4.1 on 2022-09-03 20:30

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('flux', '0006_encrypt_previously_generated_files'),
        ('groups', '0006_alter_grouptable_group'),
    ]

    operations = [
        migrations.AddField(
            model_name='group',
            name='slug',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='groupcolumn',
            name='group_table',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='columns', to='groups.grouptable'),
        ),
        migrations.CreateModel(
            name='GroupExtraction',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('extraction', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='flux.extraction')),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='groups.group')),
                ('source_database', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='groups.groupdatabase')),
            ],
        ),
    ]
