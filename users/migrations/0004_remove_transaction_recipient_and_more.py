# Generated by Django 4.1 on 2022-09-09 10:06

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0003_alter_person_phone_wallet_transaction'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='transaction',
            name='recipient',
        ),
        migrations.RemoveField(
            model_name='transaction',
            name='sender',
        ),
        migrations.RemoveField(
            model_name='wallet',
            name='user',
        ),
        migrations.DeleteModel(
            name='Person',
        ),
        migrations.DeleteModel(
            name='Transaction',
        ),
        migrations.DeleteModel(
            name='Wallet',
        ),
    ]
