from django.db import migrations

def create_default_server_and_set_databases_to_default_server(apps, schema_editor):
    Database = apps.get_model('ferdolt', 'Database')
    Server = apps.get_model('ferdolt', 'Server')

    if not Server.objects.filter(name='default').exists():
        server = Server.objects.create(name='default', address='localhost')

        # set the server of the databases with no server to the default server
        Database.objects.filter(server__isnull=True).update(server=server)

class Migration(migrations.Migration):

    dependencies = [
        ('ferdolt', '0003_database_server_historicaldatabase_server_and_more'),
    ]

    operations = [
        migrations.RunPython(
            create_default_server_and_set_databases_to_default_server, 
            reverse_code=migrations.RunPython.noop
        )
    ]
