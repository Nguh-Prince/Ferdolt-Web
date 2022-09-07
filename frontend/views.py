from datetime import timedelta
import io
import re

from django.db.models import Count, Sum
from django.http import FileResponse, HttpResponse
from django.shortcuts import render, redirect
from django.utils import timezone
from django.utils.translation import gettext as _

from reportlab.pdfgen import canvas

from ferdolt import models as ferdolt_models
from flux import models as flux_models

from core.functions import get_column_datatype, get_database_connection, sql_server_regex, postgresql_regex

################################################################################################
# Views
################################################################################################

def index(request):
    now = timezone.now()
    # adding this delta to the current time will give us midnight of today
    delta = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)

    databases = (
        ferdolt_models.Database.objects.all()
        .order_by('-time_added').annotate(Count('id'))
    )
    database_count = databases.count()
    
    today_databases = databases.filter( time_added__gte=now-delta )
    today_database_count = today_databases.count()

    extractions = flux_models.Extraction.objects.all()
    extraction_count = extractions.count()
    data_extracted = flux_models.File.objects.filter( 
        id__in=extractions.values("file__id")
    ).values("size").aggregate(data_extracted=Sum('size'))
    
    size = data_extracted['data_extracted']
    unit = None

    if size >= 1024 ** 3:
        data_extracted['data_extracted'] = round(size / 1024**3, 2)
        unit = 'Gb'
    elif size >= 1024 ** 2:
        data_extracted['data_extracted'] = round(size / 1024**2, 2)
        unit = 'Mb'
    elif size >= 1024:
        data_extracted['data_extracted'] = round(size / 1024, 2)
        unit = 'Kb'
    else:
        unit = 'bytes'

    today_extractions = extractions.filter( time_made__gte=now-delta )

    synchronizations = flux_models.Synchronization.objects.all()
    synchronization_count = synchronizations.count()

    today_synchronizations = synchronizations.filter( time_received__gte=now-delta )

    context={
        'databases': databases[:5],
        'database_count': database_count,
        'today_database_count': today_database_count,
        'extraction_count': extraction_count,
        'today_extraction_count': today_extractions.count(),
        'synchronization_count': synchronization_count,
        'today_synchronization_count':today_synchronizations.count(),
        'data_extracted': {
            'size': data_extracted['data_extracted'],
            'unit': unit
        }
    }

    return render(request, "frontend/index.html", context=context)

def pdf(request):
    buffer = io.BytesIO()

    p = canvas.Canvas(buffer)

    p.drawString(100, 100, "Hello world.")

    p.showPage()
    p.save()

    return FileResponse( buffer, as_attachment=True, filename='hello.pdf' )

def databases(request, id: int=None):
    database = None
    
    if id:
        query = ferdolt_models.Database.objects.filter(id=id)
        if not query.exists():
            return redirect("frontend:not_found")
        else:
            # try to connect to the database
            database: ferdolt_models.Database = query.first()

            is_postgres_db, is_sqlserver_db, is_mysql_db = False, False, False

            # if no schemas have been added to the database's record, add the tables and schema for that record
            if database.databaseschema_set.count() < 1:
                connection = get_database_connection(database)

                if connection:
                    cursor = connection.cursor()
                    
                    dbms_name = database.dbms_version.dbms.name

                    if sql_server_regex.search(dbms_name):
                        query = """
                            SELECT T.TABLE_NAME, T.TABLE_SCHEMA, C.COLUMN_NAME, C.DATA_TYPE, 
                            C.CHARACTER_MAXIMUM_LENGTH, C.DATETIME_PRECISION, C.NUMERIC_PRECISION, C.IS_NULLABLE, TC.CONSTRAINT_TYPE 
                            FROM INFORMATION_SCHEMA.TABLES T LEFT JOIN 
                            INFORMATION_SCHEMA.COLUMNS C ON C.TABLE_NAME = T.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA 
                            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
                            CU.COLUMN_NAME = C.COLUMN_NAME AND CU.TABLE_NAME = C.TABLE_NAME AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA LEFT JOIN 
                            INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME 
                            WHERE T.TABLE_TYPE = 'BASE TABLE' ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME
                        """
                        is_sqlserver_db = True
                    
                    if postgresql_regex.search(dbms_name):
                        query = """
                            SELECT T.TABLE_NAME, T.TABLE_SCHEMA, C.COLUMN_NAME, C.DATA_TYPE, 
                            C.CHARACTER_MAXIMUM_LENGTH, C.DATETIME_PRECISION, C.NUMERIC_PRECISION, C.IS_NULLABLE, TC.CONSTRAINT_TYPE 
                            FROM INFORMATION_SCHEMA.TABLES T LEFT JOIN 
                            INFORMATION_SCHEMA.COLUMNS C ON C.TABLE_NAME = T.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA 
                            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CU ON 
                            CU.COLUMN_NAME = C.COLUMN_NAME AND CU.TABLE_NAME = C.TABLE_NAME AND CU.TABLE_SCHEMA = C.TABLE_SCHEMA LEFT JOIN 
                            INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME 
                            WHERE T.TABLE_TYPE = 'BASE TABLE' AND NOT T.TABLE_SCHEMA = 'pg_catalog' AND NOT T.TABLE_SCHEMA = 'information_schema' 
                            ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME
                        """
                        is_postgres_db = True

                    if query:
                        results = cursor.execute(query)
                        columns = [ column[0].lower() for column in cursor.description ]

                        dictionary = {}

                        # get the database structure and store in a dictionary
                        # I did this so that we don't have to continuously query the database for the tables and schemas
                        for row in results:
                            row_dictionary = dict( zip(columns, row) )      
                            schema_dictionary = dictionary.setdefault(
                            row_dictionary['table_schema'], {})
                            table_dictionary = schema_dictionary.setdefault(
                            row_dictionary['table_name'], {})

                            column_dictionary = table_dictionary.setdefault(
                            row_dictionary['column_name'], {})

                            if not column_dictionary:
                                column_dictionary = {
                                    'data_type': get_column_datatype(is_postgres=is_postgres_db, is_sqlserver=is_sqlserver_db, 
                                        is_mysql=is_mysql_db, data_type_string=row_dictionary['data_type']),
                                    'character_maximum_length': row_dictionary['character_maximum_length'],
                                    'datetime_precision': row_dictionary['datetime_precision'],
                                    'numeric_precision': row_dictionary['numeric_precision'],
                                    'constraint_type': set([row_dictionary['constraint_type']]),
                                    'is_nullable': row_dictionary['is_nullable'].lower() == 'yes'
                                }
                            else:
                                column_dictionary['constraint_type'].add( row_dictionary['constraint_type'] )

                            table_dictionary[row_dictionary['column_name']] = column_dictionary

                        for schema in dictionary.keys():
                            schema_record = ferdolt_models.DatabaseSchema.objects.get_or_create(name=schema.lower(), database=database)[0]
                            schema_dictionary = dictionary[schema]

                            for table in schema_dictionary.keys():
                                table_record = ferdolt_models.Table.objects.get_or_create(schema=schema_record, name=table.lower())[0]
                                table_dictionary = schema_dictionary[table]

                                for column in table_dictionary.keys():
                                    column_dictionary = table_dictionary[column]

                                    column_record = ferdolt_models.Column.objects.get_or_create(table=table_record, name=column, data_type=column_dictionary['data_type'], datetime_precision=column_dictionary['datetime_precision'], character_maximum_length=column_dictionary['character_maximum_length'], numeric_precision=column_dictionary['numeric_precision'], is_nullable=column_dictionary['is_nullable'])[0]

                                    column_record.columnconstraint_set.all().delete()

                                    for constraint in column_dictionary['constraint_type']:
                                        primary_key_regex = re.compile("primary key", re.I)
                                        foreign_key_regex = re.compile("foreign key", re.I)
                                        
                                        if constraint:
                                            if primary_key_regex.search(constraint):
                                                ferdolt_models.ColumnConstraint.objects.create(column=column_record, is_primary_key=True)
                                            
                                            if foreign_key_regex.search(constraint):
                                                ferdolt_models.ColumnConstraint.objects.create(column=column_record, is_foreign_key=True)

                else:
                    return HttpResponse(f"Error connecting to {database.__str__()} database")
    return render(request, "frontend/databases.html", context={'database': database})

def not_found(request):
    return render(request, "frontend/auth-404.html")

def schemas(request):
    return render(request, "frontend/schemas.html")

def tables(request, id: int=None):
    context = {}
    if not id:
        tables = ferdolt_models.Table.objects.all()
        context['tables'] = tables
    else:
        query = ferdolt_models.Table.objects.filter(id=id)

        if query.exists():
            context['table'] = query.first()
            table: ferdolt_models.Table = query.first()
            connection = get_database_connection(table.schema.database)
                
            return render( request, "frontend/tables.html", context={ 'table': table } )

    return render(request, "frontend/tables.html", context=context)

def columns(request):
    return render(request, "frontend/columns.html")

def file_manager(request):
    return render(request, "frontend/file_manager.html")

def servers(request, id: int=None):
    if not id:
        servers = ferdolt_models.Server.objects.all()

        return render(request, "frontend/servers.html", context={'servers': servers})
    
    else:
        query = ferdolt_models.Server.objects.filter(id=id)

        if not query.exists():
            return redirect("frontend:not_found")
        
        server = query.first()

        return render(request, "frontend/servers.html", context={'server': server})

def extractions(request):
    return render(request, "frontend/extractions.html", context={'extractions': flux_models.Extraction.objects.all()})

def synchronizations(request):
    return render(request, "frontend/synchronizations.html", context={'synchronizations': flux_models.Synchronization.objects.all()})