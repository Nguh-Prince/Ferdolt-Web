from datetime import timedelta
import io
import json
import re
import urllib

from django.contrib.auth import login, logout
from django.contrib.auth.hashers import check_password
from django.contrib.auth.models import User
from django.db.models import Count, F, Max, Sum
from django.db.models.functions import Coalesce
from django.http import FileResponse, HttpResponse
from django.shortcuts import render, redirect
from django.utils import timezone
from django.utils.translation import gettext as _

from reportlab.pdfgen import canvas

from ferdolt import models as ferdolt_models
from flux import models as flux_models
from groups import models as groups_models

from core.functions import ( get_column_datatype, get_database_connection, 
sql_server_regex, postgresql_regex )

def get_file_size_and_unit(size, number_of_decimal_places=2):
    return_size = size
    unit = 'bytes'
    conversion_rate = 1

    if size >= 1024 ** 3:
        conversion_rate = 1024 ** 3
        return_size = round(size / 1024**3, number_of_decimal_places)
        unit = 'Gb'
    elif size >= 1024 ** 2:
        conversion_rate = 1024 ** 2
        return_size = round(size / 1024**2, number_of_decimal_places)
        unit = 'Mb'
    elif size >= 1024:
        conversion_rate = 1024
        return_size = round(size / 1024, number_of_decimal_places)
        unit = 'Kb'
    
    return (return_size, unit, conversion_rate)

################################################################################################
# Views
################################################################################################

def login_view(request):
    if request.method == "POST":
        username = request.POST["username"]
        password = request.POST["password"]
        
        try:
            user = User.objects.get(username=username)
        except Exception:
            context = {
                "errrors": [
                    {
                        "message": _("Username or password incorrect"),
                        "time": timezone.now(),
                    }
                ]
            }
            return render(request, "frontend/login.html", context=context)

        if not check_password(password, user.password):
            return render(
                request,
                "frontend/login.html",
                context={
                    "errors": [
                        {
                            "message": _("Username or password incorrect"),
                            "time": timezone.now(),
                        }
                    ]
                },
            )

        if not user.is_active:
            return render(
                request,
                "frontend/login.html",
                context={
                    "errors": [
                        {
                            "message": _("User account has been deactivated"),
                            "time": timezone.now(),
                        }
                    ]
                },
            )

        login(request, user)
        # to prevent javascript from character escaping the timezone
        tz = urllib.parse.quote_plus("Africa/Douala")

        response = redirect("frontend:index")

        return response
    else:
        return render(request, "frontend/login.html")


def index(request):
    now = timezone.now()
    # adding this delta to the current time will give us midnight of today
    delta = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)

    databases = (
        ferdolt_models.Database.objects.all()
        .order_by('-time_added').annotate(Count('id'))
    ).annotate(
        synchronization_count=Count(flux_models.ExtractionTargetDatabase.objects.filter(
            database__id=F("id")
        ).values("id"))
    )

    database_count = databases.count()
    
    today_databases = databases.filter( time_added__gte=now-delta )
    today_database_count = today_databases.count()

    extractions = flux_models.Extraction.objects.all()
    extraction_files = flux_models.File.objects.filter( 
        id__in=extractions.values("file__id")
    )

    extraction_count = extractions.count()
    data_extracted = extraction_files.values("size").aggregate(data_extracted=Coalesce(Sum('size'), 0.0))

    today_extractions = extractions.filter( time_made__gte=now-delta )
    data_extracted_today = (extraction_files.filter(id__in=today_extractions.values("file__id"))
        .values("size")
        .aggregate(data_extracted=Coalesce(Sum('size'), 0.0))
    )

    synchronizations = flux_models.Synchronization.objects.all()
    synchronization_count = synchronizations.count()

    today_synchronizations = synchronizations.filter( time_received__gte=now-delta )

    database_names = [ f.name for f in databases ]
    max_total_extraction_size = (extractions.values("extractionsourcedatabase__database__id")
        .annotate(size=Sum("file__size")).aggregate(Max("size"))
    )['size__max']

    data = {
        'databases': database_names,
        'number_of_extractions': [],
        'data_extracted': [],
        'number_of_synchronizations': [],
        'unit': 'bytes'
    }
    
    unit_size = get_file_size_and_unit(max_total_extraction_size)
    data['unit'] = unit_size[1]

    for database in databases:
        total_extraction_size = database.extractionsourcedatabase_set.aggregate( data_extracted=Coalesce( Sum( "extraction__file__size" ), 0.0 ) )

        data['number_of_extractions'].append( database.extractionsourcedatabase_set.count() )
        data['number_of_synchronizations'].append( database.synchronizationdatabase_set.count() )
        data['data_extracted'].append( round(total_extraction_size['data_extracted'] / unit_size[2], 2) )

    unit_and_size = get_file_size_and_unit( data_extracted['data_extracted'])
    data_extracted_today_unit_and_size = get_file_size_and_unit( 
        data_extracted_today['data_extracted'] 
    )

    context={
        'databases': databases[:5],
        'database_count': database_count,
        'today_database_count': today_database_count,
        'extraction_count': extraction_count,
        'today_extraction_count': today_extractions.count(),
        'synchronization_count': synchronization_count,
        'today_synchronization_count':today_synchronizations.count(),
        'data_extracted': {
            'size': unit_and_size[0],
            'unit': unit_and_size[1]
        },
        'data_extracted_today': {
            'size': data_extracted_today_unit_and_size[0],
            'unit': data_extracted_today_unit_and_size[1]
        },
        'chartInfo': json.dumps(data)
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
    databases = ferdolt_models.Database.objects.all()

    context = {'database': database, 
    'databases': databases}

    if id:
        query = databases.filter(id=id)
        if not query.exists():
            return redirect("frontend:not_found")
        else:
            # try to connect to the database
            database: ferdolt_models.Database = query.first()
            pending_synchronizations = flux_models.ExtractionTargetDatabase.objects.filter(
                database=database, is_applied=False
            )
            connection = get_database_connection(database)

            context['database'] = database
            context['pending_synchronizations'] = pending_synchronizations
            context['connection'] = connection

            databases = databases.filter( dbms_version=database.dbms_version )

    return render(request, "frontend/databases.html", context=context)

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

def groups(request, id: int=None):
    return render(request, "frontend/groups.html", context={'groups': groups_models.Group.objects.all()})

def extractions(request):
    return render(request, "frontend/extractions.html", context={'extractions': flux_models.Extraction.objects.all(), 'databases': ferdolt_models.Database.objects.all()})

def synchronizations(request):
    return render(request, "frontend/synchronizations.html", context={'synchronizations': flux_models.Synchronization.objects.all()})