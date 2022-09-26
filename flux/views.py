import json
import logging
import zipfile
import psycopg
import pyodbc
import re

from cryptography.fernet import Fernet

from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _

from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework import permissions as drf_permissions
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from common.permissions import IsStaff

from flux import serializers

from . import models
from .serializers import SynchronizationSerializer

from core.functions import (decrypt, get_create_temporary_table_query, get_dbms_booleans, 
get_temporary_table_name, get_type_and_precision, get_column_dictionary, synchronize_database)

from ferdolt import models as ferdolt_models
from ferdolt_web.settings import FERNET_KEY
from frontend.views import get_database_connection

class FileViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]

    serializer_class = serializers.FileSerializer
    def get_queryset(self):
        return models.File.objects.all()

    def destroy(self, request, *args, **kwargs):
        return Response( data={"message": _("This method is not allowed")}, status=status.HTTP_401_UNAUTHORIZED )

    def update(self, request, *args, **kwargs):
        return Response( data={"message": _("This method is not allowed")}, status=status.HTTP_401_UNAUTHORIZED )

class ExtractionViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.ExtractionSerializer

    def get_queryset(self):
        return models.Extraction.objects.all()

    def destroy(self, request, *args, **kwargs):
        object = self.get_object()

        file: models.File = object.file

        if file:
            file.file.delete()
            file.delete()

        return super().destroy(request, *args, **kwargs)

    @action(
        methods=['GET'],
        detail=True,
    )
    def content(self, request, *args, **kwargs):
        object = self.get_object()

        f = Fernet(FERNET_KEY)
        file_path = object.file.file.path
        file = object.file

        try:
            content = None
            zip_file = zipfile.ZipFile(file_path)

            for file in zip_file.namelist():
                if file.endswith('.json'):
                    content = zip_file.read(file)
                    
                    if content:
                        content = decrypt(content)[1]
                        logging.debug("[In flux.views.SynchronizationViewSet.create] reading the unapplied synchronization file")

            zip_file.close()
                
            return Response( data={'content': json.loads(content), 
            'message': _('Content gotten successfully') if content else _('The extraction is empty')} )
        except FileNotFoundError as e:
            return Response(data={'message': _("The file was not found. It has either been deleted, moved or renamed")}, 
            status=status.HTTP_404_NOT_FOUND)

deletion_table_regex = re.compile("_deletion$")

class SynchronizationViewSet(viewsets.ModelViewSet):
    permission_classes = [ IsStaff ]
    serializer_class = serializers.SynchronizationSerializer

    def get_queryset(self):
        return models.Synchronization.objects.all()

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data

        logging.info("[In flux.views.SynchronizationViewSet.create]")

        f = Fernet(FERNET_KEY)

        if 'use_pentaho' not in validated_data or not validated_data['use_pentaho']:
            databases = validated_data.pop("synchronizationdatabase_set")

            synchronizations = []
            synchronized_databases = []

            for database in databases:
                database_record = ferdolt_models.Database.objects.filter(id=database['database']['id'])
                
                if database_record.exists():
                    database_record = database_record.first()
                    connection = get_database_connection(database_record)
                    
                    dbms_booleans = get_dbms_booleans(database_record)

                    flag = True

                    if connection:
                        cursor = connection.cursor()
                        
                        unapplied_extractions = database_record.extractiontargetdatabase_set.filter(is_applied=False)

                        time_applied = timezone.now()

                        temporary_tables_created = set([])

                        for extraction in unapplied_extractions:
                            file_path = extraction.extraction.file.file.path
                            file = extraction.extraction.file

                            try:
                                zip_file = zipfile.ZipFile(file_path)

                                for file in zip_file.namelist():
                                    if file.endswith('.json'):
                                        content = zip_file.read(file)
                                        content = f.decrypt( bytes(content, 'utf-8') )
                                        content = content.decode('utf-8')

                                        logging.debug("[In flux.views.SynchronizationViewSet.create] reading the unapplied synchronization file")
                                        
                                        try:
                                            dictionary: dict = json.loads(content)
                                            synchronize_database(connection, database, dictionary)

                                        except json.JSONDecodeError as e:
                                            logging.error( f"[In flux.views.SynchronizationViewSet.create]. Error parsing json from file for database synchronization. File path: {file_path}" )

                                zip_file.close()
                            except FileNotFoundError as e:
                                flag = False
                                logging.error( f"[In flux.views.SynchronizationViewSet.create]. Error opening file for database synchronization. File path: {file_path}" )

                    else:
                        raise ValidationError( _("Invalid connection parameters") )
                else:
                    raise ValidationError( _("No database exists with id %(id)s" % {'id': database['database']['id'] }) )

            if synchronized_databases:
                # record the synchronization
                synchronization = models.Synchronization.objects.create(time_applied=timezone.now(), is_applied=True, file=file)
                
                for database in synchronized_databases:
                    models.SynchronizationDatabase.objects.create(synchronization=synchronization, database=database)

                synchronizations.append(synchronization)

            else:
                logging.error("No synchronizations were applied")
        
        return Response( data=SynchronizationSerializer(synchronizations, many=True).data )

    @action(
        methods=["DELETE",], detail=False
    )
    def delete_all(self, request, *args, **kwargs):
        user = request.user

        if not user.is_superuser:
            return Response( data={"message": _("You are not authorized to access this resource")}, status=status.HTTP_403_FORBIDDEN )

        all_synchronizations = models.Synchronization.objects.all()
        data = SynchronizationSerializer(all_synchronizations, many=True).data

        all_synchronizations.delete()

        return Response( data=data )