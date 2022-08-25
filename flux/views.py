from rest_framework import viewsets
from rest_framework.decorators import action
from django.core.exceptions import ImproperlyConfigured
from django.utils.translation import gettext as _

from flux import serializers

from . import models

class MultipleSerializerViewSet(viewsets.GenericViewSet):
    serializer_classes = {

    }
    
    def get_serializer_class(self):
        if not isinstance(self.serializer_classes, dict):
            raise ImproperlyConfigured(_("serializer_classes variable must be a dict mapping"))

        if self.action in self.serializer_classes.keys():
            return self.serializer_classes[self.action]
        
        return super().get_serializer_class()

class ExtractionViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.ExtractionSerializer

    def get_queryset(self):
        return models.Extraction.objects.all()


