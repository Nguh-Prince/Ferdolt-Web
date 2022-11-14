from django.core.exceptions import ImproperlyConfigured

from rest_framework.serializers import Serializer
from rest_framework import viewsets

from ferdolt import permissions

class MultipleSerializerViewSet(viewsets.GenericViewSet):
    serializer_classes = {

    }
    
    def get_serializer_class(self) -> Serializer:
        if not isinstance(self.serializer_classes, dict):
            raise ImproperlyConfigured(_("serializer_classes variable must be a dict mapping"))

        if self.action in self.serializer_classes.keys():
            return self.serializer_classes[self.action]
        
        return super().get_serializer_class()

class MultiplePermissionViewSet(viewsets.GenericViewSet):
    permission_classes_by_action = {

    }

    def get_permissions(self):
        try:
            return [ permission() for permission in self.permission_classes_by_action[self.action] ]
        except KeyError:
            return [ permission() for permission in self.permission_classes ]
