from django.core.exceptions import ImproperlyConfigured

from rest_framework import viewsets

class MultipleSerializerViewSet(viewsets.GenericViewSet):
    serializer_classes = {

    }
    
    def get_serializer_class(self):
        if not isinstance(self.serializer_classes, dict):
            raise ImproperlyConfigured(_("serializer_classes variable must be a dict mapping"))

        if self.action in self.serializer_classes.keys():
            return self.serializer_classes[self.action]
        
        return super().get_serializer_class()
