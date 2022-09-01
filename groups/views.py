from rest_framework import viewsets

from common.viewsets import MultipleSerializerViewSet

from . import models, serializers

class GroupDatabaseViewSet(MultipleSerializerViewSet, viewsets.ModelViewSet):
    serializer_class = serializers.GroupDatabaseSerializer
    serializer_classes = {
        'create': serializers.GroupDatabaseCreationSerializer
    }

    def get_queryset(self):
        return models.GroupDatabase.objects.all()
