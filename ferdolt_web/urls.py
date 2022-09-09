from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

from rest_framework.response import Response
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer

@api_view(["GET"])
@renderer_classes((JSONRenderer, BrowsableAPIRenderer))
def health(request):
    return Response({"data": "The server is online"})

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/health', health),
    path('api/ferdolt/groups', include("groups.urls")),
    path('api/ferdolt/', include('ferdolt.urls')),
    path('api/ferdolt/flux/', include('flux.urls')),
    path('api/ferdolt/groups/', include('groups.urls')),
    path('api/users/', include('users.urls')),
    path('api-auth/', include("rest_framework.urls")),
    path('chat/', include("communication.urls")),
    path("", include("frontend.urls")),
] + static ( settings.MEDIA_URL, document_root=settings.MEDIA_ROOT )
