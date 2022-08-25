from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/ferdolt/groups', include("groups.urls")),
    path('api/ferdolt/', include('ferdolt.urls')),
    path('api/ferdolt/flux/', include('flux.urls')),
    path('api-auth/', include("rest_framework.urls")),
    path("", include("frontend.urls"))
] + static ( settings.MEDIA_URL, document_root=settings.MEDIA_ROOT )
