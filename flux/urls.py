from rest_framework_extensions.routers import NestedRouterMixin
from rest_framework import routers

from . import views

class NestedDefaultRouter(NestedRouterMixin, routers.DefaultRouter):
    pass

router = NestedDefaultRouter()

extraction_routes = router.register("extractions", views.ExtractionViewSet, basename="extractions")
synchronization_routes = router.register("synchronizations", views.SynchronizationViewSet, basename="synchronizations")
file_routes = router.register("files", views.FileViewSet, basename="files")

urlpatterns = router.urls