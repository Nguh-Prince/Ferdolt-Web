from rest_framework_extensions.routers import NestedRouterMixin
from rest_framework import routers

from . import views

class NestedDefaultRouter(NestedRouterMixin, routers.DefaultRouter):
    pass

router = NestedDefaultRouter()

extraction_routes = router.register("extractions", views.ExtractionViewSet, basename="extractions")

urlpatterns = router.urls