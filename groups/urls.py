from rest_framework_extensions.routers import NestedRouterMixin
from rest_framework import routers

from . import views

class NestedDefaultRouter(NestedRouterMixin, routers.DefaultRouter):
    pass

router = NestedDefaultRouter()

group_routes = router.register("", 
views.GroupViewSet, basename="groups")

urlpatterns = router.urls