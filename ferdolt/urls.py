from rest_framework_extensions.routers import NestedRouterMixin

from rest_framework import routers

from . import views

class NestedDefaultRouter(NestedRouterMixin, routers.DefaultRouter):
    pass

router = NestedDefaultRouter()

dbms_routes = router.register("dbms", views.DatabaseManagementSystemViewSet, basename="dbms")

dbms_version_routes = router.register(
    "dbms_versions", 
views.DatabaseManagementSystemVersionViewSet, basename="dbms_versions")

database_routes = router.register("databases", 
views.DatabaseViewSet, "databases")

database_schema_routes = router.register("schemas", 
views.DatabaseSchemaViewSet, "schemass")

table_routes = router.register("tables", 
views.TableViewSet, "tables")

column_routes = router.register("columns", 
views.ColumnViewSet, "columns")

column_constraint_routes = router.register("constraints", 
views.ColumnConstraintViewSet, "constraints")

server_routes = router.register("servers", views.ServerViewSet, "servers")

create_server_request_routes = router.register("server_requests", views.CreateServerRequestViewSet, "server_requests")

urlpatterns = router.urls