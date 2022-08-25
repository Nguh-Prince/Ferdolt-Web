from django.urls import path

from . import views

app_name = "frontend"

urlpatterns = [
    path("", views.index, name="index"),
    path("databases/", views.databases, name="databases"),
    path("databases/<int:id>/", views.databases, name="database-detail"),
    path("schemas/", views.schemas, name="schemas"),
    path("tables/", views.tables, name="tables"),
    path("tables/<int:id>/", views.tables, name="tables"),
    path("columns/", views.columns, name="columns"),
    path("file_manager/", views.file_manager, name="file_manager"),
    path("servers/", views.servers, name='servers'),
    path("servers/<int:id>", views.servers, name='server-detail'),
    path("not_found/", views.not_found, name="not_found"),
]