from django.urls import path

from . import views

app_name = "frontend"

urlpatterns = [
    path("", views.index, name="index"),
    path("databases/", views.databases, name="databases"),
    path("databases/<int:id>/initialize/", views.initialize, name="initialize-database"),
    path("databases/<int:id>/delete/", views.delete_database, name="delete-database"),
    path("databases/<int:id>", views.databases, name="database-detail"),
    path("extractions/", views.extractions, name="extractions"),
    path("schemas/", views.schemas, name="schemas"),
    path('groups/', views.groups, name='groups'),
    path('groups/<int:id>', views.groups, name='groups'),
    path("tables/", views.tables, name="tables"),
    path("tables/<int:id>/", views.tables, name="tables"),
    path("columns/", views.columns, name="columns"),
    path("file_manager/", views.file_manager, name="file_manager"),
    path("servers/", views.servers, name='servers'),
    path("servers/<int:id>", views.servers, name='server-detail'),
    path("synchronizations/", views.synchronizations, name='synchronizations'),
    path("not_found/", views.not_found, name="not_found"),
    path("login/", views.login_view, name='login'),
    path("logout/", views.logout_view, name='logout'),
    path('get_stats/', views.get_stats, name='get_stats'),
]