import copy
from rest_framework import permissions

class ModelPermission(permissions.DjangoModelPermissions):
    def __init__(self):
        self.perms_map = copy.deepcopy(self.perms_map)
        self.perms_map["GET"] = ["%(app_label)s.view_%(model_name)s"]

class DummyPermission(permissions.AllowAny):
    pass