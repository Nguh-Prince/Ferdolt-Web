from django.contrib.auth import get_user_model, authenticate
from django.utils.translation import gettext as _
from django.db.models import Q
from rest_framework import serializers

User = get_user_model()

def get_and_authenticate_user(username: str, password: str):
    """
    identifier can either be username, phone number or email address
    """
    user_not_found_exception = serializers.ValidationError( _("Invalid username or password. Please try again!") )
    try:
        user = User.objects.get( Q(username=username) )
        
        user = authenticate(username=user.username, password=password)
        
        if user:
            return user
        raise user_not_found_exception

    except User.DoesNotExist:
        raise user_not_found_exception

def create_user_account(username, password, first_name="", last_name="", email="", phone="", country_code="", **kwargs):
    user = get_user_model().objects.create_user(username=username, email=email, password=password, first_name=first_name, last_name=last_name, is_staff=False)
    return user
