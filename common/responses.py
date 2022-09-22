from django.utils.translation import gettext as _

from rest_framework import status
from rest_framework.response import Response

def get_error_response(message=_("An error occured on the server, please try again"), status=status.HTTP_500_INTERNAL_SERVER_ERROR):
    return Response({'message': message}, status=status)