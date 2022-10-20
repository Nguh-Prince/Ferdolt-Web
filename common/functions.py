import hashlib
from ipaddress import ip_address
import re
import string
import random

from django.core.mail import send_mail
from django.contrib.auth.models import User
from django.db.models import Q

from ferdolt_web import settings

def is_valid_hostname(hostname):
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

def is_valid_ip_address(ip):
    try:
        ip = ip_address(ip)
    except ValueError as e:
        raise e

def generate_random_string(length=8):
    random_string = ''.join(random.choices( string.ascii_letters + string.digits + string.punctuation, k=length ))

    return random_string

def hash_file(filename):
    h = hashlib.sha256()

    with open(filename, 'rb') as file:
        chunk = 0

        while chunk != b'':
            chunk = file.read(1024)
            h.update(chunk)

    return h.hexdigest()

def send_email_to_admins(subject, message):
    recipient_list = User.objects.filter(
        Q(groups__name__iexact='admin') | Q(is_superuser=True)
    ).filter(
        email__isnull=False
    )

    email_from = settings.EMAIL_HOST_USER

    send_mail(subject, message, email_from, recipient_list)

def encrypt(message, key):
    pass