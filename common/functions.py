from ipaddress import ip_address
import re
import string
import random

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
    random_string = random.choices( string.ascii_letters + string.digits + string.punctuation, k=length )