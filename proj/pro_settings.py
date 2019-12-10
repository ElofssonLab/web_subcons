"""
Django settings for proj project in production

For more information on this file, see
https://docs.djangoproject.com/en/2.2.7/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.2.7/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
from libpredweb import myfunc
from libpredweb import webserver_common as webcom
try:
    from .shared_settings import *
except ImportError:
    pass

with open('/etc/django_pro_secret_key.txt') as f:
    SECRET_KEY = f.read().strip()


# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = []
allowed_host_file = "%s/allowed_host_pro.txt"%(BASE_DIR)
computenodefile = "%s/pred/config/computenode.txt"%(BASE_DIR)
for f in [allowed_host_file, computenodefile]:
    if os.path.exists(f):
        ALLOWED_HOSTS +=  myfunc.ReadIDList2(f,col=0)

# add also the host ip address
hostip = webcom.get_external_ip()
ALLOWED_HOSTS.append(hostip)

