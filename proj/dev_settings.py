"""
Django settings for proj project in development.

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

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.2.7/howto/deployment/checklist/
SECRET_KEY = '5&!cq9#+(_=!ou=mco0=-qrmn6h66o(f)h$ho4+0vo1#d24xdy'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []
allowed_host_file = "%s/allowed_host_dev.txt"%(BASE_DIR)
computenodefile = "%s/pred/config/computenode.txt"%(BASE_DIR)
for f in [allowed_host_file, computenodefile]:
    if os.path.exists(f):
        ALLOWED_HOSTS +=  myfunc.ReadIDList2(f,col=0)

# add also the host ip address
hostip = webcom.get_external_ip()
ALLOWED_HOSTS.append(hostip)

