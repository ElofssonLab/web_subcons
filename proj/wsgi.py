"""
WSGI config for proj project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/howto/deployment/wsgi/
"""

import os
import sys
import site

from subprocess import Popen, PIPE
from os import environ


rundir = os.path.dirname(os.path.abspath(__file__))
basedir = "%s/.."%(rundir)
# Activate the virtual env
activate_env="%s/env/bin/activate_this.py"%(basedir)
execfile(activate_env, dict(__file__=activate_env))
os.system("which python")

#Add the site-packages of the virtualenv
site.addsitedir("%s/env/lib/python2.7/site-packages/"%(basedir))

# Add the directory for the project
sys.path.append(basedir)
sys.path.append(rundir)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "proj.settings")


from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
