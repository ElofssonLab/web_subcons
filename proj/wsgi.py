"""
WSGI config for proj project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os
import sys
import site

rundir = os.path.dirname(os.path.abspath(__file__))
basedir = os.path.abspath("%s/../"%(rundir))
path_log = "%s/pred/static/log"%(rundir)

# Activate the virtual env
activate_env="%s/env/bin/activate_this.py"%(basedir)
exec(compile(open(activate_env, "r").read(), activate_env, 'exec'), dict(__file__=activate_env))
os.system("echo which python; which python >> %s/debug.log" %( path_log) )

#Add the site-packages of the virtualenv
site.addsitedir("%s/env/lib/python3.7/site-packages/"%(basedir))

# Add the directory for the project
sys.path.append(basedir)
#sys.path.insert(0,"%s/env/bin"%(basedir))


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
