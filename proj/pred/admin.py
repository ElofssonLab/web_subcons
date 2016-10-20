from django.contrib import admin

# Register your models here.

# add this so that the app pred is editable in 127.0.0.1:8000/admin
from pred.models import Query

admin.site.register(Query)
