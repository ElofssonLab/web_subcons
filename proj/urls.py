from django.conf.urls import include, url

from django.contrib import admin
admin.autodiscover()

from proj import views
from proj import pred

urlpatterns = [
    # Examples:
    # url(r'^$', 'proj.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^pred/', include('proj.pred.urls')),
    url(r'^accounts/', include('django.contrib.auth.urls')), # new
    url(r'^$', views.home, name='home'),
]
