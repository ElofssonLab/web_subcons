from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

from proj import views
from proj import pred

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'proj.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^pred/', include('pred.urls')),
    url(r'^accounts/login/$', 'django.contrib.auth.views.login'),
    url(r'^accounts/logout/$', 'django.contrib.auth.views.logout', {'next_page': '/pred/login/'}),
    url(r'^$', views.home, name='home'),
)
