"""marsabit URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls import include

from wkhtmltopdf.views import PDFTemplateView

from livhealth_scripts import views
from livhealth_scripts.api import views as api_views
from livhealth_scripts import reports

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^accounts/', include('django.contrib.auth.urls')),
    url(r'static/(?P<path>.*)$', views.serve_static_files),
    url(r'^$', views.landing_page, name='landing_page'),
    url(r'^sdss$', views.landing_page, name='landing_page'),
    url(r'^home$', views.landing_page, name='landing_page'),
    url(r'^dashboard_v2', views.dash_v2, name='dashboard_v2'),
    url(r'^login$', views.login_page, name='login_page'),
    url(r'^logout$', views.logout_view, name='logout'),
    url(r'^form_structure/', views.form_structure, name='form_structure'),
    url(r'^update_db/', views.update_db, name='update_db'),
    url(r'^form_structure/', views.form_structure, name='form_structure'),
    url(r'^download/$', views.download_page, name='download_page'),
    url(r'^manage_views/$', views.manage_views, name='manage_views'),
    url(r'^edit_view/$', views.modify_view, name='modify_view'),
    url(r'^delete_view/$', views.modify_view, name='modify_view'),
    url(r'^get_data/$', views.download_data, name='download_data'),
    url(r'^refresh_forms/$', views.refresh_forms, name='refresh_forms'),
    url(r'^biweekly/$', views.biweekly, name='biweekly'),
    url(r'^privacy_policy\.html$', views.privacy_policy, name='privacy_policy'),

    # api urls
    url(r'^smsqueue_u$', api_views.SMSQueueView.as_view(), name='SMSQueue'),
    url(r'^api/', include('livhealth_scripts.api.urls')),

    # v2 urls
    url(r'^dashboard_v1/$', views.show_dashboard, name='dashboard_v1'),
    url(r'^nd1/$', views.nd1, name='nd1'),
    url(r'^abattoir/$', views.abattoir, name='abattoir'),
    url(r'^agrovet/$', views.agrovet, name='agrovet'),

    # improvement
    url(r'^notification_settings/$', views.notification_settings, name='notification_settings'),
    url(r'^sent_notifications/$', views.sent_notifications, name='sent_notifications'),

    url(r'^delete_notification/$', views.manage_objects, name='delete_notification'),
    url(r'^delete_campaign/$', views.manage_objects, name='delete_campaign'),
    url(r'^delete_recipient/$', views.manage_objects, name='delete_recipient'),
    url(r'^delete_template/$', views.manage_objects, name='delete_template'),

    url(r'^get_campaign/$', views.manage_objects, name='get_campaign'),
    url(r'^get_template/$', views.manage_objects, name='get_template'),
    url(r'^get_recipient/$', views.manage_objects, name='get_recipient'),

    url(r'^deactivate_campaign/$', views.manage_objects, name='deactivate_campaign'),
    url(r'^deactivate_recipient/$', views.manage_objects, name='deactivate_recipient'),
    url(r'^deactivate_template/$', views.manage_objects, name='deactivate_template'),

    url(r'^save-campaign/$', views.save_campaign, name='save_campaign'),
    url(r'^save-template/$', views.save_template, name='save_template'),
    url(r'^save-recipient/$', views.save_recipient, name='save_recipient'),

    url(r'^reports/(?P<hashid>[0-9a-zA-Z]{5,})$', reports.report_wrapper, name='view_report'),

    url(r'^pdf/$', PDFTemplateView.as_view(template_name='landing.html', filename=None), name='dynamic_pdf'),
]
