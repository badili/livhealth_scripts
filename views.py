  # Python 2 only

import json
import logging
import traceback
import datetime
import re

import posixpath
from urllib.parse import unquote
from raven import Client

from django.conf import settings
from django.contrib.staticfiles import finders
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.views import static
from django.http import HttpResponse, Http404, JsonResponse
from django.shortcuts import render, redirect
from django.middleware import csrf
from django.forms.models import model_to_dict

from wsgiref.util import FileWrapper

from .odk_forms import OdkForms
from .notifications import Notification
from .terminal_output import Terminal
from .models import SMSQueue, Campaign, Recipients, MessageTemplates
from livhealth_scripts.site_management import SiteManager

import os
terminal = Terminal()
sentry = Client(settings.SENTRY_DSN)


def login_page(request):
    csrf_token = get_or_create_csrf_token(request)
    page_settings = {'page_title': "%s | Login Page" % settings.SITE_NAME, 'csrf_token': csrf_token}
    terminal.tprint(csrf_token, 'ok')

    try:
        username = request.POST['username']
        password = request.POST['pass']

        if username is not None:
            user = authenticate(username=username, password=password)

            if user is None:
                terminal.tprint("Couldn't authenticate the user... redirect to login page", 'fail')
                page_settings['error'] = 'Invalid username or password'
                return render(request, 'login.html', page_settings)
            else:
                terminal.tprint('All ok', 'debug')
                login(request, user)
                return redirect('/dashboard_v2', request=request)
        else:
            return render(request, 'login.html')
    except KeyError as e:
        # ask the user to enter the username and/or password
        terminal.tprint('Username/password not defined', 'warn')
        page_settings['message'] = "Please enter your username and password"
        return render(request, 'login.html', page_settings)
    except Exception as e:
        terminal.tprint(str(e), 'fail')
        page_settings['error'] = "There was an error while authenticating.<br />Please try again and if the error persist, please contact the system administrator"
        return render(request, 'login.html', page_settings)


def logout_view(request):
    logout(request)
    csrf_token = get_or_create_csrf_token(request)
    return render(request, 'landing.html')


def under_review_page(request):
    csrf_token = get_or_create_csrf_token(request)
    return render(request, 'under_review.html')


def landing_page(request):
    csrf_token = get_or_create_csrf_token(request)
    return render(request, 'landing.html')


def privacy_policy(request):
    return render(request, 'privacy_policy.html')


@login_required(login_url='/login')
def download_page(request):
    csrf_token = get_or_create_csrf_token(request)

    # get all the data to be used to construct the tree
    odk = OdkForms(request)
    all_forms = odk.get_all_forms()
    page_settings = {
        'page_title': "%s | Downloads" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Download Section',
        'all_forms': json.dumps(all_forms)
    }
    return render(request, 'download.html', page_settings)


@login_required(login_url='/login')
def modify_view(request):

    odk = OdkForms(request)
    if (request.get_full_path() == '/edit_view/'):
        response = odk.edit_view(request)
    elif (request.get_full_path() == '/delete_view/'):
        response = odk.delete_view(request)

    return HttpResponse(json.dumps(response))


@login_required(login_url='/login')
def manage_views(request):
    csrf_token = get_or_create_csrf_token(request)

    # get all the data to be used to construct the tree
    odk = OdkForms(request)
    all_data = odk.get_views_info()

    page_settings = {
        'page_title': "%s | Manage Generated Views" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Manage Views',
        'all_data': json.dumps(all_data)
    }
    return render(request, 'manage_views.html', page_settings)


@login_required(login_url='/login')
def update_db(request):
    odk = OdkForms(request)

    try:
        odk.update_sdss_db()
    except Exception as e:
        logging.error(traceback.format_exc())
        print((str(e)))
        return HttpResponse(traceback.format_exc())

    return HttpResponse(json.dumps({'error': False, 'message': 'Database updated'}))


@login_required(login_url='/login')
def show_dashboard(request):
    csrf_token = get_or_create_csrf_token(request)

    odk = OdkForms(request)
    stats = odk.system_stats()
    r_period = request.session['r_period']

    if r_period == 'past_week':
        period_narrative = 'For the previous 7 days'
    elif r_period == 'past_month':
        period_narrative = 'For the past month'
    elif r_period == 'past_3mo':
        period_narrative = 'For the past 3 months'
    elif r_period == 'past_6mo':
        period_narrative = 'For the past 6 months'
    else:
        period_narrative = 'Default: Last 7 days'

    page_settings = {
        'page_title': "%s | Home" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'LivHealth Dashboard',
        'data': stats,
        'r_period': r_period,
        'period_narrative': period_narrative
    }
    return render(request, 'dash_home.html', page_settings)


@login_required(login_url='/login')
def form_structure(request):
    # given a form id, get the structure for the form
    odk = OdkForms(request)
    try:
        form_id = int(request.POST['form_id'])
        structure = odk.get_form_structure_as_json(form_id)
    except KeyError:
        return HttpResponse(traceback.format_exc())
    except Exception as e:
        print((str(e)))
        logging.error(traceback.format_exc())

    return HttpResponse(json.dumps({'error': False, 'structure': structure}))


@login_required(login_url='/login')
def form_structure(request):
    # given a form id, get the structure for the form
    odk = OdkForms(request)
    try:
        form_id = int(request.POST['form_id'])
        structure = odk.get_form_structure_as_json(form_id)
    except KeyError as e:
        logging.error(traceback.format_exc())
        return HttpResponse(json.dumps({'error': True, 'message': str(e)}))
    except Exception as e:
        logging.info(str(e))
        logging.debug(traceback.format_exc())
        return HttpResponse(json.dumps({'error': True, 'message': str(e)}))

    return HttpResponse(json.dumps({'error': False, 'structure': structure}))


@login_required(login_url='/login')
def download_data(request):
    # given the nodes, download the associated data
    odk = OdkForms(request)
    try:
        data = json.loads(request.body)
        res = odk.fetch_merge_data(data['form_id'], data['nodes[]'], data['format'], data['action'], data['view_name'])
    except KeyError as e:
        response = HttpResponse(json.dumps({'error': True, 'message': str(e)}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': True, 'message': str(e)})
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        logging.error(str(e))
        response = HttpResponse(json.dumps({'error': True, 'message': str(e)}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': True, 'message': str(e)})
        return response

    if res['is_downloadable'] is True:
        filename = res['filename']
        wrapper = FileWrapper(open(filename, 'rb'))
        response = HttpResponse(wrapper, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % os.path.basename(filename)
        response['Content-Length'] = os.path.getsize(filename)
    else:
        response = HttpResponse(json.dumps({'error': False, 'message': res['message']}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': False, 'message': res['message']})

    return response


@login_required(login_url='/login')
def download(request):
    # given the nodes, download the associated data
    odk = OdkForms(request)
    try:
        data = json.loads(request.body)
        filename = odk.fetch_data(data['form_id'], data['nodes[]'], data['format'])
    except KeyError:
        return HttpResponse(traceback.format_exc())
    except Exception as e:
        print((str(e)))
        logging.error(traceback.format_exc())

    wrapper = FileWrapper(file(filename))
    response = HttpResponse(wrapper, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=%s' % os.path.basename(filename)
    response['Content-Length'] = os.path.getsize(filename)

    return response


@login_required(login_url='/login')
def refresh_forms(request):
    """
    Refresh the database with any new forms
    """
    odk = OdkForms(request)

    try:
        all_forms = odk.refresh_forms()
    except Exception:
        logging.error(traceback.format_exc())

    return HttpResponse(json.dumps({'error': False, 'all_forms': all_forms}))


def get_or_create_csrf_token(request):
    token = request.META.get('CSRF_COOKIE', None)
    if token is None:
        token = csrf._get_new_csrf_string()
        request.META['CSRF_COOKIE'] = token
    request.META['CSRF_COOKIE_USED'] = True
    return token


def serve_static_files(request, path, insecure=False, **kwargs):
    """
    Serve static files below a given point in the directory structure or
    from locations inferred from the staticfiles finders.
    To use, put a URL pattern such as::
        from django.contrib.staticfiles import views
        url(r'^(?P<path>.*)$', views.serve)
    in your URLconf.
    It uses the django.views.static.serve() view to serve the found files.
    """

    if not settings.DEBUG and not insecure:
        raise Http404
    normalized_path = posixpath.normpath(unquote(path)).lstrip('/')
    absolute_path = finders.find(normalized_path)
    if not absolute_path:
        if path.endswith('/') or path == '':
            raise Http404("Directory indexes are not allowed here.")
        raise Http404("'%s' could not be found" % path)
    document_root, path = os.path.split(absolute_path)
    return static.serve(request, path, document_root=document_root, **kwargs)


def biweekly(request):
    csrf_token = get_or_create_csrf_token(request)

    # get all the data to be used to construct the tree
    odk = OdkForms(request)
    all_data = odk.get_biweekly_report()
    terminal.tprint(json.dumps(all_data['sc']), 'ok')
    today = datetime.date.today()
    today_f = today.strftime('%a, %d %b %Y')

    page_settings = {
        'page_title': "%s | Biweekly Report" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Biweekly Report',
        'data': all_data,
        'today_f': today_f,
        'report_no': all_data['report_no']
    }
    # PDFTemplateResponse(request=request, context=page_settings, template='biweekly_report_pdf.html', filename='biweekly_report.pdf')
    return render(request, 'biweekly_report_pdf.html', page_settings)


@login_required(login_url='/login')
def dash_v2(request):
    csrf_token = get_or_create_csrf_token(request)

    odk = OdkForms(request)
    stats = odk.system_stats()

    if 'refresh_type' in request.POST:
        inputs = json.dumps(request.POST)
    else:
        inputs = {}

    err_msg = None
    try:
        inputs = {} if len(inputs) == 0 else json.loads(inputs)
        stats_v2 = odk.system_stats_v2(inputs)
    except Exception as e:
        print((str(e)))
        logging.error(traceback.format_exc())
        err_msg = "There was an error while generating the dashboard. Kindly contact the system administrator"
        sentry.captureException()

        # show a 404 page
        page_settings = {
            'page_title': "%s | Dashboard v2" % settings.SITE_NAME,
            'csrf_token': csrf_token,
            'section_title': 'Dashboard',
            'err_msg': err_msg
        }
        return render(request, '404.html', page_settings)

    page_settings = {
        'page_title': "%s | Dashboard v2" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Dashboard',
        'data': stats,
        'data_v2': stats_v2,
        'err_msg': err_msg
    }
    return render(request, 'dash_home_v2.html', page_settings)


@login_required(login_url='/login')
def nd1(request):
    csrf_token = get_or_create_csrf_token(request)

    err_msg = None
    data_nd = None
    try:
        odk = OdkForms(request)

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)
        data_nd = odk.generate_nd_system_stats(inputs)
    except Exception as e:
        err_msg = str(e)
        print(err_msg)
        logging.error(traceback.format_exc())
        # inputs = {}
        # stats_v2 = odk.system_stats_v2(inputs)

    page_settings = {
        'page_title': "%s | ND1 Reports" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'ND1 Reports',
        'data_nd': data_nd,
        'err_msg': err_msg
    }
    return render(request, 'nd_dash.html', page_settings)


@login_required(login_url='/login')
def abattoir(request):
    csrf_token = get_or_create_csrf_token(request)

    err_msg = None
    data_ag = None
    try:
        odk = OdkForms(request)

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)
        data_sh = odk.generate_abattoir_system_stats(inputs)
    except Exception as e:
        err_msg = str(e)
        print(err_msg)
        logging.error(traceback.format_exc())
        # inputs = {}
        # stats_v2 = odk.system_stats_v2(inputs)

    page_settings = {
        'page_title': "%s | Abattoir Records" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Abattoir Records',
        'data_sh': data_sh,
        'err_msg': err_msg
    }
    return render(request, 'sh_dash.html', page_settings)


@login_required(login_url='/login')
def agrovet(request):
    csrf_token = get_or_create_csrf_token(request)

    err_msg = None
    data_ag = None
    try:
        odk = OdkForms(request)

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)
        data_ag = odk.generate_agrovet_system_stats(inputs)
    except Exception as e:
        err_msg = str(e)
        print(err_msg)
        logging.error(traceback.format_exc())
        # inputs = {}
        # stats_v2 = odk.system_stats_v2(inputs)

    page_settings = {
        'page_title': "%s | Agrovet Summaries" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Agrovet Summaries',
        'data_ag': data_ag,
        'err_msg': err_msg
    }
    return render(request, 'ag_dash.html', page_settings)


@login_required(login_url='/login')
def notification_settings(request):
    csrf_token = get_or_create_csrf_token(request)

    try:
        err_msg = None
        notify = Notification()
        notify_settings = notify.get_notification_settings()

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)

        page_settings = {
            'page_title': "%s | Notification Settings" % settings.SITE_NAME,
            'csrf_token': csrf_token,
            'section_title': 'Settings',
            'data': notify_settings,
            'err_msg': err_msg
        }
        return render(request, 'notification_settings.html', page_settings)

    except Exception as e:
        print(str(e))
        sentry.captureException()
        return redirect('/dashboard_v2/')


@login_required(login_url='/login')
def system_settings(request):
    csrf_token = get_or_create_csrf_token(request)

    try:
        err_msg = None
        notify = Notification()
        notify_settings = notify.get_notification_settings()

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)

        page_settings = {
            'page_title': "%s | System Settings" % settings.SITE_NAME,
            'csrf_token': csrf_token,
            'section_title': 'Settings',
            'data': notify_settings,
            'err_msg': err_msg
        }
        return render(request, 'system_settings.html', page_settings)

    except Exception as e:
        print(str(e))
        sentry.captureException()
        return redirect('/dashboard_v2/')


@login_required(login_url='/login')
def sent_notifications(request):
    # print('Show the sent notifications')
    csrf_token = get_or_create_csrf_token(request)

    try:
        err_msg = None
        notify = Notification()
        sent_notifications = notify.get_sent_notifications()

        # get the inputs required
        inputs = json.dumps(request.POST) if 'refresh_type' in request.POST else {}
        inputs = {} if len(inputs) == 0 else json.loads(inputs)

        page_settings = {
            'page_title': "%s | Sent Notifications" % settings.SITE_NAME,
            'csrf_token': csrf_token,
            'section_title': 'Sent Notifications',
            'data': sent_notifications,
            'err_msg': err_msg
        }
        return render(request, 'sent_notifications.html', page_settings)

    except Exception as e:
        print(str(e))
        sentry.captureException()
        return redirect('/dashboard_v2/')


@login_required(login_url='/login')
def manage_objects(request):
    # delete the notification with the sent id
    # csrf_token = get_or_create_csrf_token(request)

    try:
        object_id = request.POST['object_id']
        if re.search('notification$', request.resolver_match.url_name):
            cur_object = SMSQueue.objects.filter(id=object_id).get()
        elif re.search('campaign$', request.resolver_match.url_name):
            cur_object = Campaign.objects.filter(id=object_id).get()
        elif re.search('recipient$', request.resolver_match.url_name):
            cur_object = Recipients.objects.filter(id=object_id).select_related('village').select_related('ward').select_related('village').get()
            if cur_object.village is not None:
                cur_object.ward = cur_object.village.ward
                cur_object.sub_county = cur_object.village.ward.sub_county

        elif re.search('template$', request.resolver_match.url_name):
            cur_object = MessageTemplates.objects.filter(id=object_id).get()

        if re.search('^delete', request.resolver_match.url_name):
            if re.search('recipient$', request.resolver_match.url_name):
                # check if there are some SMS Queue that are yet to be sent and delete
                SMSQueue.objects.filter(recipient=cur_object).delete()

            cur_object.delete()
            return HttpResponse(json.dumps({'error': False, 'message': 'The %s has been deleted successfully' % re.search('_(.+)$', request.resolver_match.url_name).group(1)}))
        elif re.search('^deactivate', request.resolver_match.url_name):
            cur_object.is_active = not cur_object.is_active
            cur_object.save()
            return HttpResponse(json.dumps({'error': False, 'message': 'The %s has been updated successfully' % re.search('_(.+)$', request.resolver_match.url_name).group(1)}))
        elif re.search('^get', request.resolver_match.url_name):
            return HttpResponse(json.dumps({'error': False, 'message': 'The %s has been fetched successfully' % re.search('_(.+)$', request.resolver_match.url_name).group(1), 'object': model_to_dict(cur_object)}))
    except Exception as e:
        print(str(e))
        return HttpResponse(json.dumps({'error': True, 'message': 'There was an error while managing the %s' % re.search('_(.+)$', request.resolver_match.url_name).group(1)}))


@login_required(login_url='/login')
def save_campaign(request):
    csrf_token = get_or_create_csrf_token(request)

    try:
        notify = Notification()
        notify.save_campaign(request)

        return HttpResponse(json.dumps({'error': False, 'message': 'The campaign was saved successfully'}))
    except Exception:
        return HttpResponse(json.dumps({'error': True, 'message': 'There was an error while saving the campaign details'}))


@login_required(login_url='/login')
def save_template(request):
    csrf_token = get_or_create_csrf_token(request)

    try:
        notify = Notification()
        notify.save_template(request)

        return HttpResponse(json.dumps({'error': False, 'message': 'The template was saved successfully'}))
    except Exception:
        return HttpResponse(json.dumps({'error': True, 'message': 'There was an error while saving the notification template details'}))


@login_required(login_url='/login')
def save_recipient(request):
    csrf_token = get_or_create_csrf_token(request)

    try:
        site_man = SiteManager()
        site_man.save_recipient(request)

        return HttpResponse(json.dumps({'error': False, 'message': 'The recipient was saved successfully'}))
    except Exception as e:
        print(str(e))
        return HttpResponse(json.dumps({'error': True, 'message': 'There was an error while saving the recipient details'}))
