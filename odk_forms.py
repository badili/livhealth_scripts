import requests, re, os, sys
import logging, traceback, json
import copy
import subprocess
import hashlib
import dateutil.parser
import math
import time
import shutil
import csv

from raven import Client

from configparser import ConfigParser
from datetime import datetime, date, timedelta

from django.utils import timezone
from django.conf import settings
from django.http import HttpResponse, HttpRequest
from django.db import IntegrityError, connection
from django.db.models import Q
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.core.exceptions import SuspiciousOperation

from django.template import Context

from collections import defaultdict

from django.core import serializers

from .terminal_output import Terminal
from .excel_writer import ExcelWriter
from .models import *
from .sql import Query

terminal = Terminal()

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
        },
    },
}
logger = logging.getLogger('ODKForms')
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)
request = HttpRequest()
sentry = Client(settings.SENTRY_DSN)


class OdkForms():
    def __init__(self, request=None):

        self.server = settings.ONADATA_URL
        self.api_token = settings.ONADATA_TOKEN

        self.api_all_forms = 'api/v1/forms'
        self.form_data = 'api/v1/data/'
        self.form_stats = 'api/v1/stats/submissions/'
        self.form_rep = 'api/v1/forms/'
        self.media = 'api/v1/media'
        self.metadata_uri = 'api/v1/metadata'

        self.project = settings.PROJECT_NAME
        self.county_name = settings.COUNTY_NAME
        self.form_json_rep = None
        self.top_level_hierarchy = None
        self.cur_node_id = None
        self.form_group = None
        self.cur_form_id = None

        # to avoid errors when running the cron jobs, construct an absolute path to the settings file
        self.forms_settings = os.path.join(os.path.dirname(__file__), 'forms_settings.ini')
        self.form_connection = None
        self.country_qsts = ['c1s1q8_Country_name']
        self.clean_country_codes = None

        self.email_message_inner_template = """
        <p>
            <mj-text font-family="arial" font-size="16px" align="left" color="#808080"> <span style="color:#0098CE"><b>%s:</b></span> %s </mj-text>
        </p>
        """

        self.sub_counties = settings.SUB_COUNTIES
        self.all_species = ['cattle', 'camels', 'sheep', 'goats']

        if request is not None:
            if 'r_period' in request.GET:
                self.r_period = request.GET['r_period']
            else:
                if 'r_period' not in request.session:
                    self.r_period = settings.DEFAULT_REPORTING_PERIOD
                else:
                    if request.session['r_period'] == 'undefined':
                        self.r_period = settings.DEFAULT_REPORTING_PERIOD
                    else:
                        self.r_period = request.session['r_period']

            request.session['r_period'] = self.r_period
            self.get_r_period()
        else:
            self.r_period = 7

    def get_r_period(self, period=None):
        str_period = period if period is not None else self.r_period

        if str_period == 'past_week':
            self.period_days = 7
        elif str_period == 'past_month':
            self.period_days = 30
        elif str_period == 'past_3mo':
            self.period_days = 90
        elif str_period == 'past_6mo':
            self.period_days = 180
        else:
            self.period_days = 180

    def get_all_forms(self):
        """
        Get all the forms belonging to the current project
        """

        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})
        # check whether the form is already saved in the database
        try:
            all_forms = ODKForm.objects.all()
            for form in all_forms:
                to_return.append({'title': form.form_name, 'id': form.form_id})
        except Exception as e:
            terminal.tprint(str(e), 'fail')

        return to_return

    def get_value_from_dictionary(self, t_key, update_dict=True):
        query = """
            SELECT t_value from dictionary_items where t_key = '%s'
        """ % t_key
        with connection.cursor() as cursor:
            cursor.execute(query)
            t_value = cursor.fetchall()
            try:
                return str(t_value[0][0])
            except IndexError as e:
                # We need to process all the form's structure which have a defined structure. We have all this data, so returning an unknown value is not smart
                terminal.tprint("Couldn't find the value for the key '%s' in the dictionary -- Updating the dictionary %s" % (t_key, str(e)), 'fail')
                # to avoid cyclic repetition, check if we need to update the dictionary
                if update_dict:
                    # default for now
                    return str(t_key)
                    dict_value = self.update_dictionary_items(t_key)
                    if dict_value is None:
                        terminal.tprint("Even after the dictionary update, couldn't find the value for the key '%s' in the dictionary. Giving up. %s" % (t_key, str(e)), 'fail')
                        return str(t_key)
                    else:
                        return str(dict_value)
                else:
                    return None
            except Exception as e:
                terminal.tprint("Couldn't find the value for the key '%s' in the dictionary. %s" % (t_key, str(e)), 'fail')
                return str(t_key)

    def update_dictionary_items(self, t_key=None):
        """
        Traverse through all the forms with a saved form structure and update the dictionary items. After the update, get the dictionary value for t_key
        """
        try:
            form_id_bkup = self.cur_form_id
            all_forms = ODKForm.objects.exclude(structure__isnull=True).values('form_id', 'form_name', 'structure')
            for form in all_forms:
                terminal.tprint(json.dumps(form['form_name']), 'ok')
                self.cur_form_id = form['form_id']

                # traverse through the choices and children
                for choice_name in form['structure']['choices']:
                    for choice in form['structure']['choices'][choice_name]:
                        self.add_dictionary_items(choice, 'choice')

                return self.get_value_from_dictionary(t_key, False)
            # restore the form_id backup
            self.cur_form_id = form_id_bkup
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            # restore the form_id backup
            self.cur_form_id = form_id_bkup
            return None

    def refresh_forms(self):
        """
        Refresh the list of forms in the database
        """
        url = "%s/%s" % (self.server, self.api_all_forms)
        all_forms = self.process_curl_request(url)
        if all_forms is None:
            print(("Error while executing the API request %s" % url))
            return

        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})
        for form in all_forms:
            # check whether the form is already saved in the database
            try:
                saved_form = ODKForm.objects.get(full_form_id=form['id_string'])
                terminal.tprint("The form '%s' is already saved in the database" % saved_form.form_name, 'ok')
                to_return.append({'title': saved_form.form_name, 'id': saved_form.form_id, 'full_id': form['id_string']})
            except ODKForm.DoesNotExist as e:
                # this form is not saved in the database, so save it
                terminal.tprint("The form '%s' is not in the database, saving it" % form['id_string'], 'warn')
                cur_form = ODKForm(
                    form_id=form['formid'],
                    form_name=form['title'],
                    full_form_id=form['id_string'],
                    auto_update=False,
                    is_source_deleted=False
                )
                cur_form.publish()
                # lets process the form structure, for forms that are being added dynamically
                self.get_form_structure_as_json(cur_form.form_id)
                to_return.append({'title': form['title'], 'id': form['formid'], 'full_id': form['id_string']})
            except Exception as e:
                terminal.tprint(str(e), 'fail')
                sentry.captureException()

        return to_return

    def get_all_submissions(self, form_id):
        """
        Given a form id, get all the submitted data
        """
        try:
            # the form_id used in odk_forms and submissions is totally different
            terminal.tprint("Processing form with form-id %d" % form_id, 'debug')
            odk_form = ODKForm.objects.get(form_id=form_id)
            submissions = RawSubmissions.objects.filter(form_id=odk_form.id).values('raw_data')
            submitted_instances = self.online_submissions_count(form_id)

            # check whether all the submissions from the db match the online submissions
            if submitted_instances is None:
                # There was an error while fetching the submissions, use 0 as submitted_instances
                submitted_instances = 0

            if submissions.count() == 0 and submitted_instances == 0:
                # logger.info('There are no submissions to process')
                terminal.tprint('\tNo submisions to process', 'fail')
                return None

            if submitted_instances > submissions.count():
                # we have some new submissions, so fetch them from the server and save them offline
                terminal.tprint("\tWe have some new submissions, so fetch them from the server and save them offline", 'info')
                # fetch the submissions and filter by submission time
                url = "%s/%s%d.json?start=1&limit=5&sort=%s" % (self.server, self.form_data, form_id, '{"_submission_time":-1}')
                url = "%s/%s%d.json?fields=[\"_uuid\", \"_id\"]" % (self.server, self.form_data, form_id)
                submission_uuids = self.process_curl_request(url)

                for uuid in submission_uuids:
                    # check if the current uuid is saved in the database
                    cur_submission = RawSubmissions.objects.filter(form_id=odk_form.id, uuid=uuid['_uuid'])
                    if cur_submission.count() == 0:
                        # the current submission is not saved in the database, so fetch and save it...
                        url = "%s/%s%d/%s" % (self.server, self.form_data, form_id, uuid['_id'])
                        submission = self.process_curl_request(url)

                        t_submission = RawSubmissions(
                            form_id=odk_form.id,
                            uuid=submission['_uuid'],
                            submission_time=submission['_submission_time'],
                            raw_data=submission
                        )
                        t_submission.publish()
                    else:
                        # the current submission is already saved, so stop the processing
                        # terminal.tprint("The current submission is already saved, implying that all submissions have been processed, so stop the processing!", 'fail')
                        continue

                # just check if all is now ok
                submissions = RawSubmissions.objects.filter(form_id=odk_form.id).order_by('submission_time').values('raw_data')
                if submissions.count() != submitted_instances:
                    # ok, still the processing is not complete... shout!
                    terminal.tprint("Even after processing submitted responses for '%s', the tally doesn't match (%d vs %d)!" % (odk_form.form_name, submissions.count(), submitted_instances), 'error')
                else:
                    terminal.tprint("Submissions for '%s' successfully updated." % odk_form.form_name, 'info')
            else:
                terminal.tprint("\t'%s': All submissions already saved (%d vs %d)" % (odk_form.form_name, submitted_instances, submissions.count()), 'info')

        except ODKForm.DoesNotExist as e:
            terminal.tprint("The form with form_id %d does not exits. Probably not saved in the database" % form_id, 'error')
            return None
        except Exception as e:
            logger.error('Some error....')
            logger.error(str(e))
            terminal.tprint(str(e), 'error')

        return submissions

    def online_submissions_count(self, form_id):
        # given a form id, process the number of submitted instances
        # terminal.tprint("\tComputing the number of submissions of the form with id '%s'" % form_id, 'info')
        url = "%s/%s%d?%s" % (self.server, self.form_stats, form_id, "group=&name=time")
        stats = self.process_curl_request(url)

        if stats is None:
            logger.error("Error while fetching the number of submissions")
            return None

        submissions_count = 0
        for stat in stats:
            submissions_count += int(stat['count'])

        return submissions_count

    def read_settings(self, settings_file, variable):
        parser = ConfigParser()
        parser.readfp(settings_file)

    def get_form_structure_as_json(self, form_id):
        """
        check whether the form structure is already saved in the DB
        """
        if form_id == -1:
            terminal.tprint("\tNot processing this form with an ID of -1", 'fail')
            return None

        try:
            cur_form = ODKForm.objects.get(form_id=form_id)

            # check if the structure exists
            if cur_form.structure is None:
                # we don't have the structure, so fetch, process and save the structure
                terminal.tprint("\tThe form '%s' doesn't have a saved structure, so lets fetch it and add it" % cur_form.form_name, 'warn')
                (processed_nodes, structure) = self.get_form_structure_from_server(form_id)
                if structure is not None:
                    cur_form.structure = structure
                    cur_form.processed_structure = processed_nodes
                    cur_form.publish()
                else:
                    raise Exception("There was an error in fetching the selected form and it is not yet saved in the database.")
            else:
                terminal.tprint("\tFetching the form's '%s' structure from the database" % cur_form.form_name, 'okblue')
                processed_nodes = cur_form.processed_structure
                # terminal.tprint(json.dumps(cur_form.structure), 'ok')
        except IntegrityError as e:
            # We can live with this
            terminal.tprint(str(e), 'fail')
        except Exception as e:
            print((traceback.format_exc()))
            sentry.captureException()
            logger.debug(str(e))
            terminal.tprint("The form with ID '%d' is not saved in the database" % form_id, 'ok')
            terminal.tprint(str(e), 'fail')
            raise Exception(str(e))

        return processed_nodes

    def get_form_structure_from_server(self, form_id):
        """
        Get the structure of the current form
        """
        url = "%s/%s%d/form.json" % (self.server, self.form_rep, form_id)
        terminal.tprint("Fetching the form structure for form with id = %d" % form_id, 'header')
        form_structure = self.process_curl_request(url)

        if form_structure is None:
            return (None, None)

        self.cur_node_id = 0
        self.cur_form_id = form_id
        self.repeat_level = 0
        self.all_nodes = []
        self.top_node = {"name": "Main", "label": "Top Level", "parent_id": -1, "type": "top_level", "id": 0}

        self.top_level_hierarchy = self.extract_repeating_groups(form_structure, 0)
        self.all_nodes.insert(0, self.top_node)
        # terminal.tprint("Processed %d group nodes" % self.cur_node_id, 'warn')
        
        # get the form metadata if there is additional metadata
        url = "%s/%s?xform=%d" % (self.server, self.metadata_uri, form_id)
        terminal.tprint("Fetching the form metadata for form with id = %d" % form_id, 'header')
        form_metadata = self.process_curl_request(url)
        # terminal.tprint(json.dumps(form_metadata), 'warn')
        if len(form_metadata) != 0:
            # we need to process the metadata, especially the csv files which have been added
            self.process_form_metadata(form_metadata, form_id)
        # sys.exit()
        
        return self.all_nodes, form_structure

    def extract_repeating_groups(self, nodes, parent_id):
        """
        Process a node and get the repeating groups
        """
        cur_node = []
        for node in nodes['children']:
            if 'type' in node:
                if 'label' in node:
                    node_label = node['label']
                else:
                    terminal.tprint("\t\t%s missing label. Using name('%s') instead" % (node['type'], node['name']), 'warn')
                    node_label = node['name']

                if node['type'] == 'repeat' or node['type'] == 'group':
                    terminal.tprint("\tProcessing %s" % node_label, 'okblue')
                    # only add a node when we are dealing with a repeat
                    if node['type'] == 'repeat':
                        self.cur_node_id += 1
                        t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name'], 'items': []}
                    else:
                        t_node = None

                    if 'children' in node:
                        # terminal.tprint("\t%s-%s has %d children" % (node['type'], node_label, len(node['children'])), 'ok')
                        self.repeat_level += 1
                        # determine parent_id. If we are in a group, pass the current parent_id, else pass the cur_node_id
                        t_parent_id = self.cur_node_id if node['type'] == 'repeat' else parent_id
                        child_node = self.extract_repeating_groups(node, t_parent_id)

                        if len(child_node) != 0:
                            if t_node is None:
                                # we have something to save yet it wasn't wrapped in a repeat initially
                                # self.cur_node_id += 1
                                # terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(child_node[0])), 'warn')
                                t_node = child_node[0]
                            else:
                                t_node['items'].append(child_node[0])
                    # else:
                        # this node has no children. If its a top level node, include it in the top level page
                    #    if self.repeat_level == 0:

                    if t_node is not None and node['type'] == 'repeat':
                        if 'items' in t_node and len(t_node['items']) == 0:
                            del t_node['items']
                        cur_node.append(t_node)
                        # terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(t_node)), 'warn')
                        self.add_to_all_nodes(t_node)
                else:
                    # before anything, add this node to the dictionary
                    if node['type'] != 'calculate':
                        self.add_dictionary_items(node, node['type'])

                    # if self.repeat_level == 0:
                    self.cur_node_id += 1
                    # terminal.tprint("\tAdding a top node child", 'ok')
                    t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name']}
                    self.all_nodes.append(t_node)
            else:
                # we possibly have the options, so add them to the dictionary
                self.add_dictionary_items(node, 'choice')

        self.repeat_level -= 1
        return cur_node

    def process_form_metadata(self, metadata, form_id):
        # loop through all the metadata and for each csv file, download it and then process it
        try:
            for form_md in metadata:
                if form_md['data_file_type'] == 'text/csv':
                    # download this guy
                    url = "%s/%s/%d.csv" % (self.server, self.metadata_uri, form_md['id'])
                    terminal.tprint("Fetching the csv file '%s'" % form_md['data_value'], 'header')
                    file_path = '%d_%s' % (form_md['id'], form_md['data_value'])
                    self.fetch_form_metadata(url, {'path_to_save': file_path})
                    
                    # now lets process the downloaded file
                    self.process_downloaded_file(file_path, form_id)

                    # if no error, the file is processed, now we delete it
                    os.remove(file_path)
        except Exception:
            sentry.captureException()

    def add_dictionary_items(self, node, node_type):
        # check if this key already exists
        dict_item = DictionaryItems.objects.filter(form_id=self.cur_form_id, t_key=node['name'])

        if dict_item.count() == 0:
            try:
                # terminal.tprint(json.dumps(node), 'warn')
                node_label = node['label'] if 'label' in node else node['name']
                dict_item = DictionaryItems(
                    form_id=self.cur_form_id,
                    t_key=node['name'],
                    t_type=node_type,
                    t_value=node_label
                )
                dict_item.publish()

                if 'type' in node:
                    if node['type'] == 'select one' or node['type'] == 'select all that apply':
                        if 'children' in node:
                            for child in node['children']:
                                self.add_dictionary_items(child, 'choice')
            except IntegrityError as e:
                # We can live with this
                terminal.tprint(str(e), 'okblue')
            except Exception as e:
                print((traceback.format_exc()))
                logger.debug(str(e))
                terminal.tprint(str(e), 'fail')
                raise Exception(str(e))

    def process_downloaded_file(self, input_file, form_id):
        with open(input_file, 'rt') as in_file:
            ptions_data = csv.DictReader(in_file, delimiter=',', quotechar='"')
            for row in ptions_data:
                try:
                    dict_item = DictionaryItems.objects.filter(form_id=form_id).filter(t_key=row['name'])
                    if dict_item.count() == 1:
                        # this options is already saved...
                        continue
                    dict_item = DictionaryItems(
                        form_id=form_id,
                        t_key=row['name'],
                        t_type='choice',
                        t_value=row['label']
                    )
                    dict_item.publish()
                except IntegrityError as e:
                    # We can live with this
                    terminal.tprint(str(e), 'okblue')
                except Exception as e:
                    terminal.tprint(str(e), 'fail')
                    raise

    def add_to_all_nodes(self, t_node):
        # add a node to the list of all nodes for creating the tree
        if 'items' in t_node:
            del t_node['items']

        if 'label' in t_node:
            if re.search(":$", t_node['label']) is not None:
                # in case the label was ommitted, use the name tag
                t_node['label'] = t_node['name']

        self.all_nodes.append(t_node)

    def delete_folder_contents(self, folder_path):
        """
        Given a path to a folder, delete its contents
        """
        for filename in os.listdir(folder_path):
            if filename == '.' or filename == '..':
                    continue
            terminal.tprint("Deleting '%s'" % folder_path + os.sep + filename, 'fail')
            os.unlink(folder_path + os.sep + filename)

    def save_user_view(self, form_id, view_name, nodes, all_submissions, structure):
        """
        Given a view with a section of the user defined data, create a view of the selected nodes
        """
        # get a proper view name
        prop_view_name = self.formulate_view_name(view_name)

        # save the submissions as an excel an then call a function to create the table(s)
        # create a temp dir for this
        if os.path.exists(prop_view_name):
            self.delete_folder_contents(prop_view_name)
        else:
            # create the directory
            terminal.tprint("Create the directory '%s'" % prop_view_name, 'warn')
            os.makedirs(prop_view_name)

        writer = ExcelWriter(prop_view_name, 'csv', prop_view_name)
        writer.create_workbook(all_submissions, structure)
        terminal.tprint("\tFinished creating the csv files", 'warn')

        # now we have all our selected submissions as csv files, so process them
        import_command = "csvsql --db 'postgresql:///%s?user=%s&password=%s' --encoding utf-8 --blanks --insert --tables %s %s"
        table_views = []
        for filename in os.listdir(prop_view_name):
            if filename == '.' or filename == '..':
                continue

            basename = os.path.splitext(filename)[0]
            table_name = "%s_%s" % (prop_view_name, basename)
            table_name_hash = hashlib.md5(table_name)
            # terminal.tprint("Hashed the table name '%s'" % table_name, 'warn')
            table_name_hash_dig = "v_%s" % table_name_hash.hexdigest()
            print (table_name_hash_dig)
            terminal.tprint("Hashed the table name '%s' to '%s'" % (table_name, table_name_hash_dig), 'warn')

            filename = prop_view_name + os.sep + filename

            terminal.tprint("\tProcessing the file '%s' for saving to the database" % filename, 'okblue')
            if filename.endswith(".csv"):
                cmd = import_command % (
                    settings.DATABASES['default']['NAME'],
                    settings.DATABASES['default']['USER'],
                    settings.DATABASES['default']['PASSWORD'],
                    table_name_hash_dig,
                    filename,
                )
                terminal.tprint("\tRunning the command '%s'" % cmd, 'ok')
                print((subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()))

                # run commands to create primary key
                try:
                    with connection.cursor() as cursor:
                        logging.debug("Adding a primary ket constraint for the table '%s'" % table_name)
                        query = "alter table %s add primary key (%s)" % (table_name_hash_dig, 'unique_id')
                        cursor.execute(query)

                        # if table name has a main on it, it must have a _uuid field which should be unique
                        if re.search("main$", table_name) is not None:
                            # this is finicky, omit it for now
                            terminal.tprint("Not adding a unique constraint for column '_uuid'", 'fail')
                            # logging.debug("Adding unique constraint '%s' for the table '%s'" % ('_uuid', table_name))
                            # uquery = "alter table %s add constraint %s_%s unique (%s)" % (table_name_hash_dig, table_name_hash_dig, 'uuid', '_uuid')
                            # cursor.execute(uquery)
                        else:
                            # for the other tables, add an index to top_id
                            logging.debug("Adding indexes to '%s' and '%s' for the table '%s'" % ('top_id', 'parent_id', table_name))
                            uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'top_id', table_name_hash_dig, 'top_id')
                            cursor.execute(uquery)
                            uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'parent_id', table_name_hash_dig, 'parent_id')
                            cursor.execute(uquery)
                except Exception as e:
                    logging.error("For some reason can't create a primary key or unique key, raise an error and delete the view")
                    logging.error(str(e))
                    with connection.cursor() as cursor:
                        dquery = "drop table %s" % table_name_hash_dig
                        cursor.execute(dquery)
                    raise Exception("For some reason I can't create a primary key or unique key for the table %s. Deleting it entirely" % table_name)

                table_views.append({'table_name': table_name, 'hashed_name': table_name_hash_dig})

        # clean up process
        # delete the generated files
        self.delete_folder_contents(prop_view_name)
        os.rmdir(prop_view_name)

        form_view = FormViews.objects.filter(view_name=view_name)
        odk_form = ODKForm.objects.get(form_id=form_id)

        if form_view.count() == 0:
            # save the new view
            form_view = FormViews(
                form=odk_form,
                view_name=view_name,
                proper_view_name=prop_view_name,
                structure=nodes
            )
            form_view.publish()

            # save these submissions to the database
            for submission in all_submissions:
                new_submission = ViewsData(
                    view=form_view,
                    raw_data=submission
                )
                new_submission.publish()
        else:
            logger.error("Duplicate view name '%s'. Can't save." % view_name)
            # raise Exception("Duplicate view name '%s'. Can't save." % view_name)
            # return

        # add the tables to the lookup table of views
        for view in table_views:
            cur_view = ViewTablesLookup(
                view=form_view,
                table_name=view['table_name'],
                hashed_name=view['hashed_name']
            )
            cur_view.publish()

    def formulate_view_name(self, view_name):
        """
        Formulate a proper view name that will be used as the view name in the database
        """
        # convert all to lowercase
        view_name = view_name.lower()

        # convert non alpha numeric characters to spaces
        view_name = re.sub(r"[^a-zA-Z0-9]+", '_', view_name)
        form_group = re.sub(r"[^a-zA-Z0-9]+", '_', self.form_group)

        # create a unique view name
        view_name = "%s_%s" % (form_group, view_name)
        return view_name

    def formulate_db_name(self, form_name):
        # convert all to lowercase
        db_name = form_name.lower()
        db_name = db_name.replace('.', '_')
        return db_name

    def fetch_merge_data(self, form_id, nodes, d_format, download_type, view_name):
        """
        Given a form id and nodes of interest, get data from all associated forms
        """

        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        associated_forms = []
        try:
            # get all the form ids belonging to the same group
            self.form_group = settings.get('id_' + str(form_id), 'form_group')
            for section in settings.sections():
                this_group = settings.get(section, 'form_group')
                if this_group == self.form_group:
                    m = re.findall("/?id_(\d+)$", section)
                    associated_forms.append(m[0])
                else:
                    # form_group section doesn't exist, so skip this
                    logger.info("Not interested in this form (%s), so skip it" % this_group)
                    continue
            form_name = settings.get(self.form_group, 'name')
        except Exception as e:
            # terminal.tprint("We didn't find the form with id %s. This functionality will be deprecated... Falling to default methods" % str(form_id), 'fail')
            # print(traceback.format_exc())
            # there is an error getting the associated forms, so get data from just one form
            # terminal.tprint(str(e), 'fail')
            associated_forms.append(form_id)
            form_name = "Form%s" % str(form_id)

        # having all the associated form ids, fetch the required data
        all_submissions = []

        # since we shall be merging similar forms as one, declare the indexes here
        self.cur_node_id = 0
        self.indexes = {}
        self.sections_of_interest = {}
        self.output_structure = {'main': ['unique_id']}
        self.indexes['main'] = 1

        for form_id in associated_forms:
            this_submissions = self.get_form_submissions_as_json(int(form_id), nodes)

            if this_submissions is None:
                continue
            else:
                if(isinstance(this_submissions, list)):
                    all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

        if len(all_submissions) == 0:
            terminal.tprint("The form (%s) has no submissions for download" % str(form_name), 'fail')
            logging.debug("The form (%s) has no submissions for download" % str(form_name))
            return {'is_downloadable': False, 'error': False, 'message': "The form (%s) has no submissions for download" % str(form_name)}

        # check if there is need to create a database view of this data
        if download_type == 'download_save':
            try:
                self.save_user_view(form_id, view_name, nodes, all_submissions, self.output_structure)
            except Exception as e:
                return {'is_downloadable': False, 'error': True, 'message': str(e)}
        elif download_type == 'submissions':
            return all_submissions

        # now we have all the submissions, create the Excel sheet
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        if d_format == 'xlsx':
            # now lets save the data to an excel file
            output_name = './' + form_name + '_' + now + '.xlsx'
            self.save_submissions_as_excel(all_submissions, self.output_structure, output_name)
            return {'is_downloadable': True, 'filename': output_name}

    def save_submissions_as_excel(self, submissions, structure, filename):
        writer = ExcelWriter(filename)
        writer.create_workbook(submissions, structure)

    def get_form_submissions_as_json(self, form_id, screen_nodes):
        # given a form id get the form submissions
        # if the screen_nodes is given, process and return only the subset of data in those forms

        submissions_list = self.get_all_submissions(form_id)

        if submissions_list is None or submissions_list.count() == 0:
            terminal.tprint("The form with id '%s' has no submissions returning as such" % str(form_id), 'fail')
            return None

        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        try:
            # get the fields to include as part of the form metadata
            form_meta = settings.get('id_' + str(form_id), 'metadata').split(',')
            self.pk_name = settings.get('id_' + str(form_id), 'pk_name')
            self.sk_format = settings.get('id_' + str(form_id), 'sk_name')
        except Exception:
            # terminal.tprint("Form settings haven't been defined - %s" % str(e), 'fail')
            # logger.info("The settings for the form id (%s) haven't been defined" % str(form_id))
            # logger.debug(e)
            form_meta = []
            self.pk_name = 'hh_id'

        if screen_nodes is not None:
            screen_nodes.extend(form_meta)
            screen_nodes.append('unique_id')
        # terminal.tprint(json.dumps(screen_nodes), 'warn')

        submissions = []
        for data in submissions_list:
            # data, csv_files = self.post_data_processing(data)
            pk_key = self.pk_name + str(self.indexes['main'])
            data = data['raw_data']
            data['unique_id'] = pk_key
            data = self.process_node(data, 'main', screen_nodes, False)

            submissions.append(data)
            self.indexes['main'] += 1

        return submissions

    def process_node(self, node, sheet_name, nodes_of_interest=None, add_top_id=True):
        # the sheet_name is the name of the sheet where the current data will be saved
        cur_node = {}

        for key, value in list(node.items()):
            # clean the key
            clean_key = self.clean_json_key(key)
            if clean_key == '_geolocation':
                continue

            # terminal.tprint("\t"+clean_key, 'okblue')
            if nodes_of_interest is not None:
                if clean_key not in nodes_of_interest:
                    continue

            # add this key to the sheet name
            if clean_key not in self.output_structure[sheet_name]:
                self.output_structure[sheet_name].append(clean_key)

            if clean_key in self.country_qsts:
                value = self.get_clean_country_code(value)

            is_json = True
            val_type = self.determine_type(value)

            if val_type == 'is_list':
                value = self.process_list(value, clean_key, node['unique_id'])
                is_json = False
            elif val_type == 'is_json':
                is_json = True
            elif val_type == 'is_zero':
                is_json = False
                value = 0
            elif val_type == 'is_none':
                # terminal.tprint(key, 'warn')
                is_json = False
                value = 'N/A'
            else:
                is_json = False

            if is_json is True:
                node_value = self.process_node(value, clean_key, nodes_of_interest)
                cur_node[clean_key] = node_value
            else:
                node_value = value
                cur_node[clean_key] = value

            """
            if nodes_of_interest is not None:
                # at this point, we have our data, no need to check if we have the right key
                terminal.tprint("\tAdding the processed node (%s)" % clean_key, 'ok')
                if clean_key not in self.sections_of_interest:
                    self.sections_of_interest[clean_key] = []

                if isinstance(node_value, list):
                    for node_item in node_value:
                        self.sections_of_interest[clean_key].append(node_item)
                else:
                    self.sections_of_interest[clean_key].append(node_value)
            """
            if add_top_id is True:
                cur_node['top_id'] = self.pk_name + str(self.indexes['main'])

        return cur_node

    def determine_type(self, input):
        """
        determine the input from the user

        @todo, rely on the xls form to get the input type
        """
        try:
            float(input) + 2
        except Exception:
            if isinstance(input, list) is True:
                return 'is_list'
            elif input is None:
                return 'is_none'
            elif isinstance(input, dict) is True:
                return 'is_json'
            elif input == '0E-10':
                return 'is_zero'
            else:
                try:
                    json.loads(input)
                except ValueError:
                    if isinstance(input, str) is True:
                        return 'is_string'

                    # terminal.tprint(str(input), 'fail')
                    return 'is_none'
                except Exception:
                    # try encoding the input as string
                    try:
                        json.loads(str(input))
                    except ValueError:
                        return 'is_json'
                    except Exception:
                        # terminal.tprint(json.dumps(input), 'fail')
                        return 'is_none'
                    return 'is_json'
                return 'is_json'

        return 'is_int'

    def process_list(self, list, sheet_name, parent_key):
        # at times the input is a string and not necessary a json object

        # the sheet name is where to put this subset of data
        if sheet_name not in self.output_structure:
            self.output_structure[sheet_name] = ['unique_id', 'top_id', 'parent_id']
            self.indexes[sheet_name] = 1

        cur_list = []
        for node in list:
            val_type = self.determine_type(node)
            node['unique_id'] = sheet_name + '_' + str(self.indexes[sheet_name])

            if val_type == 'is_json':
                processed_node = self.process_node(node, sheet_name)
                processed_node['parent_id'] = parent_key
                cur_list.append(processed_node)
            elif val_type == 'is_list':
                processed_node = self.process_list(node, sheet_name, node['unique_id'])
                cur_list.append(processed_node)
            else:
                cur_list.append(node)

            self.indexes[sheet_name] += 1

        return cur_list

    def post_data_processing(self, data, csv_files):
        new_data = {}
        for key, node in list(data.items()):
            if isinstance(node, list) is True:
                if key not in csv_files:
                    csv_files[key] = []

        return (new_data, csv_files)

    def clean_json_key(self, j_key):
        # given a key from ona with data, get the sane(last) part of the key
        m = re.findall("/?(\w+)$", j_key)
        return m[0]

    def get_clean_country_code(self, code):
        if self.clean_country_codes is None:
            terminal.tprint('Adding the list of country codes', 'okblue')
            self.clean_country_codes = {}

            try:
                # get the country codes to clean
                settings = ConfigParser()
                settings.read(self.forms_settings)
                country_codes = settings.items('countries')
                for country, c_code in country_codes:
                    if re.search(",", c_code) is not None:
                        c_code = c_code.split(',')
                        for t_code in c_code:
                            self.clean_country_codes[t_code] = country
                    else:
                        self.clean_country_codes[c_code] = country

            except Exception as e:
                terminal.tprint(str(e), 'fail')
                return code

        # it seems we have our countries processed, just get the clean code
        try:
            if code in self.clean_country_codes:
                return self.clean_country_codes[code]
            else:
                for c_code, country in list(self.clean_country_codes.items()):
                    if re.search(c_code, code, re.IGNORECASE) is not None:
                        return country

                # if we are still here, the code wasnt found
                terminal.tprint("Couldn't find (%s) in the settings" % code, 'fail')
                # terminal.tprint(c_code + '--' + country, 'okblue')
                print((self.clean_country_codes))
                print('')
                return code
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            return code

    def process_single_submission(self, node, watch_list):
        # given a node full of submission and a watchlist,
        # retrieve the datasets whose key is in the watchlist
        return node

    def process_curl_request(self, url):
        """
        Create and execute a curl request

        @todo deprecated since it the function has been moved to onadata class
        """
        headers = {'Authorization': "Token %s" % self.api_token}
        # terminal.tprint("Processing API request %s" % url, 'okblue')
        try:
            r = requests.get(url, headers=headers)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return None

        if r.status_code == 200:
            # print(r)
            # terminal.tprint("Response %d" % r.status_code, 'ok')
            # terminal.tprint(json.dumps(r.json()), 'warn')
            return r.json()
        else:
            terminal.tprint("Response %d" % r.status_code, 'fail')
            terminal.tprint(r.text, 'fail')
            # terminal.tprint(url, 'warn')

            return None

    def fetch_form_metadata(self, url, download_properties):
        # stream the download and save the file to the specified place
        try:
            headers = {'Authorization': "Token %s" % self.api_token}
            r = requests.get(url, headers=headers, stream=True)

            if r.status_code == 200:
                # print(r.content)
                f = open(download_properties['path_to_save'], 'wt')
                f.write(r.content.decode('utf-8'))
            else:
                terminal.tprint("Response %d" % r.status_code, 'fail')
                raise SuspiciousOperation('File download failed with the status code %d' % r.status_code)
        except Exception:
            sentry.captureException()
            raise

    def get_views_info(self):
        form_views = FormViews.objects.all()

        all_data = {'views': []}
        for form_view in form_views:
            views_sub_table = ViewTablesLookup.objects.filter(view_id=form_view.id)
            view_date = form_view.date_created.strftime("%Y-%m-%d")
            all_data['views'].append({
                'view_id': form_view.id,
                'view_name': form_view.view_name,
                'date_created': view_date,
                'no_sub_tables': views_sub_table.count(),
                'auto_process': 'Yes'
            })
        return all_data

    def delete_view(self, request):
        view = json.loads(request.POST['view'])
        view_id = int(view['view_id'])
        try:
            # first delete the records in the views_table
            view_tables = ViewTablesLookup.objects.filter(view_id=view_id)
            for fview in view_tables:
                # delete the table
                logging.error("Drop the table '%s' in the view '%s'" % (fview.hashed_name, view['view_id']))
                with connection.cursor() as cursor:
                    # delete the actual view itself
                    dquery = "drop table %s" % fview.hashed_name
                    cursor.execute(dquery)
                # now delete the record
                fview.delete()

            # delete the view record in the database
            ViewsData.objects.filter(view_id=view_id).delete()
            FormViews.objects.filter(id=view_id).delete()
            return {'error': False, 'message': 'View deleted successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return {'error': True, 'message': str(e)}

    def edit_view(self, request):
        try:
            view = json.loads(request.POST['view'])
            # delete the actual view itself
            form_view = FormViews.objects.get(id=view['view_id'])
            form_view.view_name = view['view_name']
            # form_view.auto_process = view['auto_process']
            form_view.publish()

            return {'error': False, 'message': 'View edited successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return {'error': True, 'message': str(e)}

    def system_stats(self):
        """
        Gets the system statistics
        """
        with connection.cursor() as cursor:
            cursor.execute("select count(id) from syndromic_incidences as a")
            data_count = cursor.fetchall()

            locations_q = """
                SELECT round(latitude, 2) as lat, round(longitude, 2) as lng, count(*) as count
                FROM syndromic_incidences
                GROUP BY round(latitude, 2), round(longitude, 2)
            """
            cursor.execute(locations_q)
            locations = cursor.fetchall()

            incidences_last_week_q = """
                SELECT count(*) as ct
                FROM syndromic_incidences
                WHERE EXTRACT(DAY FROM (now() - datetime_reported)) > 7 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
            """ % self.period_days
            cursor.execute(incidences_last_week_q)
            incidences_last_week = cursor.fetchall()

            incidences_this_week_q = """
                SELECT count(*) as ct
                FROM syndromic_incidences
                WHERE EXTRACT(DAY FROM (now() - datetime_reported)) < %d
            """ % self.period_days
            cursor.execute(incidences_this_week_q)
            incidences_this_week = cursor.fetchall()

            affected_species_this_week_q = """
                SELECT count(*) as ct, b.species
                FROM syndromic_incidences as a
                INNER JOIN syndromic_details as b on b.incidence_id = a.id
                WHERE a.no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY b.species
            """ % self.period_days
            cursor.execute(affected_species_this_week_q)
            affected_species_this_week = cursor.fetchall()

            affected_species_last_week_q = """
                SELECT count(*) as ct, b.species
                FROM syndromic_incidences as a
                INNER JOIN syndromic_details as b on b.incidence_id = a.id
                WHERE a.no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY b.species
            """ % self.period_days
            cursor.execute(affected_species_last_week_q)
            affected_species_last_week = cursor.fetchall()

            affected_subcounties_last_week_q = """
                SELECT count(*) as ct, sub_county
                FROM syndromic_incidences
                WHERE no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY sub_county
            """ % self.period_days
            cursor.execute(affected_subcounties_last_week_q)
            affected_subcounties_last_week = cursor.fetchall()

            affected_subcounties_this_week_q = """
                SELECT count(*) as ct, sub_county
                FROM syndromic_incidences
                WHERE no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY sub_county
            """ % self.period_days
            cursor.execute(affected_subcounties_this_week_q)
            affected_subcounties_this_week = cursor.fetchall()

            reporting_vets_last_week_q = """
                SELECT count(*) as ct, reporter
                FROM syndromic_incidences
                WHERE no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY reporter
            """ % self.period_days
            cursor.execute(reporting_vets_last_week_q)
            reporting_vets_last_week = cursor.fetchall()

            reporting_vets_this_week_q = """
                SELECT count(*) as ct, reporter
                FROM syndromic_incidences
                WHERE no_cases > 0 AND EXTRACT(DAY FROM (now() - datetime_reported)) < %d
                GROUP BY reporter
            """ % self.period_days
            cursor.execute(reporting_vets_this_week_q)
            reporting_vets_this_week = cursor.fetchall()

        # terminal.tprint('Incidences', 'fail')
        inc_this_week = self.proccess_submissions_count([[len(incidences_this_week)]])
        inc_last_week = self.proccess_submissions_count([[len(incidences_last_week)]])
        inc_change = "{0:0.1f}".format((((inc_this_week - inc_last_week) / inc_last_week)) * 100)
        inc_change_class = 'fa-level-up' if float(inc_change) > 0.0 else 'fa-level-down'
        # terminal.tprint(inc_change, 'ok')

        # terminal.tprint('Species', 'fail')
        species_this_week = self.proccess_submissions_count(affected_species_this_week)
        species_last_week = self.proccess_submissions_count(affected_species_last_week)
        species_change = "{0:0.1f}".format((((species_this_week - species_last_week) / species_last_week)) * 100)
        species_change_class = 'fa-level-up' if float(species_change) > 0 else 'fa-level-down'
        # terminal.tprint(species_change, 'ok')

        # terminal.tprint('Sub Counties', 'fail')
        subcounties_this_week = self.proccess_submissions_count(affected_subcounties_this_week)
        subcounties_last_week = self.proccess_submissions_count(affected_subcounties_last_week)
        subcounties_change = "{0:0.1f}".format((((subcounties_this_week - subcounties_last_week) / subcounties_last_week)) * 100)
        subcounties_change_class = 'fa-level-up' if float(subcounties_change) > 0 else 'fa-level-down'
        # terminal.tprint(subcounties_change, 'ok')

        # terminal.tprint('Vets', 'fail')
        vets_this_week = self.proccess_submissions_count(reporting_vets_this_week)
        vets_last_week = self.proccess_submissions_count(reporting_vets_last_week)
        vets_change = "{0:0.1f}".format((((vets_this_week - vets_last_week) / vets_last_week)) * 100)
        vets_change_class = 'fa-level-up' if float(vets_change) > 0 else 'fa-level-down'
        # terminal.tprint(vets_change, 'ok')

        all_incidences = {}
        all_incidences['cattle'] = self.get_livestock_incidences('cattle')
        all_incidences['sheep'] = self.get_livestock_incidences('sheep')
        all_incidences['goats'] = self.get_livestock_incidences('goats')
        all_incidences['camels'] = self.get_livestock_incidences('camels')

        incidences = {
            'count': "{0:0.0f}".format(inc_this_week),
            'last_week_change': inc_change,
            'last_week_change_class': inc_change_class,
            'species_count': "{0:0.0f}".format(species_this_week),
            'last_week_species_change': species_change,
            'last_week_species_change_class': species_change_class,
            'sub_counties_count': "{0:0.0f}".format(subcounties_this_week),
            'last_week_subcounties_change': subcounties_change,
            'last_week_subcounties_change_class': subcounties_change_class,
            'vets_reporting': "{0:0.0f}".format(vets_this_week),
            'last_week_vet_change': vets_change,
            'last_week_vet_change_class': vets_change_class
        }
        all_locations = []
        if len(locations) != 0:
            center = {'lat': "{0:0.2f}".format(locations[0][0]), 'lng': "{0:0.2f}".format(locations[0][1])}
        else:
            center = {'lat': "3.040", 'lng': "35.567"}

        for loc in locations:
            all_locations.append({'lat': "{0:0.2f}".format(loc[0]), 'lng': "{0:0.2f}".format(loc[1]), 'count': int(loc[2])})

        to_return = {
            'data_count': int(data_count[0][0]),
            'locations': all_locations,
            'center_point': center,
            'incidences': incidences,
            'all_incidences': all_incidences,
            'week_syndromes': self.get_syndromes_freq(7),
            'month_syndromes': self.get_syndromes_freq(31),
            'last_week': self.last_week_stats(),
            'sc': self.last_week_sc_reports()[0]
        }
        return to_return

    def last_week_stats(self):
        # get the stats based on the last 7 days reports
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) as ct FROM raw_submissions WHERE EXTRACT(DAY FROM (now() - date_created)) < %d" % self.period_days)
            submissions = cursor.fetchall()

            cursor.execute("SELECT count(*) as ct FROM syndromic_incidences as a inner join syndromic_details as b on b.incidence_id = a.id WHERE EXTRACT(DAY FROM (now() - datetime_reported)) < %d" % self.period_days)
            incidences = cursor.fetchall()

            cursor.execute("SELECT count(sub_county) as ct FROM syndromic_incidences WHERE EXTRACT(DAY FROM (now() - date_created)) < %d GROUP BY sub_county" % self.period_days)
            sub_county = cursor.fetchall()

            cursor.execute("SELECT count(reporter) as ct FROM syndromic_incidences WHERE EXTRACT(DAY FROM (now() - date_created)) < %d group by reporter" % self.period_days)
            cdrs = cursor.fetchall()

        last_week = {}
        last_week['reports_count'] = self.proccess_submissions_count(submissions)
        last_week['incidences_count'] = self.proccess_submissions_count(incidences, True)
        last_week['sub_counties_count'] = len(sub_county)
        last_week['cdrs_reporting'] = len(cdrs)

        return last_week

    def last_week_sc_reports(self):
        # get the number of reports received by subcounties in the last week
        # grouping = '' if group_by_species is False else 'GROUP BY a.sub_county, b.species'
        with connection.cursor() as cursor:
            query = """
                SELECT sum(no_cases) as ct, a.sub_county, sum(no_sick) as sick, sum(no_dead) as dead
                FROM syndromic_incidences as a
                INNER JOIN syndromic_details as b on b.incidence_id = a.id
                WHERE EXTRACT(DAY FROM (now() - a.date_created)) < %d
                GROUP BY a.sub_county
            """ % self.period_days
            cursor.execute(query)
            # terminal.tprint(query, 'fail')
            sub_counties = cursor.fetchall()

        cases = {}
        sick = {}
        dead = {}
        for sub_county in sub_counties:
            if sub_county[1] not in self.sub_counties:
                continue
            val = self.proccess_submissions_count([sub_county], True)
            cases[str(sub_county[1])] = "{:,}".format(val)
            sick[str(sub_county[1])] = "{:,}".format(int(sub_county[2]))
            dead[str(sub_county[1])] = "{:,}".format(int(sub_county[3]))

        for sub_county in self.sub_counties:
            if sub_county not in cases:
                cases[sub_county] = 0
            if sub_county not in sick:
                sick[sub_county] = 0
            if sub_county not in dead:
                dead[sub_county] = 0

        return (cases, sick, dead)

    def last_week_sc_reports_by_species(self):
        # get the number of reports received by subcounties in the last week
        # grouping = '' if group_by_species is False else 'GROUP BY a.sub_county, b.species'
        with connection.cursor() as cursor:
            query = """
                SELECT sum(no_cases) as ct, a.sub_county, sum(no_sick) as sick, sum(no_dead) as dead, b.species
                FROM syndromic_incidences as a
                INNER JOIN syndromic_details as b on b.incidence_id = a.id
                WHERE EXTRACT(DAY FROM (now() - a.date_created)) < %d
                GROUP BY a.sub_county, b.species
            """ % self.period_days
            cursor.execute(query)
            # terminal.tprint(query, 'fail')
            sub_counties = cursor.fetchall()

        all_cases = {}
        for sub_county in sub_counties:
            if sub_county[1] not in self.sub_counties:
                continue
            val = self.proccess_submissions_count([sub_county], True)
            sc_name = str(sub_county[1]).capitalize()
            specie = str(sub_county[4]).capitalize()
            all_cases[sc_name] = {}
            all_cases[sc_name][specie] = {}
            all_cases[sc_name][specie]['cases'] = "{:,}".format(math.log(val))
            sick = math.log(int(sub_county[2])) if int(sub_county[2]) != 0 else 0
            all_cases[sc_name][specie]['sick'] = "{:,}".format(sick)
            dead = math.log(int(sub_county[3])) if int(sub_county[3]) != 0 else 0
            all_cases[sc_name][specie]['dead'] = "{:,}".format(dead)
            all_cases[sc_name][specie]['cases_r'] = "{:,}".format(val)
            all_cases[sc_name][specie]['sick_r'] = "{:,}".format(int(sub_county[2]))
            all_cases[sc_name][specie]['dead_r'] = "{:,}".format(int(sub_county[3]))

        for sub_county in self.sub_counties:
            sub_county = sub_county.capitalize()
            if sub_county not in all_cases:
                all_cases[sub_county] = {}
            for specie in self.all_species:
                specie = specie.capitalize()
                if specie not in all_cases[sub_county]:
                    all_cases[sub_county][specie] = {}
                    for i_type in ['cases', 'sick', 'dead']:
                        if i_type not in all_cases[sub_county][specie]:
                            all_cases[sub_county][specie][i_type] = 0
                    for i_type in ['cases_r', 'sick_r', 'dead_r']:
                        if i_type not in all_cases[sub_county][specie]:
                            all_cases[sub_county][specie][i_type] = 0

        return all_cases

    def proccess_submissions_count(self, s_count, use_zero=False):
        # terminal.tprint(json.dumps(s_count), 'warn')

        to_return = 0
        if len(s_count) == 0:
            to_return = 0.001
        elif s_count[0][0] is None:
            to_return = 0.001
        else:
            to_return = int(s_count[0][0]) if len(s_count) != 0 else 0.001

        if to_return == 0.001:
            if use_zero is True:
                to_return = 0

        return to_return

    def get_livestock_incidences(self, specie):
        with connection.cursor() as cursor:
            incidences_q = """
                SELECT start_date, sum(no_sick), sum(no_dead)
                FROM syndromic_details
                WHERE species = '%s'
                GROUP BY start_date
                ORDER by start_date
            """ % specie
            cursor.execute(incidences_q)
            all_incidences = cursor.fetchall()

        labels = []
        fatalities = []
        incidences = []

        for inc in all_incidences:
            labels.append(str(inc[0]))
            incidences.append(int(inc[1]))
            fatalities.append(int(inc[2]))

        return {'labels': labels, 'incidences': incidences, 'fatalities': fatalities}

    def get_syndromes_freq(self, no_days, species=None):
        if species is not None:
            species_sub = " and species in (%s)" % ', '.join("'{0}'".format(w) for w in species)
        else:
            species_sub = ''
        with connection.cursor() as cursor:
            syndromes_q = """
                SELECT clinical_signs
                FROM syndromic_details
                WHERE EXTRACT(DAY FROM (now() - start_date)) <= %d %s
            """ % (no_days, species_sub)
            cursor.execute(syndromes_q)
            syndromes = cursor.fetchall()

        all_syndromes = []
        all_syndromes_freq = {}
        for synd in syndromes:
            t_synds = re.findall("(\w+)", synd[0])
            for t_synd in t_synds:
                # all_syndromes.append(self.get_value_from_dictionary(t_synd))
                t_synd = str(t_synd)
                if t_synd not in all_syndromes_freq:
                    all_syndromes_freq[t_synd] = 0

                all_syndromes_freq[t_synd] += 1

        for t_synd, freq in list(all_syndromes_freq.items()):
            tt_synd = str(self.get_value_from_dictionary(t_synd))
            all_syndromes.append({'text': tt_synd, 'size': freq * 8})

        # terminal.tprint(json.dumps(all_syndromes), 'warn')
        return all_syndromes

    def get_diseases_freq(self, no_days, species=None):
        if species is not None:
            species_sub = " and species in (%s)" % ', '.join("'{0}'".format(w) for w in species)
        else:
            species_sub = ''
        with connection.cursor() as cursor:
            diseases_q = """
                SELECT prov_diagnosis
                FROM syndromic_details
                WHERE EXTRACT(DAY FROM (now() - start_date)) <= %d %s
            """ % (no_days, species_sub)
            cursor.execute(diseases_q)
            # terminal.tprint(diseases_q, 'warn')
            diseases = cursor.fetchall()

        all_diseases = []
        all_diseases_freq = {}
        for synd in diseases:
            t_synds = re.findall("(\w+)", synd[0])
            for t_synd in t_synds:
                # all_diseases.append(self.get_value_from_dictionary(t_synd))
                t_synd = str(t_synd)
                if t_synd not in all_diseases_freq:
                    all_diseases_freq[t_synd] = 0

                all_diseases_freq[t_synd] += 1

        for t_synd, freq in list(all_diseases_freq.items()):
            tt_synd = str(self.get_value_from_dictionary(t_synd))
            all_diseases.append({'text': tt_synd, 'size': freq * 8})

        # terminal.tprint(json.dumps(all_diseases), 'warn')
        return all_diseases

    def get_biweekly_report(self):
        # get the statistics for the biweekly report
        self.period_days = 14
        small_ruminant_syndromes = self.get_syndromes_freq(self.period_days, ['goats', 'sheep'])
        cattle_syndromes = self.get_syndromes_freq(self.period_days, ['cattle'])

        small_ruminant_diseases = self.get_diseases_freq(self.period_days, ['goats', 'sheep'])
        cattle_diseases = self.get_diseases_freq(self.period_days, ['cattle'])

        # get the overall reports for all the species
        (cases, sick, dead) = self.last_week_sc_reports()
        # get the reports grouped by species
        all_cases = self.last_week_sc_reports_by_species()

        d1 = date(2017, 10, 1)
        d2 = date.today()
        monday1 = (d1 - timedelta(days=d1.weekday()))
        monday2 = (d2 - timedelta(days=d2.weekday()))

        no_biweekly = (monday2 - monday1).days / 14
        report_no = ordinal(no_biweekly + 1)

        return {
            'small_ruminant_syndromes': small_ruminant_syndromes,
            'cattle_syndromes': cattle_syndromes,
            'small_ruminant_diseases': small_ruminant_diseases,
            'cattle_diseases': cattle_diseases,
            'sc': cases,
            'sick': sick,
            'dead': dead,
            'report_no': report_no,
            'by_species': all_cases,
            'sub_counties': [county.capitalize() for county in self.sub_counties],
            'all_species': [specie.capitalize() for specie in self.all_species]
        }

    def system_stats_v2(self, inputs):
        all_subcounties = []
        for sc_code in self.sub_counties:
            all_subcounties.append({'code': sc_code, 'label': str(self.get_value_from_dictionary(sc_code))})

        all_species = []
        for sp_code in self.all_species:
            all_species.append({'code': sp_code, 'label': str(self.get_value_from_dictionary(sp_code))})

        # print inputs.refresh_type
        if len(inputs) == 0:
            # print('using defaults')
            # we don't have anything defined, get the defaults
            end_date = date.today() + timedelta(days=1)
            start_date = end_date - timedelta(days=self.period_days)
            subcounties_search = self.sub_counties
            species_search = self.all_species
            period = 'past_6mo'
        else:
            # if 'refresh_type' not in inputs or inputs['refresh_type'] is not 'all':
            # print('get a subset')
            # get the period we are interested in
            if 'refresh_type' not in inputs:
                end_date = date.today()
                start_date = end_date - timedelta(days=self.r_period)
                period = 'past_6mo'
            else:
                if inputs['refresh_type'] == '':
                    # compute the selected period
                    dates = inputs['range'].split(' - ')
                    if len(dates) != 2:
                        raise ValueError('The date range must be specified.')

                    # strip the dates and then format them
                    start_date = datetime.strptime(dates[0], '%m/%d/%Y').strftime('%Y-%m-%d')
                    end_date = datetime.strptime(dates[1], '%m/%d/%Y').strftime('%Y-%m-%d')
                    period = 'custom'
                else:
                    self.get_r_period(inputs['refresh_type'])
                    end_date = date.today()
                    start_date = end_date - timedelta(days=self.period_days)
                    period = inputs['refresh_type']

            # get the species of interest
            if len(inputs['species']) == 0:
                species_search = self.all_species
            else:
                species_search = [inputs['species']]

            # get the subcounties of interest
            if len(inputs['subcounties']) == 0:
                subcounties_search = self.sub_counties
            else:
                subcounties_search = [inputs['subcounties']]

        # add an empty string if we have an array of 1
        subcounties_search.append('') if len(subcounties_search) == 1 else subcounties_search
        species_search.append('') if len(species_search) == 1 else species_search
        # print(subcounties_search)
        # print(species_search)
        extra_data = self.dash_stats(start_date, end_date, subcounties_search, species_search)

        to_return = {
            'sub_counties': all_subcounties,
            'species': all_species,
            'sel_range': "%s - %s" % (datetime.strptime(str(start_date), '%Y-%m-%d').strftime('%m-%d-%Y'), datetime.strptime(str(end_date), '%Y-%m-%d').strftime('%m-%d-%Y')),
            'from': datetime.strptime(str(start_date), '%Y-%m-%d').strftime('%m-%d-%Y'),
            'to': datetime.strptime(str(end_date), '%Y-%m-%d').strftime('%m-%d-%Y'),
            'period': str(period)
        }
        to_return.update(extra_data)

        return to_return

    def dash_stats(self, start_date, end_date, sub_county, species):
        # print(("Input params: startdate = %s, enddate = %s, sub_county = %s, species = %s" % (start_date, end_date, sub_county, species)))
        with connection.cursor() as cursor:
            submissions_q = """
                SELECT date(datetime_reported) as r_date, count(*) as ct
                FROM syndromic_incidences as a INNER JOIN syndromic_details as b on a.id=b.incidence_id
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s AND b.species IN %s
                GROUP BY date(datetime_reported)
            """ % (str(start_date), str(end_date), tuple(sub_county), tuple(species))
            # print(submissions_q)
            cursor.execute(submissions_q)
            submissions = cursor.fetchall()

            all_submissions_q = """
                SELECT count(*) as ct FROM syndromic_incidences
            """
            cursor.execute(all_submissions_q)
            all_submissions = cursor.fetchall()
            all_submissions_count = int(all_submissions[0][0])

            # submission dates
            dates_q = """
                SELECT date(datetime_reported) as r_date
                FROM syndromic_incidences as a
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s
                GROUP BY date(datetime_reported) ORDER BY date(datetime_reported)
            """ % (str(start_date), str(end_date), tuple(sub_county))
            # print(dates_q)
            cursor.execute(dates_q)
            dates = cursor.fetchall()

            charts_dates = []
            for s_date in dates:
                charts_dates.append(str(s_date[0]))

            # get the reported mortalities
            mortalities_q = """
                SELECT date(datetime_reported) as r_date, count(*) as ct
                FROM syndromic_incidences as a INNER JOIN syndromic_details as b on a.id=b.incidence_id
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s AND b.no_dead > 0
                GROUP BY date(datetime_reported) ORDER BY date(datetime_reported)
            """ % (str(start_date), str(end_date), tuple(sub_county))
            # print(mortalities_q)
            cursor.execute(mortalities_q)
            mortalities = cursor.fetchall()

            all_mortalities = []
            total_mortalities = 0
            cur_date_index = 0
            for mort in mortalities:
                if str(mort[0]) != str(charts_dates[cur_date_index]):
                    while str(mort[0]) != str(charts_dates[cur_date_index]):
                        all_mortalities.append([charts_dates[cur_date_index], 0])
                        cur_date_index = cur_date_index + 1

                all_mortalities.append([str(mort[0]), int(mort[1])])
                total_mortalities = total_mortalities + int(mort[1])
                cur_date_index = cur_date_index + 1

            # zero reports
            zero_reports_q = """
                SELECT date(datetime_reported) as r_date, count(*) as ct
                FROM syndromic_incidences as a
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s AND no_cases = 0
                GROUP BY date(datetime_reported) ORDER BY date(datetime_reported)
            """ % (str(start_date), str(end_date), tuple(sub_county))
            # print(zero_reports_q)
            cursor.execute(zero_reports_q)
            zero_reports = cursor.fetchall()

            all_zero_reports = []
            total_zeroreports = 0
            cur_date_index = 0
            for zr in zero_reports:
                if str(zr[0]) != str(charts_dates[cur_date_index]):
                    while str(zr[0]) != str(charts_dates[cur_date_index]):
                        all_zero_reports.append([charts_dates[cur_date_index], 0])
                        cur_date_index = cur_date_index + 1

                all_zero_reports.append([str(zr[0]), int(zr[1])])
                total_zeroreports = total_zeroreports + int(zr[1])
                cur_date_index = cur_date_index + 1

            # submissions
            submissions_q = """
                SELECT date(datetime_reported) as r_date, count(*) as ct
                FROM syndromic_incidences as a INNER JOIN syndromic_details as b on a.id=b.incidence_id
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s
                GROUP BY date(datetime_reported) ORDER BY date(datetime_reported)
            """ % (str(start_date), str(end_date), tuple(sub_county))
            # print(submissions_q)
            cursor.execute(submissions_q)
            submissions = cursor.fetchall()

            all_submissions = []
            total_submissions = 0
            cur_date_index = 0
            for subm in submissions:
                if str(subm[0]) != str(charts_dates[cur_date_index]):
                    while str(subm[0]) != str(charts_dates[cur_date_index]):
                        all_submissions.append([charts_dates[cur_date_index], 0])
                        cur_date_index = cur_date_index + 1

                all_submissions.append([str(subm[0]), int(subm[1])])
                total_submissions = total_submissions + int(subm[1])
                cur_date_index = cur_date_index + 1

            # reported syndromes
            reported_syndromes = self.get_syndromes_freq_v2(start_date, end_date, species, sub_county)

            # sub counties reorting
            subcounties_reporting_q = """
                SELECT count(*) as ct, sub_county
                FROM syndromic_incidences
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND sub_county IN %s
                GROUP BY sub_county ORDER BY ct
            """ % (str(start_date), str(end_date), tuple(sub_county))
            cursor.execute(subcounties_reporting_q)
            subcounties_reporting = cursor.fetchall()

            total_reports = 0
            sub_county_reports = {'max': 0, 'min': 9999999, 'data': []}
            # my_scs = self.sub_counties
            for rec in subcounties_reporting:
                total_reports = total_reports + rec[0]
                sub_county_reports['data'].append({'feature_id': rec[1], 'value': rec[0], 'name': str(self.get_value_from_dictionary(rec[1]))})
                sub_county_reports['max'] = rec[0] if rec[0] > sub_county_reports['max'] else sub_county_reports['max']
                sub_county_reports['min'] = rec[0] if rec[0] < sub_county_reports['min'] else sub_county_reports['min']
                # my_scs.remove(rec[1])

            # add the missing subcounties
            my_scs = []
            for sc in my_scs:
                sub_county_reports['data'].append({'feature_id': sc, 'value': 0, 'name': str(self.get_value_from_dictionary(sc))})
            # reports
            reports_q = """
                SELECT date(datetime_reported) as r_date, sub_county, ward, syndrome, species, no_sick, no_dead, prov_diagnosis, latitude, longitude
                FROM syndromic_incidences as a INNER JOIN syndromic_details as b on a.id=b.incidence_id
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND sub_county IN %s AND species IN %s
                ORDER BY datetime_reported DESC
            """ % (str(start_date), str(end_date), tuple(sub_county), tuple(species))
            cursor.execute(reports_q)
            reports = cursor.fetchall()

            all_reports = []
            for rep in reports:
                all_reports.append({
                    'r_date': str(rep[0]),
                    'sub_county': str(self.get_value_from_dictionary(rep[1])),
                    'ward': str(self.get_value_from_dictionary(rep[2])),
                    'syndrome': str(self.get_value_from_dictionary(rep[3])),
                    'species': str(self.get_value_from_dictionary(rep[4])),
                    'no_sick': int(rep[5]),
                    'no_dead': int(rep[6]),
                    'prov_diagnosis': str(rep[7]),
                    'lat': str(rep[8]),
                    'long': str(rep[9])
                })

            no_subcounties_reporting = len(subcounties_reporting)
            if no_subcounties_reporting == 0:
                least_active_subcounty = 'NA'
                most_active_subcounty = 'NA'
                least_active_subcounty_records = 0
                most_active_subcounty_records = 0
                average_no_reports = 0
            else:
                least_active_subcounty = str(self.get_value_from_dictionary(subcounties_reporting[0][1]))
                most_active_subcounty = str(self.get_value_from_dictionary(subcounties_reporting[no_subcounties_reporting - 1][1]))
                least_active_subcounty_records = subcounties_reporting[0][0]
                most_active_subcounty_records = subcounties_reporting[no_subcounties_reporting - 1][0]
                average_no_reports = "{0:0.1f}".format(float(total_reports) / float(no_subcounties_reporting))

            # wards reorting
            wards_reporting_q = """
                SELECT count(*) as ct, ward
                FROM syndromic_incidences
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND sub_county IN %s
                GROUP BY ward ORDER BY ct
            """ % (str(start_date), str(end_date), tuple(sub_county))
            cursor.execute(wards_reporting_q)
            wards_reporting = cursor.fetchall()

            no_wards_reporting = len(wards_reporting)
            ward_reports = 0
            ward_min = 999999
            ward_max = -1
            for rec in wards_reporting:
                ward_reports = ward_reports + rec[0]
                if rec[0] > ward_max:
                    ward_max = rec[0]
                if rec[0] < ward_min:
                    ward_min = rec[0]

            if no_wards_reporting == 0:
                average_ward_no_reports = 0
            else:
                average_ward_no_reports = "{0:0.1f}".format(float(ward_reports) / float(no_wards_reporting))

            # cdrs reorting
            cdrs_reporting_q = """
                SELECT count(*) as ct, reporter
                FROM syndromic_incidences
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND sub_county IN %s
                GROUP BY reporter ORDER BY ct
            """ % (str(start_date), str(end_date), tuple(sub_county))
            cursor.execute(cdrs_reporting_q)
            cdrs_reporting = cursor.fetchall()

            no_cdr_reporting = len(cdrs_reporting)
            cdr_reports = 0
            for rec in cdrs_reporting:
                cdr_reports = cdr_reports + rec[0]

            if no_cdr_reporting == 0:
                average_cdr_reports = 0
            else:
                average_cdr_reports = "{0:0.1f}".format(float(cdr_reports) / float(no_cdr_reporting))

        total_incidences = total_submissions - total_zeroreports
        to_return = {
            'all_mortalities': all_mortalities,
            'all_zero_reports': all_zero_reports,
            'all_submissions': all_submissions,
            'total_submissions': total_submissions,
            'total_zeroreports': total_zeroreports,
            'total_mortalities': total_mortalities,
            'total_incidences': total_incidences,
            'reported_syndromes': reported_syndromes,
            'perc_submissions': '0' if total_submissions == 0 or all_submissions_count == 0 else "{0:0.1f}".format(((float(total_submissions) / float(all_submissions_count))) * 100),
            'perc_zeroreports': '0' if total_zeroreports == 0 or total_submissions == 0 else "{0:0.1f}".format(((float(total_zeroreports) / float(total_submissions))) * 100),
            'perc_incidences': '0' if total_incidences == 0 or total_submissions == 0 else "{0:0.1f}".format(((float(total_incidences) / float(total_submissions))) * 100),
            'perc_mortalities': '0' if total_mortalities == 0 or total_submissions == 0 else "{0:0.1f}".format(((float(total_mortalities) / float(total_submissions))) * 100),
            'subcounties_reporting': no_subcounties_reporting,
            'least_active_subcounty': least_active_subcounty,
            'most_active_subcounty': most_active_subcounty,
            'most_active_subcounty_records': int(most_active_subcounty_records),
            'least_active_subcounty_records': int(least_active_subcounty_records),
            'average_no_reports': average_no_reports,
            'average_ward_no_reports': average_ward_no_reports,
            'no_wards_reporting': no_wards_reporting,
            'no_cdr_reporting': no_cdr_reporting,
            'cdr_reports': int(cdr_reports),
            'average_cdr_reports': average_cdr_reports,
            'all_reports': all_reports,
            'charts_dates': charts_dates,
            'sub_county_reports': sub_county_reports
        }
        return to_return

    def get_syndromes_freq_v2(self, start_date, end_date, species=None, sub_counties=None):
        with connection.cursor() as cursor:
            syndromes_q = """
                SELECT b.clinical_signs
                FROM syndromic_incidences as a INNER JOIN syndromic_details as b on a.id=b.incidence_id
                WHERE datetime_reported > '%s' AND datetime_reported < '%s' AND a.sub_county IN %s AND species IN %s
            """ % (str(start_date), str(end_date), tuple(sub_counties), tuple(species))
            cursor.execute(syndromes_q)
            syndromes = cursor.fetchall()

        all_syndromes = []
        all_syndromes_freq = {}
        for synd in syndromes:
            t_synds = re.findall("(\w+)", synd[0])
            for t_synd in t_synds:
                # all_syndromes.append(self.get_value_from_dictionary(t_synd))
                t_synd = str(t_synd)
                if t_synd not in all_syndromes_freq:
                    all_syndromes_freq[t_synd] = 0

                all_syndromes_freq[t_synd] += 1

        for t_synd, freq in list(all_syndromes_freq.items()):
            tt_synd = str(self.get_value_from_dictionary(t_synd))
            all_syndromes.append({'text': tt_synd, 'size': freq * 8})

        # terminal.tprint(json.dumps(all_syndromes), 'warn')
        return all_syndromes

    def generate_nd_system_stats(self, inputs):
        """
        Generate the statistics to be used for the notifiable disease dashboard
        """
        # disease frequency
        nd_reporting_q = """
            SELECT date(date_trunc('week', nd_date_reported::date)) as weekly, disease, count(*)
            FROM nd_details as a INNER JOIN nd_reports as b on a.nd_report_id=b.id WHERE disease != 'unknown'
            GROUP BY weekly, disease
            ORDER BY weekly, disease
        """
        # when filtering use this criteria
        #  % (str(start_date), str(end_date), tuple(sub_county))
        # all reports
        nd_all_reports_q = """
            SELECT date(nd_date_reported), sub_county, ward, village, latitude, longitude, accuracy, nd_date_started, nd_date_reported, disease, species, diagnosis_type, production_system, is_zoonotic, no_risk, no_sick, no_dead, no_slaughtered, measure_taken, no_vaccinated, org_survey
            FROM nd_details as a INNER JOIN nd_reports as b on a.nd_report_id=b.id
            ORDER BY nd_date_reported DESC
        """

        #  % (str(start_date), str(end_date), tuple(sub_county), tuple(species))

        with connection.cursor() as cursor:
            cursor.execute(nd_reporting_q)
            nd_reporting = cursor.fetchall()

            cursor.execute(nd_all_reports_q)
            reports = cursor.fetchall()

        all_reports = []
        for rep in reports:
            all_reports.append({
                'upload_date': str(rep[0]),
                'sub_county': str(rep[1]),
                'ward': str(rep[2]),
                'village': str(rep[3]),
                'latitude': str(rep[4]),
                'longitude': str(rep[5]),
                'accuracy': str(rep[6]),
                'start_date': str(rep[7]),
                'report_date': str(rep[8]),
                'disease': str(rep[9]),
                'species': str(rep[10]),
                'type_diagnosis': self.get_value_from_dictionary(rep[11]),
                'prod_system': self.get_value_from_dictionary(rep[12]),
                'is_zoonotic': str(rep[13]),
                'no_risk': int(rep[14]),
                'no_sick': int(rep[15]),
                'no_dead': int(rep[16]),
                'no_slaughtered': int(rep[17]),
                'measures': self.get_value_from_dictionary(rep[18]),
                'no_vaccinated': 'N/A' if rep[19] is None else int(rep[19]),
                'org': self.get_value_from_dictionary(rep[20]),
            })

        nd_count = len(nd_reporting)
        (dis_series, dis_categories) = self.generate_highcharts_series(nd_reporting)

        if nd_count == 0:
            r_from = 'N/A'
            r_to = 'N/A'
        elif nd_count == 1:
            r_from = str(nd_reporting[0][0])
            r_to = str(nd_reporting[0][0])
        else:
            r_from = str(nd_reporting[0][0])
            r_to = str(nd_reporting[nd_count - 1][0])

        to_return = {
            'from': r_from,
            'to': r_to,
            'sel_range': "%s - %s" % (r_from, r_to),
            'dis_series': dis_series,
            'dis_categories': dis_categories,
            'all_reports': all_reports
        }

        return to_return

    def generate_agrovet_system_stats(self, inputs):
        # disease frequency
        ag_reporting_q = """
            SELECT date(date_trunc('week', report_date)) as weekly, drug_sold, count(*)
            FROM ag_detail as a INNER JOIN ag_reports as b on a.ag_report_id=b.id
            GROUP BY weekly, drug_sold
            ORDER BY weekly, drug_sold
        """
        # when filtering use this criteria
        #  % (str(start_date), str(end_date), tuple(sub_county))
        # all reports
        ag_all_reports_q = """
            SELECT date(datetime_uploaded), date(report_date), agrovet_name, outlet_name, latitude, longitude, accuracy, syndrome, syndrome_start_date, drug_sold, drug_quantity, farmer_location
            FROM ag_detail as a INNER JOIN ag_reports as b on a.ag_report_id=b.id
            ORDER BY report_date DESC
        """

        #  % (str(start_date), str(end_date), tuple(sub_county), tuple(species))

        # print(ag_reporting_q)
        # print(ag_all_reports_q)

        with connection.cursor() as cursor:
            cursor.execute(ag_reporting_q)
            ag_reporting = cursor.fetchall()

            cursor.execute(ag_all_reports_q)
            reports = cursor.fetchall()

        all_reports = []
        for rep in reports:
            all_reports.append({
                'upload_date': str(rep[0]),
                'report_date': str(rep[1]),
                'agrovet_name': str(rep[2]),
                'outlet_name': str(rep[3]),
                'latitude': str(rep[4]),
                'longitude': str(rep[5]),
                'accuracy': str(rep[6]),
                'syndrome': str(rep[7]),
                'syndrome_start_date': str(rep[8]),
                'drug_sold': self.get_value_from_dictionary(rep[9]),
                'drug_quantity': str(rep[10]),
                'farmer_location': str(rep[11]),
            })

        ag_count = len(ag_reporting)
        (ag_series, ag_categories) = self.generate_highcharts_series(ag_reporting)

        if ag_count == 0:
            r_from = 'N/A'
            r_to = 'N/A'
        elif ag_count == 1:
            r_from = str(ag_reporting[0][0])
            r_to = str(ag_reporting[0][0])
        else:
            r_from = str(ag_reporting[0][0])
            r_to = str(ag_reporting[ag_count - 1][0])
        to_return = {
            'from': r_from,
            'to': r_to,
            'sel_range': "%s - %s" % (r_from, r_to),
            'ag_series': ag_series,
            'ag_categories': ag_categories,
            'all_reports': all_reports
        }

        return to_return

    def generate_abattoir_system_stats(self, inputs):
        # disease frequency
        sh_reporting_q = """
            SELECT date(date_trunc('week', report_date)) as weekly, lesions, count(*)
            FROM sh_reports as a
            INNER JOIN sh_species as b on a.id=b.sh_report_id
            INNER JOIN sh_body_parts as c on b.id=c.sh_specie_id
            GROUP BY weekly, lesions ORDER BY weekly, lesions;
        """
        # when filtering use this criteria
        #  % (str(start_date), str(end_date), tuple(sub_county))
        # all reports
        sh_all_reports_q = """
            SELECT date(report_date), abattoir, animal_source, latitude, longitude, accuracy, specie, no_slaughtered, b.no_condemned, body_part, lesions, c.no_condemned, sample_collected
            FROM sh_reports as a
            INNER JOIN sh_species as b on a.id=b.sh_report_id
            INNER JOIN sh_body_parts as c on b.id=c.sh_specie_id
            ORDER BY report_date DESC
        """

        #  % (str(start_date), str(end_date), tuple(sub_county), tuple(species))

        with connection.cursor() as cursor:
            cursor.execute(sh_reporting_q)
            sh_reporting = cursor.fetchall()

            cursor.execute(sh_all_reports_q)
            reports = cursor.fetchall()

        all_reports = []
        for rep in reports:
            all_reports.append({
                'report_date': str(rep[0]),
                'abattoir': str(rep[1]),
                'animal_source': str(rep[2]),
                'latitude': str(rep[3]),
                'longitude': str(rep[4]),
                'accuracy': str(rep[5]),
                'specie': str(rep[6]),
                'no_slaughtered': int(rep[7]),
                'carcas_no_condemned': int(rep[8]),
                'body_part': str(rep[9]),
                'lesions': str(rep[10]),
                'part_no_condemned': int(rep[11]),
                'sample_collected': str(rep[12]),
            })

        sh_count = len(sh_reporting)
        (sh_series, sh_categories) = self.generate_highcharts_series(sh_reporting)

        if sh_count == 0:
            r_from = 'N/A'
            r_to = 'N/A'
        elif sh_count == 1:
            r_from = str(sh_reporting[0][0])
            r_to = str(sh_reporting[0][0])
        else:
            r_from = str(sh_reporting[0][0])
            r_to = str(sh_reporting[sh_count - 1][0])

        to_return = {
            'from': r_from,
            'to': r_to,
            'sel_range': "%s - %s" % (r_from, r_to),
            'sh_series': sh_series,
            'sh_categories': sh_categories,
            'all_reports': all_reports
        }

        return to_return

    def generate_highcharts_series(self, data):
        """
        Given a set of data like:
           weekly   |     disease     | count
        ------------+-----------------+-------
         2015-08-24 | ENTERITIS       |     1
         2018-08-20 | HEARTWATER      |     1
         2018-08-27 | HELMINTHIASIS   |     2
         2018-08-27 | LSD             |     1
         2018-08-27 | SKIN INFECTION  |     3
         2018-09-03 | COCCIDIOSIS     |     2

        Where:
            1st column is dates
            2nd column contains metrics to be used as series
            3rd column contains the counts

        Generate series that can be used in highcharts, like:
        series: [{
            name: 'Tokyo',
            data: [49.9, 71.5, 106.4, 129.2, 144.0, 176.0, 135.6, 148.5, 216.4, 194.1, 95.6, 54.4]

        }, {
            name: 'New York',
            data: [83.6, 78.8, 98.5, 93.4, 106.0, 84.5, 105.0, 104.3, 91.2, 83.5, 106.6, 92.3]

        }, {
            name: 'London',
            data: [48.9, 38.8, 39.3, 41.4, 47.0, 48.3, 59.0, 59.6, 52.4, 65.2, 59.3, 51.2]

        }, {
            name: 'Berlin',
            data: [42.4, 33.2, 34.5, 39.7, 52.6, 75.5, 57.4, 60.4, 47.6, 39.1, 46.8, 51.1]

        }]
        """

        # get the unique metrics to use as well as put the data in a dictionary which is easy to query
        metrics = []
        dates = []
        series_data = defaultdict(dict)
        series = defaultdict(dict)
        for node in data:
            t_date = str(node[0])
            t_metric = str(node[1])
            t_count = int(node[2])

            # create our unique metrics
            if t_metric not in metrics:
                metrics.append(t_metric)
                series[t_metric] = {'name': t_metric, 'data': []}

            # create our unique dates
            if t_date not in dates:
                dates.append(t_date)

            series_data[t_date][t_metric] = t_count

        for t_date in dates:
            for t_metric in metrics:
                try:
                    # try to append the data if its there, if the data doesn't exist it will throw an exception where we shall add a 0
                    series[t_metric]['data'].append(series_data[t_date][t_metric])
                except Exception:
                    series[t_metric]['data'].append(0)

        # now lets remove the series names from the series
        clean_series = []
        for t_serie in series:
            clean_series.append(series[t_serie])

        # terminal.tprint(json.dumps(series_data), 'okblue')
        # terminal.tprint(json.dumps(clean_series), 'okblue')

        return clean_series, dates

    def schedule_notification(self, template, recipient, message):
        # This function should be in the notifications module, but due to cyclic dependancies, we include it here
        try:
            # if we are using PostgreSQL, there is no need to plug the timezone
            if re.search('livhealth$', settings.SITE_NAME, re.IGNORECASE) is None: 
                cur_time = timezone.localtime(timezone.now())
            else:
                cur_time = datetime.now()
            # print(message)
            queue_item = SMSQueue(
                template=template,
                message=message,
                recipient=recipient,
                # recipient_no=recipient.cell_no if recipient.cell_no else recipient.alternative_cell_no,
                recipient_no='+254720000097' if settings.DEBUG else recipient.cell_no if recipient.cell_no else recipient.alternative_cell_no,
                msg_status='SCHEDULED',
                schedule_time=cur_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            queue_item.full_clean()
            queue_item.save()
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def send_email(self, subject, recipients, text_content, html_content):
        # This function should be in the notifications module, but due to cyclic dependancies, we include it here
        msg = EmailMultiAlternatives(subject, text_content, settings.DEFAULT_FROM_EMAIL, recipients)
        msg.attach_alternative(html_content, "text/html")
        msg.send()


ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n/10%10!=1)*(n%10<4)*n%10::4])


# lets capture the missing information when processing and then send a notification to the admin
missing_info = {}
def auto_process_submissions():
    terminal.tprint('Starting the auto process function', 'warn')
    odk_forms = OdkForms(None)

    # get all the forms and process the forms matching the criteria like 'dsf'
    all_forms = odk_forms.refresh_forms()
    if all_forms is None: return None
    syndromic_forms = []
    nd_forms = []
    abattoir_forms = []
    agrovet_forms = []
    for form in all_forms:
        if form['id'] == '-1':
            continue

        if re.search("_dsf_", form['full_id']) is not None:
            syndromic_forms.append(form['id'])
        elif re.search("nd1_", form['full_id']) is not None:
            nd_forms.append(form['id'])
        elif re.search("abattoir_", form['full_id']) is not None:
            abattoir_forms.append(form['id'])
        elif re.search("agrovets_", form['full_id']) is not None:
            agrovet_forms.append(form['id'])

    # lets process the syndromic submissions
    process_syndromic_submissions(syndromic_forms)
    process_notifiable_diseases(nd_forms)
    process_agrovet_records(agrovet_forms)
    process_abattoir_records_v1(abattoir_forms)

    # loop through the missing information and create an email to the admin
    try:
        email_message = ''
        plain_message = ''
        has_skipped_subm = False
        for i_key in missing_info.keys():
            missing_list = list(set(missing_info[i_key]))
            if len(missing_list) == 0:
                continue
            if i_key == 'missing_mapped_village':
                cur_message = odk_forms.email_message_inner_template % ('Villages with missing GPS coordinates', ', '.join(list(set(missing_info[i_key]))))
            elif i_key == 'missing_cdr':
                cur_message = odk_forms.email_message_inner_template % ('CDRs missing from the recipients list', ', '.join(list(set(missing_info[i_key]))))
            elif i_key == 'missing_cdr_no':
                cur_message = odk_forms.email_message_inner_template % ('Reporting CDRs with missing phone numbers in the recipients list', ', '.join(list(set(missing_info[i_key]))))
            elif i_key == 'skipped_subm':
                has_skipped_subm = True
                missing_list = [str(missing_info[i_key][0])]
                cur_message = odk_forms.email_message_inner_template % ('Number of skipped submissions due to incompatibility of forms ', str(missing_info[i_key][0]))

            plain_message = '\t%s: %s' % (i_key, ', '.join(missing_list))
            email_message = "%s\n%s" % (email_message, cur_message)

        # if we only have a skipped_subm skip this email
        if has_skipped_subm and len(missing_info.keys()) == 1:
            # we only have skipped subm
            print('skip sending this email')
            email_message = ''
            plain_message = ''

        if email_message != '':
            # we need to send an email with errors
            livhealth_admins = Recipients.objects.filter(designation='livhealth_admin').exclude(recipient_email__isnull=True).exclude(recipient_email__exact='').all()
            recipients = []
            for admn in livhealth_admins:
                recipients.append(admn.recipient_email)

            text_content = render_to_string('email-missing-info.txt', { 'message_details': plain_message })
            html_content = render_to_string('email-missing-info.html', {'message_details': email_message })
            email_subject = '[%s] Missing details' % settings.SITE_NAME

            odk_forms.send_email(email_subject, recipients, text_content, html_content)
    except Exception as e:
        terminal.tprint(str(e), 'fail')
        # terminal.tprint(json.dumps(subm), 'fail')
        sentry.captureException()


def process_syndromic_submissions(form_ids):
    terminal.tprint('\n\nProcessing syndromes...', 'warn')
    odk_forms = OdkForms(None)

    all_submissions = []
    for form_id in form_ids:
        this_submissions = odk_forms.fetch_merge_data(form_id, None, 'json', 'submissions', None)
        if(isinstance(this_submissions, list)):
            all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

    # terminal.tprint(json.dumps(all_submissions), 'ok')
    # if there is no GPS to use, default to use ILRI's GPS coordinates
    default_gps = "-1.2696984092022385 36.726427731756985 1702.0 8.6"

    try:
        for subm in all_submissions:
            # check if the current submission is already processed
            # I made a blunder by adding a 9 when saving some incidences, lets atone for our sins
            s_inc = SyndromicIncidences.objects.filter(Q(uuid=subm['_uuid']) | Q(uuid='%s%s' %  (subm['_uuid'],'9')))
            if s_inc.count() > 0:
                # terminal.tprint("Submission '%s' already processed, continue" % subm['_uuid'], 'warn')
                continue
            else:
                # some old forms structure were quite wrong and should be omitted
                # for some strange reason, some forms are missing the village record.... just omit them
                if subm['_xform_id_string'] in ["marsabit_dsf_v1"] or 's1q6_village' not in subm:
                    if 'skipped_subm' not in missing_info:
                        missing_info['skipped_subm'] = [0]
                    
                    missing_info['skipped_subm'][0] = missing_info['skipped_subm'][0] + 1
                    continue

                # terminal.tprint(json.dumps(subm), 'fail')
                # we have a submission to process
                datetime_subm = timezone.make_aware(datetime.strptime(subm['_submission_time'], "%Y-%m-%dT%H:%M:%S"))
                datetime_rep = timezone.make_aware(datetime.strptime(subm['s0q2_start_time'][:23], "%Y-%m-%dT%H:%M:%S.%f"))
                try:
                    mapped_village = VillageMapping.objects.filter(village_code=subm['s1q6_village'])
                    if len(mapped_village) > 0:
                        mapped_village = mapped_village[0]
                    else:
                        if 'missing_mapped_village' not in missing_info:
                            missing_info['missing_mapped_village'] = []
                        missing_info['missing_mapped_village'].append(subm['s1q6_village'])
                        raise ValueError("Village '%s' not found in the mapping database, use the fall back plan..." % subm['s1q6_village'])

                    latitude = mapped_village.latitude
                    longitude = mapped_village.longitude
                    accuracy = 1
                    terminal.tprint('\t%s: Using mapped villages' % subm['s1q6_village'], 'fail')
                except Exception as e:
                    # terminal.tprint(str(e), 'warn')
                    terminal.tprint('\t%s: Using collected GPS' % subm['s1q6_village'], 'fail')
                    try:
                        geo = subm['s1q1_gps'].split()
                    except KeyError:
                        geo = default_gps.split()

                    latitude = geo[0]
                    longitude = geo[1]
                    accuracy = geo[3]

                new_inc = SyndromicIncidences(
                    # uuid=subm['_uuid'],
                    uuid=subm['_uuid'],
                    datetime_reported=datetime_rep,
                    datetime_uploaded=datetime_subm,
                    county=subm['s1q2_county'],
                    sub_county=subm['s1q3_sub_county'],
                    ward=subm['s1q5_ward'],
                    village=subm['s1q6_village'],
                    reporter=subm['s1q7_cdr_name'],
                    latitude=latitude,
                    longitude=longitude,
                    accuracy=accuracy,
                    no_cases=int(subm['s2q3_rpt_livestock_count']),
                    scvo_reporter=subm['s1q4_enum']
                )
                new_inc.publish()

                reported_species = []
                if subm['s2q1_new_cases'] == 'yes':
                    top_inc = subm['s2q3_rpt_livestock'][0]
                    for inc in subm['s2q3_rpt_livestock'][0]['s2q7_rpt_syndromes']:
                        # terminal.tprint(json.dumps(inc), 'warn')
                        end_date = inc['s2q13_end_date'] if inc['s2q12_still_persistent'] == 'no' else None
                        reported_species.append(top_inc['s2q4_cur_livestock'])
                        inc_det = SyndromicDetails(
                            incidence=new_inc,
                            species=top_inc['s2q4_cur_livestock'],
                            syndrome=inc['s2q8_cur_syndrome'],
                            start_date=inc['s2q11_start_date'],
                            end_date=end_date,
                            herd_size=int(inc['s2q14_herd_size']),
                            no_sick=int(inc['s2q15_no_sick']),
                            no_dead=int(inc['s2q16_no_dead']),
                            clinical_signs=inc['s2q10_clinical_signs'],
                            prov_diagnosis=inc['s2q17_prov_diagnosis']
                        )
                        inc_det.publish()

                # schedule to send a SMS to the CDR
                # get the CDR who reported this incidence
                try:
                    cdr = Recipients.objects.filter(username=subm['s1q7_cdr_name']).get()
                    if cdr.cell_no is None and cdr.alternative_cell_no is None:
                        if 'missing_cdr_no' not in missing_info:
                            missing_info['missing_cdr_no'] = []
                        missing_info['missing_cdr_no'].append(subm['s1q7_cdr_name'])
                        continue
                    template = MessageTemplates.objects.filter(template_name='CDR Feedback').get()
                    message = template.template % (cdr.first_name, subm['s1q6_village'].upper(), ', '.join(reported_species).upper(), datetime_rep.strftime("%d/%m/%Y"))
                    odk_forms.schedule_notification(template, cdr, message)
                except Recipients.DoesNotExist:
                    # missing a CDR in the recipients list. Ask the admin to update the list
                    # terminal.tprint(json.dumps(subm), 'fail')
                    if 'missing_cdr' not in missing_info:
                        missing_info['missing_cdr'] = []
                    missing_info['missing_cdr'].append(subm['s1q7_cdr_name'])

    except Exception as e:
        terminal.tprint(str(e), 'fail')
        # terminal.tprint(json.dumps(subm), 'fail')
        sentry.captureException()


def process_notifiable_diseases(form_ids):
    terminal.tprint('\n\nProcessing notifiable diseases submissions...', 'warn')
    odk_forms = OdkForms(None)

    all_submissions = []
    for form_id in form_ids:
        this_submissions = odk_forms.fetch_merge_data(form_id, None, 'json', 'submissions', None)
        if(isinstance(this_submissions, list)):
            all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

    # terminal.tprint(json.dumps(all_submissions), 'ok')
    # if there is no GPS to use, default to use ILRI's GPS coordinates
    default_gps = "-1.2696984092022385 36.726427731756985 1702.0 8.6"

    try:
        for subm in all_submissions:
            # check if the current submission is already processed
            nd_report = NDReport.objects.filter(uuid=subm['_uuid'])
            if nd_report.count() == 1:
                # this report is already processed
                continue
            else:
                # we have a submission to process
                # terminal.tprint(json.dumps(subm), 'ok')
                # print ""
                # continue

                # for some strange reason the location information is missing... so just ski this submission
                if 's1q5_village' not in subm:
                    continue

                datetime_subm = timezone.make_aware(datetime.strptime(subm['_submission_time'], "%Y-%m-%dT%H:%M:%S"))            # datetime_uploaded
                datetime_rep = timezone.make_aware(datetime.strptime(subm['s0q2_start_time'][:23], "%Y-%m-%dT%H:%M:%S.%f"))      # datetime_reported
                try:
                    mapped_village = VillageMapping.objects.filter(village_code=subm['s1q5_village']).filter(latitude__isnull=False)
                    if len(mapped_village) > 0:
                        mapped_village = mapped_village[0]
                        latitude = mapped_village.latitude
                        longitude = mapped_village.longitude
                        accuracy = 1
                        terminal.tprint('\t%s: Using mapped villages' % subm['s1q5_village'], 'fail')
                    else:
                        if 'missing_mapped_village' not in missing_info:
                            missing_info['missing_mapped_village'] = []
                        missing_info['missing_mapped_village'].append(subm['s1q5_village'])
                        raise ValueError("Village '%s' was not found in the mapping database..." % subm['s1q5_village'])

                except Exception as e:
                    # terminal.tprint(str(e), 'warn')
                    # terminal.tprint('\t%s: Using collected GPS' % subm['s1q5_village'], 'fail')
                    try:
                        geo = subm['s1q1_gps'].split()
                    except KeyError:
                        # resort to the default gps
                        geo = default_gps.split()

                    latitude = geo[0]
                    longitude = geo[1]
                    accuracy = geo[3]

                # generate the ND report object instance
                nd_report = NDReport(
                    uuid=subm['_uuid'],
                    datetime_reported=datetime_rep,
                    datetime_uploaded=datetime_subm,
                    county=subm['s1q2_county'],
                    sub_county=subm['s1q3_sub_county'],
                    ward=subm['s1q4_ward'],
                    village=subm['s1q5_village'],
                    latitude=latitude,
                    longitude=longitude,
                    accuracy=accuracy,
                    org_survey=subm['s2q14_organisation'],
                    nd_date_started=subm['s1q7_date'],
                    nd_date_reported=subm['s1q8_date']
                )
                nd_report.publish()

                for disease in subm['s2q2_rpt_disease']:
                    for specie in disease['s2q5_rpt_animal_species']:
                        # terminal.tprint(json.dumps(specie), 'fail')
                        nd_detail = NDDetail(
                            nd_report=nd_report,
                            disease=disease['s2q2_cur_disease_label'],
                            species=specie['s2q5_cur_animal_species_label'],
                            diagnosis_type=disease['s2q3_diagnosis'],
                            production_system=specie['s2q10_prod_sys'],
                            is_zoonotic=specie['s2q11_zoonosis'] if(specie['s2q11_zoonosis'] == 0 or specie['s2q11_zoonosis'] == 1) else None,
                            no_risk=specie['s2q6_risk_nos'],
                            no_sick=specie['s2q7_sick_nos'],
                            no_dead=specie['s2q8_death_nos'],
                            measure_taken=specie['s2q12_measure']
                        )

                        if 's2q13_vaccination_nos' in specie:
                            nd_detail.no_vaccinated = specie['s2q13_vaccination_nos']

                        if 's2q9_salughtered_nos' in specie:
                            nd_detail.no_slaughtered = specie['s2q9_salughtered_nos']

                        nd_detail.publish()

    except Exception as e:
        terminal.tprint(str(e), 'fail')
        # terminal.tprint(json.dumps(subm), 'fail')
        sentry.captureException()


def process_abattoir_records_v1(form_ids):
    terminal.tprint('\n\nProcessing abattoir submissions...', 'warn')
    odk_forms = OdkForms(None)

    all_submissions = []
    for form_id in form_ids:
        this_submissions = odk_forms.fetch_merge_data(form_id, None, 'json', 'submissions', None)
        if(isinstance(this_submissions, list)):
            all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

    # terminal.tprint(json.dumps(all_submissions), 'ok')
    # if there is no GPS to use, default to use ILRI's GPS coordinates
    default_gps = "-1.2696984092022385 36.726427731756985 1702.0 8.6"

    try:
        for subm in all_submissions:
            # check if the current submission is already processed
            sh_report = SHReport.objects.filter(uuid=subm['_uuid'])
            if sh_report.count() == 1:
                # this report is already processed
                continue
            else:
                # we have a submission to process
                # terminal.tprint(json.dumps(subm), 'ok')
                # print ""
                # continue
                datetime_subm = timezone.make_aware(datetime.strptime(subm['_submission_time'], "%Y-%m-%dT%H:%M:%S"))            # datetime_uploaded
                datetime_rep = timezone.make_aware(datetime.strptime(subm['s0q2_start_time'][:23], "%Y-%m-%dT%H:%M:%S.%f"))      # datetime_reported
                try:
                    geo = subm['s1q7_gps'].split()
                except KeyError:
                    # resort to the default gps
                    geo = default_gps.split()

                latitude = geo[0]
                longitude = geo[1]
                accuracy = geo[3]

                # generate the SH report object instance
                sh_report = SHReport(
                    uuid=subm['_uuid'],
                    datetime_reported=datetime_rep,
                    datetime_uploaded=datetime_subm,
                    report_date=subm['s0q3_survey_date'],
                    county=subm['s1q8_county'] if 's1q8_county' in subm else subm['s1q4_county'],
                    abattoir=subm['s1q5_abattoir_name'] if 's1q5_abattoir_name' in subm else subm['s1q6_abattoir'],
                    latitude=latitude,
                    longitude=longitude,
                    accuracy=accuracy,
                    reporter=subm['s1q2_ins_name'] if 's1q2_ins_name' in subm else subm['s1q7_ins_name'],
                    animal_source=subm['s1q4_animal_source'] if 's1q4_animal_source' in subm else subm['s2q1_animal_source']
                )
                sh_report.publish()
                # print "report ok"

                # get the species in this report
                # The form has an error in that I can't link species and body parts, so we are going to get the first specie
                if 's2q1_rpt_animal_species' in subm:
                    specie = subm['s2q1_rpt_animal_species'][0]

                    # loop to get the no of animals slaughtered
                    slaughter_count = 0
                    rejected_count = 0
                    for anim_cat in specie['s2q2_rpt_animal_category']:
                        for age_grp in anim_cat['s2q3_rpt_animal_age']:
                            slaughter_count = slaughter_count + int(age_grp['s2q4_consignment_nos'])
                            rejected_count = rejected_count + int(age_grp['s2q6_rejected_nos'])

                    sh_specie = SHSpecies(
                        sh_report=sh_report,
                        specie=specie['s2q1_cur_animal_species_label'],
                        no_slaughtered=slaughter_count,
                        no_condemned=rejected_count,
                    )
                    sh_specie.publish()
                    # print "specie ok"

                    # now lets get the body parts
                    for b_part in subm['s3q1_rpt_body_part']:
                        all_lesions = b_part['s3q5_lesions'].split()
                        for lesion in all_lesions:
                            sh_part = SHParts(
                                sh_specie=sh_specie,
                                body_part=b_part['s3q1_cur_body_part_label'],
                                lesions=lesion,
                                no_condemned=b_part['s3q4_condemned_nos'],
                                sample_collected=False if b_part['s3q6_samples'] == 'no' else True
                            )
                            sh_part.publish()
                            # print "body part ok"
                elif 's2q4_rpt_species' in subm:
                    for specie in subm['s2q4_rpt_species']:
                        sh_specie = SHSpecies(
                            sh_report=sh_report,
                            specie=specie['s2q6_cur_specie_name'],
                            no_slaughtered=specie['s2q8_no_slaughtered'],
                            no_condemned=specie['s2q8_no_condemned'],
                        )
                        sh_specie.publish()

                        # now lets save the body parts
                        if int(specie['s2q10_rpt_body_part_count']) != 0:
                            for b_part in specie['s2q10_rpt_body_part']:
                                all_lesions = b_part['s2q15_lesions'].split()
                                for lesion in all_lesions:
                                    sh_part = SHParts(
                                        sh_specie=sh_specie,
                                        body_part=b_part['s2q12_cur_body_part_label'],
                                        lesions=lesion,
                                        no_condemned=b_part['s2q14_no_condemned'],
                                        sample_collected=False if specie['s2q16_samples'] == 'no' else True
                                    )
                                    sh_part.publish()
                        elif specie['s2q8_no_slaughtered'] == specie['s2q8_no_condemned']:
                            # we condemned a whole carcass
                            sh_part = SHParts(
                                sh_specie=sh_specie,
                                body_part='Whole Carcass',
                                lesions='Not Specified',
                                no_condemned=b_part['s2q8_no_condemned'],
                                sample_collected=False if specie['s2q16_samples'] == 'no' else True
                            )
                            sh_part.publish()

    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        logger.error(traceback.format_exc())
        # terminal.tprint(json.dumps(subm), 'fail')
        sentry.captureException()


def process_agrovet_records(form_ids):
    terminal.tprint('\n\nProcessing agrovet submissions...', 'warn')
    odk_forms = OdkForms(None)

    all_submissions = []
    for form_id in form_ids:
        this_submissions = odk_forms.fetch_merge_data(form_id, None, 'json', 'submissions', None)
        if(isinstance(this_submissions, list)):
            all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

    # terminal.tprint(json.dumps(all_submissions), 'ok')
    # if there is no GPS to use, default to use ILRI's GPS coordinates
    default_gps = "-1.2696984092022385 36.726427731756985 1702.0 8.6"

    try:
        for subm in all_submissions:
            # check if the current submission is already processed
            # print(subm['_uuid'])
            ag_report = AGReport.objects.filter(uuid=subm['_uuid'])
            # print(ag_report[0].uuid)
            if ag_report.count() == 1:
                # this report is already processed
                # print('Already processed...')
                # ag_report.delete()
                continue
            else:
                # we have a submission to process
                # if 's2q1_rpt_drug_sold' in subm:
                #     terminal.tprint(subm['s2q1_rpt_drug_sold'][0]['s2q2_syndromes'], 'ok')
                # print('Adding...')
                # continue
                datetime_subm = timezone.make_aware(datetime.strptime(subm['_submission_time'], "%Y-%m-%dT%H:%M:%S"))            # datetime_uploaded
                datetime_rep = timezone.make_aware(datetime.strptime(subm['s0q2_start_time'][:23], "%Y-%m-%dT%H:%M:%S.%f"))     # datetime_reported
                try:
                    geo = subm['s1q7_gps'].split()
                except KeyError:
                    # resort to the default gps
                    geo = default_gps.split()

                latitude = geo[0]
                longitude = geo[1]
                accuracy = geo[3]

                # generate the SH report object instance
                ag_report = AGReport(
                    uuid=subm['_uuid'],
                    datetime_reported=datetime_rep,
                    datetime_uploaded=datetime_subm,
                    report_date=subm['s0q3_survey_date'],
                    county=settings.COUNTY_NAME,
                    agrovet_name=subm['s1q1_agrovet_name'],
                    outlet_name=subm['s1q2_outlet_name'] if 's1q2_outlet_name' in subm else subm['s1q6_agrovet_stockists'],
                    latitude=latitude,
                    longitude=longitude,
                    accuracy=accuracy
                )
                ag_report.publish()
                # print "report ok"

                # generate the drug report
                if 's2q2_syndromes' in subm:
                    # print("s2q2_syndromes...")
                    for drug in subm['s2q1_rpt_drug_sold']:
                        ag_detail = AGDetail(
                            ag_report=ag_report,
                            syndrome=subm['s2q2_syndromes'],
                            syndrome_start_date=drug['s2q4_date_started'],
                            drug_sold=drug['s2q1_cur_drug_label'],
                            drug_quantity=1,                                # assume its 1 since the quantity is not indicated
                            farmer_location=drug['s2q5_location']
                        )
                        ag_detail.publish()
                    # print "detail ok"
                elif 's2q1_rpt_drug_sold' in subm:
                    # print("s2q1_rpt_drug_sold...")
                    for drug in subm['s2q1_rpt_drug_sold']:
                        syndromes = drug['s2q2_syndromes'].split()
                        for syndrome in syndromes:
                            ag_detail = AGDetail(
                                ag_report=ag_report,
                                syndrome=syndrome,
                                syndrome_start_date=drug['s2q4_date_started'],
                                drug_sold=drug['s2q1_cur_drug_label'],
                                drug_quantity=subm['s2q1_rpt_drug_sold_count'],
                                farmer_location=drug['s2q5_location']
                            )
                            ag_detail.publish()
                elif 's2q1_rpt_syndromes' in subm:
                    # print("s2q1_rpt_syndromes...")
                    for syndrome in subm['s2q1_rpt_syndromes']:
                        drugs = syndrome['s2q2_drugs_sold'].split()
                        for drug in drugs:
                            ag_detail = AGDetail(
                                ag_report=ag_report,
                                syndrome=syndrome['s2q1_cur_syndromes_label'],
                                syndrome_start_date=syndrome['s2q4_date_started'],
                                drug_sold=drug,
                                drug_quantity=syndrome['s2q3_drug_qty'],
                                farmer_location=syndrome['s2q5_location']
                            )
                            ag_detail.publish()
                        # print "detail ok"

    except Exception as e:
        terminal.tprint(str(e), 'fail')
        # terminal.tprint(json.dumps(subm), 'fail')
        sentry.captureException()
