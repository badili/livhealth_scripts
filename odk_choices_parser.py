"""The main processing unit for processing ODK options

"""
import csv
import re
import datetime
import uuid
import pytz
import json

from django.db import transaction
from django.utils import timezone
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from raven import Client

from .terminal_output import Terminal
from .models import Recipients, DictionaryItems, SubCounty, Ward, Village

terminal = Terminal()
sentry = Client(settings.SENTRY_DSN)

settings.TIME_ZONE
current_tz = pytz.timezone(settings.TIMEZONE)
timezone.activate(current_tz)


class ImportODKChoices():
    def __init__(self):
        # silence is golden
        self.module_name = 'Processing ODK Choices'
        # the mandatory headers for the choices spreadsheet
        self.odk_mandatory_headers = ['list_name', 'name', 'label', 'county', 'syndromes', 'disease', 'cdr_village', 'ward_subcounty', 'village_ward', 'enumerator_subcounty']
        self.phone_number_update_headers = ['nick_name', 'phone_number']

    def process_odk_choices_file(self, input_file):
        terminal.tprint('Processing the file %s...' % input_file, 'info')

        try:
            transaction.set_autocommit(False)
            with open(input_file, 'rt', -1, 'utf-8') as in_file:
                test_data = csv.DictReader(in_file, delimiter=',', quotechar='"')
                self.confirm_data_headers(test_data.fieldnames, self.odk_mandatory_headers)
                for row in test_data:
                    # print(row.values())
                    try:
                        if row['list_name'] == 'sub_county':
                            self.process_subcounty(row)
                        elif row['list_name'] == 'wards':
                            self.process_ward(row)
                        elif row['list_name'] == 'villages':
                            self.process_village(row)
                        elif row['list_name'] == 'cdrs':
                            self.process_personnel(row, 'cdr')
                        elif row['list_name'] == 'enumerators':
                            self.process_personnel(row, 'enumerator')
                        elif row['list_name'] == 'livhealth_mgmnt':
                            self.process_personnel(row, 'livhealth_mgmnt')
                        elif row['list_name'] == 'livhealth_admin':
                            self.process_personnel(row, 'livhealth_admin')
                    except ObjectDoesNotExist:
                        continue
        except UnicodeDecodeError as e:
            terminal.tprint("Cannot process the data below.\n%s" % str(e), 'fail')
        except Exception as e:
            transaction.rollback()
            sentry.captureException()
            terminal.tprint(str(e), 'fail')

        transaction.commit()
        terminal.tprint("The input file '%s' with test data has been processed successfully..." % input_file, 'info')

    def confirm_data_headers(self, file_headers, mandatory_headers):
        missing_headers = []
        for header in mandatory_headers:
            if header not in file_headers:
                missing_headers.append(header)

        if len(missing_headers):
            raise Exception("The input file is missing '%s' column(s) which is required" % ', '.join(missing_headers))

    def process_subcounty(self, subcounty):
        """Given a sub county details, add it to the database if it does not exist

        """
        try:
            sub_county = SubCounty.objects.filter(nick_name=subcounty['name'].strip()).get()
        except SubCounty.DoesNotExist:
            try:
                sub_county = SubCounty(
                    sub_county_name=subcounty['label'].strip(),
                    nick_name=subcounty['name'].strip()
                )
                sub_county.full_clean()
                sub_county.save()
            except Exception:
                raise
        except Exception:
            raise

        return sub_county

    def process_ward(self, ward):
        """Given a ward details, add it to the database if it does not exist
        """
        try:
            saved_ward = Ward.objects.filter(nick_name=ward['name'].strip()).get()
        except Ward.DoesNotExist:
            try:
                # get the subcounty of this ward
                sub_county = SubCounty.objects.filter(nick_name=ward['ward_subcounty'].strip()).get()
                saved_ward = Ward(
                    ward_name=ward['label'].strip(),
                    nick_name=ward['name'].strip(),
                    sub_county=sub_county
                )
                saved_ward.full_clean()
                saved_ward.save()
            except SubCounty.DoesNotExist:
                message = "'%s' sub county does not exist in the database." % ward['ward_subcounty'].strip()
                terminal.tprint(message, 'info')
                raise ObjectDoesNotExist(message)
            except Exception:
                raise
        except Exception:
            raise

        return saved_ward

    def process_village(self, village):
        """Given a ward details, add it to the database if it does not exist
        """
        try:
            saved_village = Village.objects.filter(nick_name=village['name'].strip()).get()
        except Village.DoesNotExist:
            try:
                # get the ward of the current village
                ward = Ward.objects.filter(nick_name=village['village_ward'].strip()).get()
                saved_village = Village(
                    village_name=village['label'].strip(),
                    nick_name=village['name'].strip(),
                    ward=ward
                )
                saved_village.full_clean()
                saved_village.save()
            except Ward.DoesNotExist:
                message = "'%s' ward does not exist in the database." % village['village_ward'].strip()
                terminal.tprint(message, 'info')
                raise ObjectDoesNotExist(message)
            except Exception:
                raise
        except Exception:
            raise

        return saved_village 

    def process_personnel(self, pers, pers_type):
        # we are only interested in updating CDR details
        # check if the cdr details are already saved in the database... if they are, update them if they are different
        try:
            personnel = Recipients.objects.filter(nick_name=pers['name'].strip()).get()
        except Recipients.DoesNotExist:
            # the pers is not saved in the Recipients database, so lets add him
            # get the village of this CDR
            village = None
            sub_county = None
            try:
                if pers_type is 'cdr':
                    village = Village.objects.filter(nick_name=pers['cdr_village'].strip()).get()
                elif pers_type is 'enumerator':
                    sub_county = SubCounty.objects.filter(nick_name=pers['enumerator_subcounty']).get()
                else:
                    sub_county = None

                cdr_label = pers['label'].strip()
                if re.search('^Dr|Mr|Mrs|Prof|Miss\.?', cdr_label) is None:
                    cdr_names = cdr_label.split()
                    salutation = None
                else:
                    # exclude the salutation, strip the remainder string of spaces and split it
                    split_names = re.split('^(Dr|Mrs|Mr|Prof|Miss)\.?', cdr_label)
                    salutation = split_names[1:2][0]
                    cdr_names = split_names[2:][0].strip().split()

                # print("%s == %s: %s - %s - %s" % (cdr_label, salutation if salutation is not None else '', cdr_names[:1][0], ' '.join(cdr_names[1:]), pers['name'].strip()))
                personnel = Recipients(
                    salutation=salutation,
                    first_name=cdr_names[:1][0],
                    other_names=None if len(cdr_names[1:]) == 0 else ' '.join(cdr_names[1:]),
                    nick_name=pers['name'].strip(),
                    designation=pers_type,
                    village=village,
                    sub_county=sub_county
                )
                personnel.full_clean()
                personnel.save()
            except Village.DoesNotExist:
                terminal.tprint("'%s' village does not exist in the database." % pers['cdr_village'].strip(), 'info')
            except Exception:
                raise
        except Exception:
            raise

    def process_phone_numbers(self, input_file):
        # we have a list of phone numbers that we need to update in our contact list
        terminal.tprint('Processing the file %s...' % input_file, 'info')

        try:
            transaction.set_autocommit(False)
            with open(input_file, 'rt', -1, 'utf-8') as in_file:
                test_data = csv.DictReader(in_file, delimiter=',', quotechar='"')
                self.confirm_data_headers(test_data.fieldnames, self.phone_number_update_headers)
                for row in test_data:
                    # terminal.tprint(json.dumps(row), 'error')
                    self.update_personnel(row)
        except UnicodeDecodeError as e:
            terminal.tprint("Cannot process the data below.\n%s" % str(e), 'fail')
        except Exception as e:
            transaction.rollback()
            sentry.captureException()
            terminal.tprint(str(e), 'fail')

        transaction.commit()
        terminal.tprint("The input file '%s' with test data has been processed successfully..." % input_file, 'info')

    def update_personnel(self, pers):
        try:
            personnel = Recipients.objects.filter(nick_name=pers['nick_name'].strip()).get()
            to_save = False
            processed_number = self.format_phone_number(pers['phone_number'])
            if 'email' in pers:
                if pers['email'].strip() != '':
                    # we have some email... check if its ok first
                    if re.search("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", pers['email']) is not None:
                        personnel.recepient_email = pers['email'].strip()
                        to_save = True

            if processed_number is not None:
                to_save = True
                if personnel.cell_no is None:
                    personnel.cell_no = processed_number
                elif personnel.cell_no != processed_number:
                    personnel.alternative_cell_no = processed_number

            if to_save:
                personnel.full_clean()
                personnel.save()
        except ValueError as e:
            terminal.tprint(str(e), 'fail')
        except Recipients.DoesNotExist:
            terminal.tprint("'%s' does not exist in the database, skipping them for now..." % pers['nick_name'].strip(), 'info')
        except Exception:
            raise

    def format_phone_number(self, phone_number):
        """Given a phone number, format it to include the country code
        """
        if phone_number is '':
            return None
        elif re.search('^7(\d{8})$', phone_number) is not None:
            return '+2547%s' % re.split('^7(\d{8})$', phone_number)[1:2][0]
        elif re.search('^0(\d{9})$', phone_number) is not None:
            return '+254%s' % re.split('^0(\d{9})$', phone_number)[1:2][0]
        elif re.search('^(\d{9})$', phone_number) is not None:
            return '+2547%s' % re.split('^(\d{9})$', phone_number)[1:2][0]
        elif re.search('^\+254\d{9}$', phone_number) is not None:
            return phone_number
        else:
            raise ValueError("Encountered a phone number '%s' of unknown format" % phone_number)
