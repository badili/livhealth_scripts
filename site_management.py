import os
import csv
import pytz

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from hashids import Hashids
from raven import Client

from .models import Recipients, SubCounty, Ward, Village
from .odk_choices_parser import ImportODKChoices
from .terminal_output import Terminal
from .onadata import Onadata

my_hashids = Hashids(min_length=5, salt=settings.SECRET_KEY)
terminal = Terminal()
sentry = Client(settings.SENTRY_DSN)

current_tz = pytz.timezone(settings.TIMEZONE)
timezone.activate(current_tz)

class SiteManager():

    def __init__(self):
        print('Silence is golden')

    def save_recipient(self, request):
        try:
            # get the campaign details and add them to the database
            salutation = request.POST.get('salutation')
            first_name = request.POST.get('first-name').strip()
            other_names = request.POST.get('other-names').strip()
            designation = request.POST.get('designation')
            email = request.POST.get('email').strip()
            cell_no = request.POST.get('cell_no').strip()
            alternative_cell_no = request.POST.get('alternative_cell_no').strip() if request.POST.get('alternative_cell_no') != '' else None
            sub_county_id = request.POST.get('sub-county')
            ward_id = request.POST.get('ward').strip()
            village_id = request.POST.get('village').strip()
            update_selects = request.POST.get('update_livhealth_app')

            # get the campaign names for this template
            if sub_county_id == '-1' or sub_county_id == '':
                sub_county = None
                ward = None
                village = None
            else:
                sub_county = SubCounty.objects.filter(id=sub_county_id).get()
                if ward_id != '' and ward_id.isnumeric() is False:
                    # we have a new ward...
                    odk_choices_parser = ImportODKChoices()
                    new_ward = {
                        'label': ward_id,
                        'name': ward_id.replace("'.- ", '').lower(),
                        'ward_subcounty': sub_county.nick_name
                    }
                    ward = odk_choices_parser.process_ward(new_ward)
                else:
                    ward = Ward.objects.filter(id=ward_id).get() if ward_id != '' else None
                
                if village_id != '' and village_id.isnumeric() is False:
                    # we have a new village...
                    odk_choices_parser = ImportODKChoices()
                    new_village = {
                        'label': village_id,
                        'name': village_id.replace("'.- ", '').lower(),
                        'village_ward': ward.nick_name
                    }
                    village = odk_choices_parser.process_village(new_village)
                else:
                    village = Village.objects.filter(id=village_id).get() if village_id != '' else None

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                recipient = Recipients.objects.filter(id=request.POST.get('object_id')).get()
                recipient.salutation = salutation
                recipient.first_name = first_name
                recipient.other_names = other_names
                recipient.designation = designation
                recipient.cell_no = cell_no
                recipient.alternative_cell_no = alternative_cell_no
                recipient.recipient_email = email
                recipient.village = village
                recipient.ward = ward
                recipient.sub_county = sub_county
            else:
                # fabricate a nick_name for the recipient
                nick_name = '%s_%s' % (first_name.replace("'.- ", '').lower(), other_names.replace("'.- ", '').lower())
                recipient = Recipients(
                    salutation=salutation,
                    first_name=first_name,
                    other_names=other_names,
                    designation=designation,
                    cell_no=cell_no,
                    alternative_cell_no=alternative_cell_no,
                    recipient_email=email,
                    nick_name=nick_name,
                    village=village,
                    ward=ward,
                    sub_county=sub_county
                )
            recipient.full_clean()
            recipient.save()

            # now lets push the data to the ona form
            if update_selects == 'yes':
                self.create_updated_selects()

            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def create_updated_selects(self):
        # generate a new csv list for itemsets
        # 1. county
        # 2. sub_county
        # 3. wards
        # 4. villages
        # 5. cdrs
        # 6. enumerators

        try:
            # print('We gonna create a new list for external selects')
            itemsets = 'itemsets.csv'
            # ona = Onadata(settings.ONADATA_URL, settings.ONADATA_MASTER)
            ona = Onadata(settings.ONADATA_URL, settings.ONADATA_TOKEN)

            with open(itemsets, 'w', newline='') as csvfile:
                itemsetswriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                itemsetswriter.writerow(['list_name','name','label','county','syndromes','disease','cdr_village','ward_subcounty','village_ward','enumerator_subcounty'])

                # 1. Syndromic surveillance
                # county
                itemsetswriter.writerow(['county', 'Turkana', 'Turkana'])

                # sub counties
                all_subcounties = SubCounty.objects.order_by('nick_name').all()
                for s_county in all_subcounties:
                    itemsetswriter.writerow(['sub_county', s_county.nick_name, '%s' % s_county.sub_county_name, 'Turkana'])

                # wards
                all_wards = Ward.objects.select_related('sub_county').order_by('nick_name').all()
                for ward in all_wards:
                    itemsetswriter.writerow(['wards', ward.nick_name, ward.ward_name, '', '', '', '', ward.sub_county.nick_name])

                # villages
                all_villages = Village.objects.select_related('ward').order_by('nick_name').all()
                for village in all_villages:
                    itemsetswriter.writerow(['villages', village.nick_name, village.village_name, '', '', '', '', '', village.ward.nick_name])

                # cdrs
                all_cdrs = Recipients.objects.select_related('village').filter(designation='cdr', is_active=True).order_by('nick_name').all()
                for cdr in all_cdrs:
                    if cdr is None: continue
                    itemsetswriter.writerow(['cdrs', cdr.nick_name, '%s %s' % (cdr.first_name, cdr.other_names), '', '', '', cdr.village.nick_name])

                # yes -- no
                itemsetswriter.writerow(['yes_no', 'yes', 'Yes'])
                itemsetswriter.writerow(['yes_no', 'no', 'No'])

                # enumerators
                all_users = Recipients.objects.select_related('sub_county').filter(designation__in=('enumerator', 'meat_inspector', 'lab_personnel'), is_active=True).order_by('nick_name').all()
                for user in all_users:
                    if user.sub_county is None: continue
                    itemsetswriter.writerow(['enumerators', user.nick_name, '%s %s' % (user.first_name, user.other_names), '', '', '', '', '', '', user.sub_county.nick_name ])

                # now upload the itemsets
                ona.upload_itemsets_csv(itemsets, 'itemsets.csv', ['turkana_ssf_'])

            '''
            # 2. meat inspectors
            meat_inspectors = 'itemsets.csv'
            with open(meat_inspectors, 'w', newline='') as csvfile:
                itemsetswriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                itemsetswriter.writerow(['list_name','name','label','county','abattoir_subcounty','inspector_abattoir'])
            '''

            # now lets delete the file, if we aren't able its not a catastrophe
            try:
                os.remove(itemsets)
            except Exception as e:
                sentry.captureMessage('Cant delete a created file, reason %s' % str(e), level='warning', extra={'files': [itemsets]})

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise

    def share_forms(self, username):
        try:
            onadata = Onadata(settings.ONADATA_URL, settings.ONADATA_MASTER)

            # share the forms with the new users
            all_peeps = list(Recipients.objects.filter(nick_name=username).values('designation', 'nick_name').all())
            permissions = []
            for peep in all_peeps:
                if peep['designation'] != 'enumerator': continue
                permissions.append({'role': 'dataentry', 'username': peep['nick_name'].lower()})

            form_prefixes = '|'.join(settings.FORMS_PREFIXES)
            shared_forms = onadata.share_project_forms(form_prefixes, permissions)

            return shared_forms

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise