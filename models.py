# import datetime

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.core.validators import RegexValidator, MaxLengthValidator, MinLengthValidator
from django.conf import settings

from django.contrib.postgres.fields import JSONField

# since we dont want microseconds in our times, lets disable that
# check https://stackoverflow.com/questions/46539755/how-to-add-datetimefield-in-django-without-microsecond?noredirect=1&lq=1 for more info
# from django.db.backends.mysql.base import DatabaseWrapper
# DatabaseWrapper.data_types['DateTimeField'] = 'datetime'

settings.TIME_ZONE


class BaseTable(models.Model):
    """
    Base abstract table to be inherited by all other tables
    """
    date_created = models.DateTimeField(auto_now=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Model(BaseTable):
    """
    Defines the structure of the model table
    """
    # the columns for the tables
    model_name = models.CharField(max_length=100)

    class Meta:
        db_table = '__models'

    def publish(self):
        self.save()


class Attribute(BaseTable):
    """
    Defines the structure of the attributes table
    """
    _name = models.CharField(max_length=100)
    _type = models.CharField(max_length=50)
    _size = models.SmallIntegerField()
    _model = models.ForeignKey('Model', on_delete=models.PROTECT)

    class Meta:
        db_table = '__attributes'

    def publish(self):
        self.save()


class ODKForm(BaseTable):
    # Define the structure of the form table
    form_id = models.IntegerField(unique=True)
    form_name = models.CharField(max_length=200, unique=True)
    full_form_id = models.CharField(max_length=200, unique=True)
    structure = JSONField(null=True)
    processed_structure = JSONField(null=True)
    auto_update = models.BooleanField(default=False)
    is_source_deleted = models.BooleanField(default=False)

    class Meta:
        db_table = 'odkform'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class RawSubmissions(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm, on_delete=models.PROTECT)
    uuid = models.CharField(max_length=100, unique=True)
    submission_time = models.CharField(max_length=100)
    raw_data = JSONField()

    class Meta:
        db_table = 'raw_submissions'

    def publish(self):
        self.save()

    def get_id(self):
        return self.uuid


class FormViews(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm, on_delete=models.PROTECT)
    view_name = models.CharField(max_length=100, unique=True)
    proper_view_name = models.CharField(max_length=100)
    structure = JSONField()

    class Meta:
        db_table = 'form_views'

    def publish(self):
        self.save()

    def get_id(self):
        return self.view_name


class ViewTablesLookup(BaseTable):
    # Define the structure of the views that will be generated
    view = models.ForeignKey(FormViews, on_delete=models.PROTECT)
    table_name = models.CharField(max_length=250, unique=True)
    proper_table_name = models.CharField(max_length=250, null=True)
    hashed_name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = 'views_table_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.table_name


class ViewsData(BaseTable):
    # Define the structure of the submission table
    view = models.ForeignKey(FormViews, on_delete=models.PROTECT)
    raw_data = JSONField()

    class Meta:
        db_table = 'views_data'

    def publish(self):
        self.save()

    def get_id(self):
        return self.view


class ImagesLookup(models.Model):
    # Define the structure of the submission table
    filename = models.CharField(max_length=50, unique=True)
    species = models.CharField(max_length=50, null=True)
    breed = models.CharField(max_length=50, null=True)
    country = models.CharField(max_length=80, null=True)

    class Meta:
        db_table = 'images_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.filename


class DictionaryItems(BaseTable):
    # define the dictionary structure
    form_id = models.IntegerField()
    t_key = models.CharField(max_length=100)
    t_locale = models.CharField(max_length=50)
    t_type = models.CharField(max_length=30)
    t_value = models.CharField(max_length=1000)

    class Meta:
        unique_together = ('form_id', 't_key')
        db_table = 'dictionary_items'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key


class SyndromicIncidences(BaseTable):
    uuid = models.CharField(unique=True, max_length=100)
    datetime_reported = models.DateTimeField()
    datetime_uploaded = models.DateTimeField()
    county = models.CharField(max_length=50)
    sub_county = models.CharField(max_length=50)
    ward = models.CharField(max_length=50, null=True)
    village = models.CharField(max_length=50, null=True)
    reporter = models.CharField(max_length=50)
    scvo_reporter = models.CharField(max_length=50, null=False, blank=False, default='not_set')
    latitude = models.DecimalField(max_digits=11, decimal_places=9)
    longitude = models.DecimalField(max_digits=12, decimal_places=9)
    accuracy = models.DecimalField(max_digits=7, decimal_places=1)
    no_cases = models.IntegerField()

    class Meta:
        db_table = 'syndromic_incidences'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SyndromicDetails(BaseTable):
    incidence = models.ForeignKey('SyndromicIncidences', on_delete=models.PROTECT)
    species = models.CharField(max_length=20)
    syndrome = models.CharField(max_length=1000)
    start_date = models.DateField()
    end_date = models.DateField(null=True)
    herd_size = models.IntegerField()
    no_sick = models.IntegerField()
    no_dead = models.IntegerField()
    prov_diagnosis = models.CharField(max_length=1000)
    clinical_signs = models.CharField(max_length=1000)

    class Meta:
        db_table = 'syndromic_details'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class VillageMapping(BaseTable):
    village_code = models.CharField(max_length=50)
    ward_code = models.CharField(max_length=50)
    village_name = models.CharField(max_length=100)
    latitude = models.DecimalField(max_digits=11, decimal_places=9, null=True)
    longitude = models.DecimalField(max_digits=12, decimal_places=9, null=True)

    class Meta:
        db_table = 'village_mapping'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SubCounty(models.Model):
    sub_county_name = models.CharField(max_length=200, unique=True, blank=False, null=False)
    nick_name = models.CharField(max_length=100, unique=True, blank=False, null=False)

    class Meta:
        db_table = 's3ld_subcounty'


class Ward(models.Model):
    ward_name = models.CharField(max_length=200, blank=False, null=False)
    nick_name = models.CharField(max_length=100, unique=True, blank=False, null=False)
    sub_county = models.ForeignKey(SubCounty, on_delete=models.PROTECT)

    class Meta:
        unique_together = ('ward_name', 'sub_county')

    class Meta:
        db_table = 's3ld_ward'


class Village(models.Model):
    village_name = models.CharField(max_length=200, blank=False, null=False)
    nick_name = models.CharField(max_length=100, unique=True, blank=False, null=False)
    ward = models.ForeignKey(Ward, on_delete=models.PROTECT)

    class Meta:
        unique_together = ('village_name', 'ward')

    class Meta:
        db_table = 's3ld_village'


class NDReport(BaseTable):
    uuid = models.CharField(unique=True, max_length=100)
    datetime_reported = models.DateTimeField()
    datetime_uploaded = models.DateTimeField()
    county = models.CharField(max_length=50)
    sub_county = models.CharField(max_length=50)
    ward = models.CharField(max_length=50, null=True)
    village = models.CharField(max_length=50, null=True)
    # reporter = models.CharField(max_length=50)                # Can't find the reporter field, why??
    latitude = models.DecimalField(max_digits=11, decimal_places=9)
    longitude = models.DecimalField(max_digits=12, decimal_places=9)
    accuracy = models.DecimalField(max_digits=7, decimal_places=1)
    org_survey = models.CharField(max_length=50, null=True)
    nd_date_started = models.DateField()
    nd_date_reported = models.DateField()

    class Meta:
        db_table = 'nd_reports'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class NDDetail(BaseTable):
    nd_report = models.ForeignKey('NDReport', on_delete=models.PROTECT)
    disease = models.CharField(max_length=1000)
    species = models.CharField(max_length=30)
    diagnosis_type = models.CharField(max_length=50)
    production_system = models.CharField(max_length=30)
    is_zoonotic = models.BooleanField(null=True, blank=True)
    no_risk = models.IntegerField()
    no_sick = models.IntegerField()
    no_dead = models.IntegerField()
    no_slaughtered = models.IntegerField(null=True)
    measure_taken = models.CharField(max_length=30)
    no_vaccinated = models.IntegerField(null=True)

    class Meta:
        db_table = 'nd_details'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SlaughterHouse(BaseTable):
    sub_county = models.ForeignKey(SubCounty, on_delete=models.PROTECT)
    sh_name = models.CharField(max_length=200, unique=True, blank=False, null=False)


class SHReport(BaseTable):
    uuid = models.CharField(unique=True, max_length=100)
    datetime_reported = models.DateTimeField()
    datetime_uploaded = models.DateTimeField()
    report_date = models.DateField(default='2018-01-01')
    county = models.CharField(max_length=50)
    abattoir = models.CharField(max_length=100)
    latitude = models.DecimalField(max_digits=11, decimal_places=9, default=-1.2696984092022385)
    longitude = models.DecimalField(max_digits=12, decimal_places=9, default=36.726427731756985)
    accuracy = models.DecimalField(max_digits=7, decimal_places=1, default=1)
    reporter = models.CharField(max_length=50)
    animal_source = models.CharField(max_length=100)

    class Meta:
        db_table = 'sh_reports'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SHSpecies(BaseTable):
    sh_report = models.ForeignKey('SHReport', on_delete=models.PROTECT)
    specie = models.CharField(max_length=50)
    no_slaughtered = models.IntegerField()
    no_condemned = models.IntegerField()

    class Meta:
        db_table = 'sh_species'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SHParts(BaseTable):
    sh_specie = models.ForeignKey('SHSpecies', on_delete=models.PROTECT)
    body_part = models.CharField(max_length=50)
    lesions = models.CharField(max_length=1000)
    no_condemned = models.IntegerField()
    sample_collected = models.BooleanField(default=False)

    class Meta:
        db_table = 'sh_body_parts'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class AGReport(BaseTable):
    uuid = models.CharField(unique=True, max_length=100)
    datetime_reported = models.DateTimeField()
    datetime_uploaded = models.DateTimeField()
    report_date = models.DateField()
    county = models.CharField(max_length=50)
    agrovet_name = models.CharField(max_length=100)
    outlet_name = models.CharField(max_length=100)
    latitude = models.DecimalField(max_digits=11, decimal_places=9, default=-1.2696984092022385)
    longitude = models.DecimalField(max_digits=12, decimal_places=9, default=36.726427731756985)
    accuracy = models.DecimalField(max_digits=7, decimal_places=1, default=1)

    class Meta:
        db_table = 'ag_reports'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class AGDetail(BaseTable):
    ag_report = models.ForeignKey('AGReport', on_delete=models.PROTECT)
    syndrome = models.CharField(max_length=150)
    syndrome_start_date = models.DateField()
    drug_sold = models.CharField(max_length=150)
    drug_quantity = models.CharField(max_length=50)
    farmer_location = models.CharField(max_length=150)

    class Meta:
        db_table = 'ag_detail'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class Campaign(BaseTable):
    """ Campaign details

    All campaigns details will be saved in this model
    """
    campaign_name = models.CharField(max_length=100, blank=False, unique=True, validators=[
        MaxLengthValidator(100, message='The campaign name must not be more than 100 characters'),
        MinLengthValidator(3, message='The campaign name must be more than 3 chaaracters')
    ])
    # the recepients are a comma separated field with the recepients' designation
    recipients = models.CharField(max_length=1000, blank=True, unique=False, null=True, validators=[
        MaxLengthValidator(1000, message='The campaign recipients must not be more than 1000 characters')
    ])
    # i cant figure out how to express when the cron jobs should be ran
    # All notifications will be sent on Monday's at 8am. The script will add another determinant whether the notifications should go
    schedule_time = models.CharField(max_length=100, blank=True, unique=False, null=True, validators=[
        MaxLengthValidator(100, message='The schedule time of the job expressed in the database')
    ])
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'campaigns'


class MessageTemplates(BaseTable):
    """
    A list of message templates

    A collection of message templates to be sent. The templates can belong to a campaign or not
    """
    template_name = models.CharField(max_length=100, blank=False, unique=True, validators=[
        MaxLengthValidator(100, message='The template name must be less than 100 characters')
    ])
    template_type = models.CharField(max_length=20, blank=False, null=False, validators=[
        MaxLengthValidator(20, message='The template type must be less than 20 characters')
    ])
    template = models.CharField(max_length=5000, blank=False, validators=[
        MaxLengthValidator(5000, message='The template to be sent must not be more than 5000 characters'),
        MinLengthValidator(10, message='The template must be more than 3 characters')
    ])
    # using uuid as the unique identifier of a message since MySQL doesn't allow unique columns of text more than 255 characters
    uuid = models.CharField(max_length=36, blank=False, unique=True, validators=[
        MaxLengthValidator(36, message='The template UUID can only be 36 characters'),
        MinLengthValidator(36, message='The template UUID can only be 36 characters')
    ])
    campaign = models.ForeignKey(Campaign, on_delete=models.PROTECT, blank=True, null=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ('uuid', 'campaign')
        db_table = 'mssg_templates'

    class Meta:
        db_table = 's3ld_messagetemplates'


class Recipients(AbstractUser):
    """ Message recepients

    A master list of all our recepients. Messages will only be sent to recepients in this list
    """
    names_validator = RegexValidator(regex="^[a-zA-Z']*$", message='A name should only contain letters')
    nick_name_validator = RegexValidator(regex='^[a-zA-Z_]*$', message='A nick name should only contain letters and/or an underscore')
    other_names_validator = RegexValidator(regex="^[a-zA-Z\s'\.]*$", message='A name should only contain letters and/or a space')
    phone_validator = RegexValidator(regex='^\+\d+$', message='The phone number should be in the format +1xxxxxxxxxxx with no spaces')

    salutation = models.CharField(max_length=10, null=True, blank=True, validators=[MaxLengthValidator(10, message='The salutation must be less than 10 characters')])
    first_name = models.CharField(max_length=30, null=True, blank=True, validators=[
        MaxLengthValidator(30, message='The first name must be less than 30 characters'),
        names_validator
    ])
    other_names = models.CharField(max_length=100, null=True, blank=True, validators=[
        MaxLengthValidator(100, message='The other names must be less than 100 characters'),
        other_names_validator
    ])
    # this is the unique identifier used in the ODK forms
    username = models.CharField(max_length=200, null=False, blank=False, unique=True, validators=[
        MaxLengthValidator(200, message='The nick names must be less than 100 characters'),
        nick_name_validator
    ])
    designation = models.CharField(max_length=100, validators=[
        MaxLengthValidator(100, message='The recepient designation must be less than 100 characters'),
        nick_name_validator
    ])
    cell_no = models.CharField(max_length=15, blank=True, null=True, unique=True, validators=[
        MaxLengthValidator(15, message='The recepient phone number must not be more than 15 characters long'),
        phone_validator
    ])
    alternative_cell_no = models.CharField(max_length=15, blank=True, null=True, validators=[
        MaxLengthValidator(15, message='The recepient alternative phone number must not be more than 15 characters long'),
        phone_validator
    ])
    recipient_email = models.CharField(max_length=100, blank=True, null=True, validators=[MaxLengthValidator(100, message='The recepient email must not be more than 100 characters')])
    is_active = models.BooleanField(default=True)

    # the following will store the location association to the recepient
    # For a CDR this will be the village/ward
    # A SCVO will be a sub county
    # A SC worker will be a ward... maybe
    # The CDVS and ICT officer, the location association will be blank since they are associated at the county level
    village = models.ForeignKey(Village, on_delete=models.PROTECT, blank=True, null=True)
    ward = models.ForeignKey(Ward, on_delete=models.PROTECT, blank=True, null=True)
    sub_county = models.ForeignKey(SubCounty, on_delete=models.PROTECT, blank=True, null=True)

    class Meta:
        db_table = 'recipients'


class SMSQueue(BaseTable):
    template = models.ForeignKey(MessageTemplates, on_delete=models.PROTECT)
    # The actual message that will be sent
    message = models.CharField(max_length=1000, blank=False, validators=[
        MaxLengthValidator(1000, message='The message to be sent must not be more than 1000 characters')
    ])
    recipient = models.ForeignKey(Recipients, blank=False, on_delete=models.PROTECT)
    # the actual number the message was sent to
    recipient_no = models.CharField(max_length=15, blank=False, validators=[
        MaxLengthValidator(15, message='The recepient number must not be more than 15 characters long')
    ])
    # expecting the statuses: SCHEDULED, QUEUED, SENT, RECEIVED, FAILED
    msg_status = models.CharField(max_length=50, blank=False, validators=[
        MaxLengthValidator(50, message='The status must not be more than 15 characters long')
    ])
    # some providers provide a unique id for the sent messages, save this ID here
    provider_id = models.CharField(max_length=100, null=True, blank=True)
    schedule_time = models.DateTimeField(blank=False)                               # the time the sms is to be sent
    in_queue = models.BooleanField(blank=False, default=0)                          # is the message in the sending queue
    queue_time = models.DateTimeField(blank=True, null=True, default=None)          # the time the sms was added to the queue
    delivery_time = models.DateTimeField(blank=True, null=True, default=None)       # the time the sms was delivered to the recepient

    class Meta:
        db_table = 'sms_queue'
