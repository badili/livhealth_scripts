from django.core.management.base import BaseCommand, CommandError
from livhealth_scripts.odk_choices_parser import ImportODKChoices, UpdateDatabase


class Command(BaseCommand):
    help = 'Performs various updates to the database based on the received reports as well as changing requirements over time'

    def add_arguments(self, parser):
        parser.add_argument(
            '--update_submitters_records',
            action='store_true',
            help='Update the record of the SCVO who submitted the record. This info is included in the data collection forms but it is currently not being processed',
        )

    def handle(self, *args, **options):

        if options['update_submitters_records']:
            updater = UpdateDatabase()
            updater.update_syndromic_submitter()
