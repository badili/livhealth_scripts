from django.core.management.base import BaseCommand, CommandError
from livhealth_scripts.odk_choices_parser import ImportODKChoices


class Command(BaseCommand):
    help = 'Imports the choices sheet as defined in the ODK form that we are using. The input file must be a csv delimited by a <comma> and cells quoted by <""> where necessary'

    def add_arguments(self, parser):
        parser.add_argument('--odk_choices_csv', nargs='?', type=str)
        parser.add_argument('--phone_numbers', nargs='?', type=str)

    def handle(self, *args, **options):
        parser = ImportODKChoices()
        if options['odk_choices_csv'] is None and options['phone_numbers'] is None:
            raise CommandError('Please provide an input file to process. The command syntax is:\n\tpython manage.py process_odk_choices --odk_choices_csv <path/to/files> --phone_numbers <path/to/files>')

        # for in_file in options['odk_choices_csv']:
        if options['odk_choices_csv'] is not None:
            parser.process_odk_choices_file(options['odk_choices_csv'])

        # for in_file in options['phone_numbers']:
        if options['phone_numbers'] is not None:
            parser.process_phone_numbers(options['phone_numbers'])
