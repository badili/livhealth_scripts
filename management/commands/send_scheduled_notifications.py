from django.core.management.base import BaseCommand
from livhealth_scripts.notifications import Notification


class Command(BaseCommand):
    help = 'Process notifications to be sent from the system. This script sends queued messages as well as queues the messages to be sent...'

    def add_arguments(self, parser):
        parser.add_argument('--provider', nargs='?', type=str)

        parser.add_argument(
            '--send_sms',
            action='store_true',
            help='Processes and sends queued SMS notifications',
        )

        parser.add_argument(
            '--send_reports',
            action='store_true',
            help='Send emails to the necessary people with links to the necessary reports',
        )

    def handle(self, *args, **options):
        if options['send_reports']:
            queue = Notification()
            queue.send_periodic_reports()

        if options['send_sms']:
            """Select and configure the provider that the user wants to send the message with

            """
            if 'provider' in options:
                if options['provider'] is None:
                    print("No default provider selected. The bulk SMS will be spread across the defined providers")
                else:
                    print("Requested to use '%s' as the default provider" % options['provider'])
                provider = options['provider']
            else:
                provider = None

            queue = Notification()

            if provider == 'at':
                queue.configure_at()
            elif provider == 'nexmo':
                queue.configure_nexmo()
            else:
                # configure all the providers so that they can be selected randomly
                queue.configure_at()
                queue.configure_nexmo()

            queue.periodic_processing(provider)
