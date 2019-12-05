from django.db import connections
from django.conf import settings
from .terminal_output import Terminal

terminal = Terminal()

class Query():

    def __init__(self, db_name):
        self.db_name = db_name

        return None

    def register_database(self):
        terminal.tprint("Registering a new database '%s'" % self.db_name, 'warn')

        new_database = {}
        new_database['id'] = self.db_name
        new_database['ENGINE'] = settings.DATABASES['default']['ENGINE']
        new_database['NAME'] = self.db_name
        new_database['USER'] = settings.DATABASES['default']['USER']
        new_database['PASSWORD'] = settings.DATABASES['default']['PASSWORD']
        new_database['HOST'] = settings.DATABASES['default']['HOST']
        new_database['PORT'] = settings.DATABASES['default']['PORT']

        connections.databases[self.db_name] = new_database

    def execute_query(self, query, params):
        with connection.cursor() as cursor:
            cursor.execute(query, [params])
