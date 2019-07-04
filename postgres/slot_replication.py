"""
    Represents slots replication data
"""
import traceback

from psycopg2 import ProgrammingError
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from utils import get_last_lsn_by_slot_name


class SlotReplication(object):
    def __init__(self, slot_name, db_name, host, user, password, options=None):
        self.slot_name = slot_name
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.options = options
        self.last_lsn = get_last_lsn_by_slot_name(slot_name)

        self._create_slot_connection()

    def _create_slot_connection(self):
        db_connection = psycopg2.connect("dbname='{}' host='{}' user='{}' password='{}'"
                                         .format(self.db_name, self.host, self.user, self.password),
                                         connection_factory=LogicalReplicationConnection)

        self.db_cursor = db_connection.cursor()

        try:
            self.db_cursor.create_replication_slot(self.slot_name, output_plugin='wal2json')
        except ProgrammingError:
            print('Slot with name {} already exists.'.format(self.slot_name))

    def increment_lsn(self, next_lsn):
        try:
            with open('./postgres/lsns/{}.txt'.format(self.slot_name), 'w+') as f:
                f.seek(0)
                f.write(next_lsn)
        except EnvironmentError as e:
            print(str(e))
            traceback.print_exc()

    def get_slot_name(self):
        return self.slot_name

    def get_db_name(self):
        return self.db_name

    def start_stream(self, consumer_listener):
        if self.db_cursor:
            self.db_cursor.start_replication(
                slot_name=self.slot_name,
                options=self.options,
                decode=True,
                start_lsn=self.last_lsn
            )

            self.db_cursor.consume_stream(consumer_listener)
