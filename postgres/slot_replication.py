import string
import logging
import traceback

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

def _is_hex(s):
    hex_digits = set(string.hexdigits)
    return all(c in hex_digits for c in s)


def get_file_content(f, write=False):
    value = '0/0'

    try:
        lsn = f.read()
        lsn_parts = lsn.split('/')

        if len(lsn_parts) == 2 and \
                _is_hex(lsn_parts[0]) and \
                _is_hex(lsn_parts[1]):
            value = lsn
        if write:
            f.seek(0)
            f.write(value)
    except ValueError as e:
        logging.error('Invalid value stored in {} file.'.format(f.name))
        logging.error(e)
        traceback.print_exc()
    finally:
        return value

def _get_last_lsn(slot_name):
    try:
        with open('./postgres/lsns/{}.txt'.format(slot_name), 'r+') as f:
            return get_file_content(f)
    except FileNotFoundError as e:
        logging.info(e)

        with open('./postgres/lsns/{}.txt'.format(slot_name), 'w+') as f:
            return get_file_content(f, True)


class SlotReplication(object):
    def __init__(self, slot_name, db_name, host, user, password, options=None):
        self.slot_name = slot_name
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.options = options
        self.last_lsn = _get_last_lsn(slot_name)

        self._create_slot_connection()

    def _create_slot_connection(self):
        db_connection = psycopg2.connect("dbname='{}' host='{}' user='{}' password='{}'"
                                         .format(self.db_name, self.host, self.user, self.password),
                                         connection_factory=LogicalReplicationConnection)

        self.db_cursor = db_connection.cursor()

        try:
            self.db_cursor.create_replication_slot(self.slot_name, output_plugin='wal2json')
        except Exception as e:
            logging.info('Slot with name {} already exists.'.format(self.slot_name))

    def increment_lsn(self, next_lsn):
        try:
            with open('./postgres/lsns/{}.txt'.format(self.slot_name), 'w+') as f:
                f.seek(0)
                f.write(next_lsn)
        except EnvironmentError as e:
            logging.error('Error on writting lsn to file: {}'.format(str(e)))
            traceback.print_exc()

    def get_name(self):
        return self.slot_name

    def start_stream(self, consumer_listener):
        if self.db_cursor:
            self.db_cursor.start_replication(
                slot_name=self.slot_name,
                options=self.options,
                decode=True,
                start_lsn=self.last_lsn
            )

            self.db_cursor.consume_stream(consumer_listener)