"""
    Sends data to Kinesis Data Stream
"""
import json
import threading
import time

import boto3

from utils import chunk_list_to_kinesis
from structure import StructureCDCData


class KinesisDataSender(object):
    def __init__(self, stream_name, slot_replication, interval=0.5):
        self.kns_client = boto3.client('kinesis', region_name='us-east-1')
        self.stream_name = stream_name
        self.slot_replication = slot_replication
        self.data_queue = list()
        self.interval = interval
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Killed by parent process
        thread.start()

    '''
        Add change data in queue
    '''
    def send_to_queue(self, data):
        # Add msg to queue
        self.data_queue.append(data)

    '''
        Process the queue to send data to Kinesis in parts
    '''
    def _send_to_kinesis(self):

        # Each data of queue can contains lots of db change capture data
        for data in self.data_queue:
            payload = json.loads(data.payload)

            data_kns_list = list()

            if payload.get('change'):

                payload = json.loads(data.payload)

                for change in payload.get('change'):
                    structured_data = StructureCDCData(self.slot_replication.get_db_name(), payload, change)

                    data_kns_list.append(structured_data.get_structured_data())

            # 500 is the number of kinesis put_records Records limit size
            kns_chunks = chunk_list_to_kinesis(data_kns_list, 500, 'default')

            for i, chunk in enumerate(kns_chunks):

                print('\r' + '[{}] Sending changes to Kinesis: {} of {} chunks. (Queue length: {})'.format(
                    self.slot_replication.get_slot_name(), i + 1, len(kns_chunks), len(self.data_queue)))

                kns_fail = False

                kns_res = self.kns_client.put_records(
                    StreamName=self.stream_name,
                    Records=chunk
                )

                if not kns_res.get('ResponseMetadata') or \
                        (kns_res.get('ResponseMetadata') and
                         kns_res.get('ResponseMetadata').get('HTTPStatusCode') != 200):
                    kns_fail = True

                if not kns_fail:
                    data.cursor.send_feedback(flush_lsn=data.data_start)
                    self.slot_replication.increment_lsn(payload.get('nextlsn'))

            self.data_queue = list(filter(lambda x: x != data, self.data_queue))

        self.data_queue = list()

        print(
            '\r' + '[[{}] Waiting records to process. (Queue length: {})'.format(self.slot_replication.get_slot_name(),
                                                                                 str(len(self.data_queue))))

    '''
        Keeps thread always running in background
    '''
    def run(self):
        print(
            '\r' + '[[{}] Waiting records to process. (Queue length: {})'.format(self.slot_replication.get_slot_name(),
                                                                                 str(len(self.data_queue))))

        while True:
            # Check queue length and start sending data to Kinesis
            if len(self.data_queue) > 0:
                self._send_to_kinesis()

            time.sleep(self.interval)
