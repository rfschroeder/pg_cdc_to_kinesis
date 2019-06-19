import json
import threading
import time

import boto3


class KinesisDataSender(object):
    def __init__(self, stream_name, slot_replication, interval=0.5):
        self.kns_client = boto3.client('kinesis', region_name='us-east-1')
        self.stream_name = stream_name
        self.slot_replication = slot_replication
        self.data_queue = list()
        self.interval = interval
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True # Killed by parent process
        thread.start()

    def send_to_queue(self, data):
        # Add msg to queue
        self.data_queue.append(data)

    def _send_to_kinesis(self):

        data_queue = self.data_queue

        for i, data in enumerate(data_queue):
            payload = json.loads(data.payload)

            if payload.get('change'):

                payload = json.loads(data.payload)

                kns_fail = False

                for i, change in enumerate(payload.get('change')):
                    print('\r' + '[{}] Sending to Kinesis: {} of {} changes. (Queue length: {})'.format(
                        self.slot_replication.get_name(), i + 1, len(payload.get('change')), len(self.data_queue)))

                    kns_res = self.kns_client.put_record(
                        StreamName=self.stream_name,
                        Data=json.dumps(change),
                        PartitionKey="default"
                    )

                    if not kns_res.get('ResponseMetadata') or (
                            kns_res.get('ResponseMetadata') and kns_res.get('ResponseMetadata').get(
                            'HTTPStatusCode') != 200):
                        kns_fail = True

                if not kns_fail:
                    data.cursor.send_feedback(flush_lsn=data.data_start)
                    self.slot_replication.increment_lsn(payload.get('nextlsn'))

            self.data_queue = list(filter(lambda x: x != data, self.data_queue))

        self.data_queue = list()

    def run(self):
        """ Method that runs forever in background """
        while True:
            # Check queue length and start sending data to Kinesis
            print('\r' + '[[{}] No changes to process. (Queue length: {})'.format(self.slot_replication.get_name(),
                                                                                 str(len(self.data_queue))))

            if len(self.data_queue) > 0:
                self._send_to_kinesis()

            time.sleep(self.interval)
