import logging
import json
import os
import sys
import traceback
from threading import Thread

from utils import CDCConfigs
from kinesis import KinesisDataSender
from postgre import SlotReplication


# Background class to each replication slot in configs.yaml
class CDCApp(Thread):

    def __init__(self, kns_data_sender, slot_replication):
        super(CDCApp, self).__init__()
        self.kns_data_sender = kns_data_sender
        self.slot_replication = slot_replication
        self.last_stream_value = ''

    def _consume_stream(self, data):
        payload = json.loads(data.payload)

        if payload.get('change'):
            self.kns_data_sender.send_to_queue(data)

    def run(self):
        try:
            self.slot_replication.start_stream(self._consume_stream)

        except Exception as e:
            logging.error('Error on stream connection: {}'.format(str(e)))
            traceback.print_exc()

        finally:
            # Terminate all running threads and restart execution when got exception
            python = sys.executable
            os.execl(python, python, *sys.argv)
            sys.exit()

def main():

    replication_slots = CDCConfigs.get_config('replication_slots')

    for slot in replication_slots:

        slot_name = [key for key in slot][0] # Get slot name of replication instance on configs.yaml

        slot_replication = SlotReplication(
            slot_name=slot_name,
            db_name=slot[slot_name]['db_name'],
            host=slot[slot_name]['host'],
            user=slot[slot_name]['user'],
            password=slot[slot_name]['password'],
            options=slot[slot_name]['options']
        )

        kns_data_sender = KinesisDataSender(slot[slot_name]['kns_stream'], slot_replication)

        cdc_app = CDCApp(kns_data_sender, slot_replication)
        cdc_app.start()

if __name__ == '__main__':
    main()

