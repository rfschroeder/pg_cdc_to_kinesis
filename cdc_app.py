"""
    Main execution file
"""
import json
from threading import Thread

from psycopg2.extras import StopReplication

from configs import CDCConfigs
from kinesis import KinesisDataSender
from postgres import SlotReplication

"""
    Represents each replication slot
"""


class CDCApp(Thread):

    def __init__(self, kns_data_sender, slot_replication):
        super(CDCApp, self).__init__()
        self.kns_data_sender = kns_data_sender
        self.slot_replication = slot_replication

    def _consume_stream(self, data):
        payload = json.loads(data.payload)

        if payload.get('change'):
            self.kns_data_sender.send_to_queue(data)

    def run(self):
        # Always keeps streaming, even while exception is thrown
        while True:
            try:
                self.slot_replication.start_stream(self._consume_stream)
            except StopReplication:
                pass


def main():
    replication_slots = CDCConfigs.get_config_repl_slots()

    for slot in replication_slots:
        slot_replication = SlotReplication(
            slot_name=slot,
            db_name=replication_slots[slot]['db_name'],
            host=replication_slots[slot]['host'],
            user=replication_slots[slot]['user'],
            password=replication_slots[slot]['password'],
            options=replication_slots[slot]['options']
        )

        kns_data_sender = KinesisDataSender(replication_slots[slot]['kns_stream'], slot_replication)

        cdc_app = CDCApp(kns_data_sender, slot_replication)
        cdc_app.start()


if __name__ == '__main__':
    main()
