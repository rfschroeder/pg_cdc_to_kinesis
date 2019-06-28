# pg_cdc
A Python app stream cdc events from a PostgreSQL databases to a Kinesis Data Stream through wal2json plugin.

Requirements PostgreSQL parameters:

- wal_level = logical (logical slot replication)
- max_replication_slots > 1 (Number of tasks used with slot replications)
- max_wal_senders = 1 (Number of simultaneous tasks running at the same time)
- wal_sender_timeout = 0 (Close all connections which are inactivated for a time bigger than the number specified in milliseconds. Default is 60).
