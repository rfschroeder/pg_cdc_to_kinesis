# pg_cdc_to_kinesis
A Python project stream CDC (Change Data Capture) events from PostgreSQL databases to a AWS Kinesis Data Stream through wal2json plugin.

## Requirements:

### PostgreSQL:

- PostgreSQL version > 9.6
- **in /etc/postgresql/X.X/main/postgresql.conf:**
  - wal_level = logical (logical slot replication)
  - max_replication_slots > 1 (Number of tasks used with slot replications)
  - max_wal_senders = 1 (Number of simultaneous tasks running at the same time)
  - wal_sender_timeout = 0 (Close all connections which are inactivated for a time bigger than the number specified in milliseconds. Default is 60)

### Python:

- Python version > 3.6
- pip

## Installation

After cloning repository, enter in the project folder with terminal and execute:

```shel
$ pip install -r requirements
```
## Setup logical slots replication

Parametrize **configs.yml** according to your needs of CDC streams from specific databases, schemas, tables and columns as well.

In the **replication_slots** section you provide a list of logical replication slots.


Required parameters of **replication_slots** section:
- host (database connection host)
- user (username)
- password (password)
- db_name (database name)
- options (parameters according [wal2json parameters doc](https://github.com/eulerto/wal2json))
- kns_stream (AWS Kinesis Data Stream name)

**Example:**

```yaml
replication_slots:
    - my_slot:
        host: 'localhost'
        user: postgres
        password: *****
        db_name: my_db
        options: {'include-timestamp': True, 'add-tables': 'public.*', 'include-lsn': True}
        kns_stream: 'my-kinesis-stream'
```

In the section **databases** you can provide a list of databases to be reused by **replication_slots** section.

Required parameters of **database** section:
- host (database connection host)
- user (username)
- password (password)
- db_name (database name)

**Example:**

```yaml
databases:
  - my_database: &my_db
      host: 'localhost'
      user: postgres
      password: *****
      db_name: my_db

replication_slots:
    - my_slot:
        <<: *my_db (will repeate all &my_db node)
        options: {'include-timestamp': True, 'add-tables': 'public.*', 'include-lsn': True}
        kns_stream: 'my-kinesis-stream'
```

## Running project

Open the root folder project on terminal and execute:

```shel
$ python cdc_app.py
```
You will see theses prints on terminal:

```shell
[[my_slot1] Waiting records to process. (Queue length: 0)
[[my_slot2] Waiting records to process. (Queue length: 0)
```

When data changes begins to be send to Kinesis, will print the following message on terminal:

```shell
[my_slot1] Sending changes to Kinesis: 1 of 100 chunks. (Queue length: 1)
[my_slot2] Sending changes to Kinesis: 1 of 15 chunks. (Queue length: 1)
```

> All replication slots stream works in parallel, they won't block each other during this execution.
> Each chunk represents until 500 change records (limit of Kinesis Data Stream PutRecords).
> These approaches were thought aiming a fastest delivery process.

After all chunks sent to kinesis the following message will be shown again on screen:

```shell
[[my_slot1] Waiting records to process. (Queue length: 0)
```

**Have fun!**

