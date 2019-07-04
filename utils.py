"""
    Contains useful methods
"""
import json
import string
import traceback
import yaml


def convert_columns_values_to_dict(columns, values):
    result = dict()

    for i, column in enumerate(columns):
        result.update({
            column: values[i]
        })

    return result


def chunk_list_to_kinesis(l, n, p):
    lst = list()
    for i in range(0, len(l), n):
        chunk = l[i:i + n]
        lst.append([{'Data': json.dumps(c), 'PartitionKey': p} for c in chunk])

    return lst


def is_hex(s):
    hex_digits = set(string.hexdigits)
    return all(c in hex_digits for c in s)


def _get_lsn_file_content(f, write=False):
    value = '0/0'

    try:
        lsn = f.read()
        lsn_parts = lsn.split('/')

        if len(lsn_parts) == 2 and \
                is_hex(lsn_parts[0]) and \
                is_hex(lsn_parts[1]):
            value = lsn
        if write:
            f.seek(0)
            f.write(value)
    except ValueError as e:
        print('Invalid value stored in {} file.'.format(f.name))
        print(str(e))
        traceback.print_exc()
    finally:
        return value


def get_last_lsn_by_slot_name(slot_name):
    try:
        with open('./postgres/lsns/{}.txt'.format(slot_name), 'r+') as f:
            return _get_lsn_file_content(f)
    except FileNotFoundError:
        # If LSN file still does not exist, it going to be created
        pass

    with open('./postgres/lsns/{}.txt'.format(slot_name), 'w+') as f:
        return _get_lsn_file_content(f, True)


def load_config_file():
    config_file = None

    try:
        with open('./configs.yml', 'r') as db_configs:
            config_file = yaml.safe_load(db_configs)
    except yaml.YAMLError:
        print('Error when reading file configs.yml')
        pass

    return config_file
