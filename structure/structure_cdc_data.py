"""
    Structure the data to be interpreted by Kinesis Data Stream consumer
"""
from utils import convert_columns_values_to_dict


class StructureCDCData:

    def __init__(self, db_name, payload, change):
        self.data = {
            'kind': change.get('kind'),
            'database': db_name,
            'schema': change.get('schema'),
            'table': change.get('table'),
            'timestamp': payload.get('timestamp')
        }
        self.columns = change.get('columnnames')

        self.values = change.get('columnvalues')

    def get_structured_data(self):
        self.data.update({
            'data': convert_columns_values_to_dict(self.columns, self.values)
        })

        return self.data
