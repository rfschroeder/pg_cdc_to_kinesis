"""
    Get configurations from configs.yml on root folder
"""
from utils import load_config_file


class CDCConfigs:

    @staticmethod
    def get_configs(cfg_type, **kwargs):
        cfg_file = load_config_file()

        config = None

        if kwargs.get('cfg_name') is not None:
            result = list(filter(lambda x: x.get(kwargs.get('cfg_name')) is not None, cfg_file.get(cfg_type)))
            if result:
                config = result[0][kwargs.get('cfg_name')]
        else:
            config = cfg_file.get(cfg_type)

        return config

    @staticmethod
    def get_config_repl_slots():
        configs = CDCConfigs.get_configs('replication_slots')

        return {[key for key in slot][0]: slot[[key for key in slot][0]] for slot in configs}
