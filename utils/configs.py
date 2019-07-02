import yaml
import logging


def _load_config_file():
    try:
        with open('./configs.yaml', 'r') as db_configs:
            config_file = yaml.safe_load(db_configs)
    except yaml.YAMLError as exc:
        logging.error('Error when reading file configs.yaml')
        config_file = None
    finally:
        return config_file


class CDCConfigs:

    @staticmethod
    def get_config(cfg_type, **kwargs):
        cfg_file = _load_config_file()

        config = None

        if kwargs.get('cfg_name') is not None:
            result = list(filter(lambda x: x.get(kwargs.get('cfg_name')) is not None, cfg_file.get(cfg_type)))
            if result:
                config = result[0][kwargs.get('cfg_name')]
        else:
            config = cfg_file.get(cfg_type)

        return config
