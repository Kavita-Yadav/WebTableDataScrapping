# ## Import
# standard library imports
import logging
import yaml
import pathlib


def load_yaml_to_public_holiday_dict() -> dict:
    """
        Function that returns all the parameter from 2 standard yaml files as dictionary
        First project.yaml two folder up, at project level
        Second that complement/overrule the first at workflow level, one folder up
    """
    param_dict = dict()
    # read parameters workflow level, 1 folder above
    # and combine both with the workflow one overriding the project one
    try:
        with open(pathlib.Path(__file__).parent.absolute().parent.joinpath('yaml_config.yaml')) as info:
            param_dict.update(yaml.safe_load(info))
            info.close()
        if param_dict.get('vtest') == 'data':
            logging.debug('parameters loaded')
        else:
            logging.error('parameters not loded properly vtest not in yaml_config.yaml')
    except IOError:
        logging.error('file yaml_config.yaml can not be open')
    return param_dict