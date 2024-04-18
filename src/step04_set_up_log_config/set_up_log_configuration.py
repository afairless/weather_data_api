#! /usr/bin/env python3

import json
from pathlib import Path


def main():
    """
    Set up logging configuration in a JSON file for the weather API

    # this is a basic logging configuration
    # a more performant option would be to log the API responses from the socket
    #   using a separate Python process, as mentioned here:
    #       https://docs.python.org/3/howto/logging-cookbook.html#deploying-web-applications-using-gunicorn-and-uwsgi
    """

    log_config_filename = 'log_config.json'
    log_config_filepath = (
        Path.cwd() / 'src' / 'step05_api' / log_config_filename)

    log_path = Path.home() / 'Documents'
    log_path.mkdir(exist_ok=True, parents=True)
    log_filepath = log_path / 'weather_api.log'

    log_configuration = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(asctime)s; %(name)s; %(levelname)s; %(message)s'}},
        'handlers': {
            'console' : {
                'level': 'INFO', 
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'stream': 'ext://sys.stdout'},
            'file_handler': {
                'level': 'INFO', 
                'class': 'logging.FileHandler',
                'formatter': 'default', 
                'filename': log_filepath.__str__(), 
                'mode': 'a',
                'encoding': 'utf-8'}},
        'loggers': {
            'default': {
                'level': 'INFO', 
                'handlers': ['file_handler']}}}

    with open(log_config_filepath, 'w') as json_file:
        json.dump(log_configuration, json_file, indent=2)


if __name__ == '__main__':
    main()
