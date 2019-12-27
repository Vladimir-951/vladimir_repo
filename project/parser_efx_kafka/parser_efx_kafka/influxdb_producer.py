# coding=utf-8
"""Producer для записи данных в InfluxDB.

"""
# Standard library imports
import time

# Third party imports
from influxdb import InfluxDBClient, exceptions as ie
from requests import exceptions as re
import sys
import traceback

# Local application imports
import helpers


class InfluxBDProducer:

    def __init__(self, env_type, logger, cfg, root_path):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            root_path (str): полный корневой путь к программе
            env_type (str): тип среды запуска программы

        """
        self.logger = logger
        self.cfg = cfg
        self.root_path = root_path
        self.env_type = env_type

        self.client = InfluxDBClient(host=self.cfg['influxdb']['host'],
                                     port=self.cfg['influxdb']['port'],
                                     username=self.cfg['influxdb']['username'],
                                     password=helpers.decipher_password('influxdb', env_type, root_path),
                                     database=self.cfg['influxdb']['database'],
                                     timeout=self.cfg['influxdb']['reconnect_timeout_sec'],
                                     retries=self.cfg['influxdb']['reconnect_retries']
                                     )

    def write_error(self, module):
        """Запись ошибок в базу InfluxDB

        Args:
            module (str): имя модуля, где произошла ошибка

        """
        measurement = 'efx_errors'
        json_body = [
            {
                "measurement": measurement,
                "tags": {
                    "module": module
                },
                "fields": {
                    "value": 1
                }
            }
        ]

        try:
            self.client.write_points(json_body, time_precision='u')
        except (ie.InfluxDBClientError, ie.InfluxDBServerError, re.ConnectionError) as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while writing error into InfluxDB\n{0}\n{1}"
                              .format(e, traceback.extract_tb(exc_traceback)))
            self.client.close()

    def write_deal(self, table):
        """Запись ошибок в базу InfluxDB

        Args:
            table (str): имя типологии по сделке

        """
        measurement = 'efx_deals'
        json_body = [
            {
                "measurement": measurement,
                "tags": {
                    "table": table
                },
                "fields": {
                    "value": 1
                }
            }
        ]

        try:
            self.client.write_points(json_body, time_precision='u')
        except (ie.InfluxDBClientError, ie.InfluxDBServerError, re.ConnectionError) as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while writing deal into InfluxDB\n{0}\n{1}"
                              .format(e, traceback.extract_tb(exc_traceback)))
            self.client.close()
