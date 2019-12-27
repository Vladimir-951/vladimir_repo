# coding=utf-8
"""Consumer для чтения данных из socket.

"""
# Standard library imports
import sys
import socket
import time

# Third party imports

# Local application imports


class EfxConsumer:

    def __init__(self, logger, cfg, email_notification, influxdb_client):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            email_notification (EmailNotification): объект для отправки email уведомлений
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB

        """
        self.logger = logger
        self.cfg = cfg
        self.email_notification = email_notification
        self.consumer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.influxdb_client = influxdb_client

    def get_tables(self):
        """Получаем список таблиц для записи в kafka

         Returns:
            (str): список таблиц для записи в kafka

        """
        return '\n'.join(self.cfg['efx_server']['tables']) + '\n'

    def receive_data(self):
        """Функция для чтения сообщений из socket

        yields:
            (list): список ообщений для ззаписи в kafka

        """
        try:
            self.consumer.settimeout(600.0)
            self.consumer.connect((self.cfg['efx_server']['ip'], self.cfg['efx_server']['port']))
            data_str = self.get_tables()
            self.consumer.send(data_str.encode('utf-8'))
            data = b''
            while True:
                raw_data = self.consumer.recv(4096)
                if raw_data:
                    data += raw_data
                    try:
                        result = data.decode('utf-8').split('\n')
                    except UnicodeDecodeError as decode_error:
                        self.logger.info(decode_error)
                        continue

                    result_kafka = result[:-1:]
                    data = result[-1].encode('utf-8')

                    yield result_kafka
                else:
                    time.sleep(5)
                    continue

        except socket.timeout as s_t_e:
            self.logger.error("SOCKET_TIMEOUT: something's wrong with {0}:{1}. Exception is {2}"
                              .format(self.cfg['efx_server']['ip'], self.cfg['efx_server']['port'], s_t_e))
            self.influxdb_client.write_error(module="EFX_CONSUMER_TIMEOUT")
            sys.exit(1)

        except socket.error as s_e:
            self.logger.error("SOCKET: something's wrong with {0}:{1}. Exception is {2}"
                              .format(self.cfg['efx_server']['ip'], self.cfg['efx_server']['port'], s_e))
            self.influxdb_client.write_error(module="EFX_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)

        except Exception as e:
            self.logger.error("COMMON: something's wrong with {0}:{1}. Exception is {2}"
                              .format(self.cfg['efx_server']['ip'], self.cfg['efx_server']['port'], e))
            self.influxdb_client.write_error(module="EFX_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)
