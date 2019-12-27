# coding=utf-8
"""Producer для записи данных в Kafka.

"""
# Standard library imports
import sys
import traceback

# Third party imports
from confluent_kafka import Producer, KafkaException

# Local application imports


class KafkaProducer:

    def __init__(self, logger, cfg, influxdb_client, email_notification):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB
            email_notification (EmailNotification): объект для отправки email уведомлений

        """
        self.logger = logger
        self.cfg = cfg
        self.influxdb_client = influxdb_client
        self.email_notification = email_notification
        self.producer = Producer(self.cfg['kafka_broker']['producer_config'])

    @staticmethod
    def delivery_callback(err, msg):
        if err:
            raise KafkaException(err)
        else:
            pass
            # sys.stderr.write('Message delivered to {0} [{1}] @ {2}\n'.
            #                  format(msg.topic(), msg.partition(), msg.offset()))

    def write_message(self, topic, key, message, headers):
        """Запись одного сообщения в очередь Kafka

        Args:
            topic (str): имя очереди для записи сообщения
            key (str): id сообщения (message.id из Traffic)
            message (str): разобранная сделка с атрибутами и значениями в формате словаря
            headers (dict): заголовки сообщения

        """
        try:
            self.producer.produce(topic=topic,
                                  key=key,
                                  value=message,
                                  headers=headers,
                                  callback=self.delivery_callback)
            # синхронная запись сообщений в kafka
            self.producer.flush()
            return True
        except BufferError as be:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Local producer queue is full ({0} messages awaiting delivery): try again\n{1}\n{2}"
                              .format(len(self.producer), be, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            return False
        except KafkaException as ke:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while writing message into Kafka\n{0}\n{1}\n{2}"
                              .format(ke, message, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        except TypeError as te:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while writing message into Kafka\n{0}\n{1}\n{2}"
                              .format(te, message, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
