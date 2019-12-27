# coding=utf-8
"""Producer для записи данных в Kafka.

"""
# Standard library imports
import sys

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

    def write_message(self, topic, message):
        """Запись одного сообщения в очередь Kafka

        Args:
            topic (str): имя очереди для записи сообщения
            message (str): разобранная сделка с атрибутами и значениями в формате словаря

        """
        try:
            self.producer.produce(topic=topic,
                                  value=message,
                                  callback=self.delivery_callback)
            # синхронная запись сообщений в kafka
            self.producer.flush()
            return True
        except BufferError as be:
            self.logger.error("Local producer queue is full ({0} messages awaiting delivery): try again\n{1}".
                              format(len(self.producer), be))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            return False
        except KafkaException as ke:
            self.logger.error("Error occurred while writing message into Kafka\n{0}\n{1}".format(ke, message))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        except TypeError as te:
            self.logger.error("Error occurred while writing message into Kafka\n{0}\n{1}".format(te, message))
            self.influxdb_client.write_error(module="KAFKA_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
