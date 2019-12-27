# coding=utf-8
"""Consumer для чтения данных из Kafka.

"""
# Standard library imports
import sys
import traceback

# Third party imports
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

# Local application imports


class KafkaConsumer:

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
        self.consumer = Consumer(self.cfg['kafka_broker']['consumer_config'])
        self.consumer.subscribe(self.cfg['kafka_broker']['consumer_topic'])

    def receive_message(self):
        """Читаем и отдаём одно сообщение из очереди со всеми атрибутами

         Returns:
            (message): сообщение из очереди со всеми атрибутами

        """
        try:
            msg = self.consumer.poll(self.cfg['kafka_broker']['poll_timeout'])

            if msg and not msg.error():
                return msg
            elif msg and msg.error().code() == KafkaError._PARTITION_EOF:
                self.logger.warning("{0} {1} reached end at offset {2}\n".
                                    format(msg.topic(), msg.partition(), msg.offset()))
            elif msg and msg.error():
                raise KafkaException(msg.error())
            elif not msg:
                self.logger.warning('No more messages, end of the queue')
            else:
                self.logger.warning('Something new (unexpected turn)')
        except KafkaError as kf:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("KafkaError\n{0}\n{1}".format(kf, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)
        except KafkaException as ke:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("KafkaException\n{0}\n{1}".format(ke, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)

    def seek_message(self, partition, offset):
        """Сбрасываем offset в конкретной partition при ошибке обработке сообщения
        для возможности повторной обработки

        Args:
            partition (int): partition, в которой необходимо сбросить offset
            offset (int): offset, на который необходимо сбросить метку

        """
        try:
            topic_partition = TopicPartition(self.cfg['kafka_broker']['consumer_topic'][0], partition, offset)

            self.consumer.seek(topic_partition)

        except KafkaError as kf:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("KafkaError\n{0}\n{1}".format(kf, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)
        except KafkaException as ke:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("KafkaException\n{0}\n{1}".format(ke, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="KAFKA_CONSUMER")
            self.email_notification.send_error_notification()
            self.consumer.close()
            sys.exit(1)
