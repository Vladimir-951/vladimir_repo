# coding=utf-8
"""Здесь будет комментарий

"""
# Standard library imports
import argparse
from datetime import datetime, time
import os
import time as t

# Third party imports

# Local application imports
import efx_consumer as ec
import email_notification as en
import helpers
import influxdb_producer
import kafka_producer as kp


def read_from_efx():
    """Функция чтения данных из сокета

    """
    # Создаём объекты классов
    influxdb_client = influxdb_producer.InfluxBDProducer(env_type=env_type, logger=logger, cfg=cfg, root_path=root_path)
    email_notification = en.EmailNotification(env_type=env_type, logger=logger, cfg=cfg,
                                              influxdb_client=influxdb_client)
    kafka_producer = kp.KafkaProducer(logger=logger, cfg=cfg, influxdb_client=influxdb_client,
                                      email_notification=email_notification)
    efx_consumer = ec.EfxConsumer(logger=logger, cfg=cfg, email_notification=email_notification,
                                  influxdb_client=influxdb_client)

    data = efx_consumer.receive_data()

    for el in data:
        for json in el:
            if kafka_producer.write_message(topic=cfg['kafka_broker']['producer_topic_deals'], message=json):
                influxdb_client.write_deal()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    env_list = ['dev', 'sit', 'prd']

    parser.add_argument(
        '-env',
        choices=env_list,
        metavar=str(env_list),
        required=True,
        help='parser environment type'
    )

    # Парсинг аргументов
    args = vars(parser.parse_args())
    env_type = args['env']

    root_path = os.path.dirname(os.path.realpath(__file__))
    cfg = helpers.parse_config(env_type=env_type, root_path=root_path)
    logger = helpers.create_timed_rotating_log(cfg=cfg, root_path=root_path)

    start_time = datetime.combine(datetime.now().date(), time(4, 0, 0))
    if datetime.now() < start_time:
        logger.info('service will be starting at 04:00:00')
        t.sleep((start_time - datetime.now()).seconds)

    read_from_efx()
