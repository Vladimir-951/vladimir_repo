# coding=utf-8
"""Здесь будет комментарий

"""
# Standard library imports
import argparse
from collections import deque
from datetime import datetime
import multiprocessing as mp
import os
import sys
from time import sleep, time

# Third party imports

# Local application imports
import attribute_functions as af
import email_notification as en
import fs_consumer
import fs_producer
import json_parser as jp
import helpers
import influxdb_producer
import kafka_consumer as kc
import kafka_producer as kp
import mssql_producer as mssql


def develop():
    """Функция чтения данных из сокета

    """
    # Создаём объекты классов
    attribute_functions = af.AttributeFunctions()
    influxdb_client = influxdb_producer.InfluxBDProducer(env_type=env_type,
                                                         logger=logger,
                                                         cfg=cfg,
                                                         root_path=root_path)
    email_notification = en.EmailNotification(env_type=env_type,
                                              logger=logger,
                                              cfg=cfg,
                                              influxdb_client=influxdb_client)
    mssql_producer_row_insert = mssql.MSSQLProducer(env_type=env_type,
                                                    logger=logger,
                                                    cfg=cfg,
                                                    influxdb_client=influxdb_client,
                                                    root_path=root_path,
                                                    email_notification=email_notification)
    kafka_producer = kp.KafkaProducer(logger=logger,
                                      cfg=cfg,
                                      influxdb_client=influxdb_client,
                                      email_notification=email_notification)
    kafka_consumer = kc.KafkaConsumer(logger=logger,
                                      cfg=cfg,
                                      influxdb_client=influxdb_client,
                                      email_notification=email_notification)
    file_producer = fs_producer.FSProducer(logger=logger,
                                           influxdb_client=influxdb_client,
                                           email_notification=email_notification)
    json_parser = jp.JsonParser(logger=logger,
                                cfg=cfg,
                                email_notification=email_notification,
                                influxdb_client=influxdb_client,
                                env_type=env_type,
                                attribute_functions=attribute_functions,
                                mode_type=mode_type,
                                file_producer=file_producer)

    error_message_list = deque(maxlen=9)

    for i in range(100000):
        # Читаем сообщение из очереди Kafka
        deal = kafka_consumer.receive_message()
        if deal:
            # Запоминаем партицию и оффсет
            message_partition = deal.partition()
            message_offset = deal.offset()

            key = str(message_partition) + '_' + str(message_offset)

            result_dict = json_parser.parse_deal(tables_metadata=tables_metadata, deal=deal)
            if result_dict:
                sql = result_dict['sql_query']
                table = result_dict['table_name'].lower()
                content = str(result_dict['content'])
                headers = {'table': table}
                if mssql_producer_row_insert.execute_row(table, sql):
                    kafka_consumer.consumer.commit(message=deal, asynchronous=True)
                else:
                    # Пытаемся обработать сообщение несколько раз, если не получается, выходим с ошибкой
                    error_message_list.append(key)
                    influxdb_client.write_error(module="UNACKNOWLEDGED_MESSAGE")
                    if error_message_list.count(key) == 3:
                        # Записываем ошибочное сообщение в Kafka в топик ошибок
                        if kafka_producer.write_message(topic=cfg['kafka_broker']['producer_topic_errors'],
                                                        key=key,
                                                        message=content,
                                                        headers=headers):
                            kafka_consumer.consumer.commit(message=deal, asynchronous=True)
                            email_notification.send_error_notification()
                            sys.exit(1)
                        else:
                            email_notification.send_error_notification()
                            sys.exit(1)
                    else:
                        kafka_consumer.seek_message(partition=message_partition, offset=message_offset)
                    sleep(1)

    kafka_consumer.consumer.close()


def stream():
    """Функция чтения данных из сокета

    """
    # Создаём объекты классов
    attribute_functions = af.AttributeFunctions()
    influxdb_client = influxdb_producer.InfluxBDProducer(env_type=env_type,
                                                         logger=logger,
                                                         cfg=cfg,
                                                         root_path=root_path)
    email_notification = en.EmailNotification(env_type=env_type,
                                              logger=logger,
                                              cfg=cfg,
                                              influxdb_client=influxdb_client)
    mssql_producer_row_insert = mssql.MSSQLProducer(env_type=env_type,
                                                    logger=logger,
                                                    cfg=cfg,
                                                    influxdb_client=influxdb_client,
                                                    root_path=root_path,
                                                    email_notification=email_notification)
    kafka_producer = kp.KafkaProducer(logger=logger,
                                      cfg=cfg,
                                      influxdb_client=influxdb_client,
                                      email_notification=email_notification)
    kafka_consumer = kc.KafkaConsumer(logger=logger,
                                      cfg=cfg,
                                      influxdb_client=influxdb_client,
                                      email_notification=email_notification)
    file_producer = fs_producer.FSProducer(logger=logger,
                                           influxdb_client=influxdb_client,
                                           email_notification=email_notification)
    json_parser = jp.JsonParser(logger=logger,
                                cfg=cfg,
                                email_notification=email_notification,
                                influxdb_client=influxdb_client,
                                env_type=env_type,
                                attribute_functions=attribute_functions,
                                mode_type=mode_type,
                                file_producer=file_producer,)

    error_message_list = deque(maxlen=9)

    while True:
        # Читаем сообщение из очереди Kafka
        deal = kafka_consumer.receive_message()
        if deal:
            # Запоминаем партицию и оффсет
            message_partition = deal.partition()
            message_offset = deal.offset()

            key = str(message_partition) + '_' + str(message_offset)

            result_dict = json_parser.parse_deal(tables_metadata=tables_metadata, deal=deal)
            if result_dict:
                sql = result_dict['sql_query']
                table = result_dict['table_name'].lower()
                content = str(result_dict['content'])
                headers = {'table': table}
                if mssql_producer_row_insert.execute_row(table, sql):
                    kafka_consumer.consumer.commit(message=deal, asynchronous=True)
                else:
                    # Пытаемся обработать сообщение несколько раз, если не получается, выходим с ошибкой
                    error_message_list.append(key)
                    influxdb_client.write_error(module="UNACKNOWLEDGED_MESSAGE")
                    if error_message_list.count(key) == 3:
                        # Записываем ошибочное сообщение в Kafka в топик ошибок
                        if kafka_producer.write_message(topic=cfg['kafka_broker']['producer_topic_errors'],
                                                        key=key,
                                                        message=content,
                                                        headers=headers):
                            kafka_consumer.consumer.commit(message=deal, asynchronous=True)
                            email_notification.send_error_notification()
                            sys.exit(1)
                        else:
                            email_notification.send_error_notification()
                            sys.exit(1)
                    else:
                        kafka_consumer.seek_message(partition=message_partition, offset=message_offset)
                    sleep(1)


def batch(env_type, cfg, tables_metadata, mode_type, start_dt, table, root_path):
    """Запуск процесса чтения данных из файловой системы и запись сообщений в MSSQL

    Args:
        env_type (str): тип среды запуска программы
        cfg (dict): словарь параметров
        tables_metadata (dict): словарь метаданных по таблице
        mode_type (str): режим запуска программы
        start_dt (str): дата - нижняя граница чтения сообщений из файлов
        table (str): таблица для парсинга из файлов
        root_path (str): полный корневой путь к программе

    """
    start_time = time()
    # Создаём объекты классов
    attribute_functions = af.AttributeFunctions()
    logger = helpers.create_timed_rotating_log(mode_type=mode_type, cfg=cfg, root_path=root_path)
    influxdb_client = influxdb_producer.InfluxBDProducer(env_type=env_type,
                                                         logger=logger,
                                                         cfg=cfg,
                                                         root_path=root_path)
    email_notification = en.EmailNotification(env_type=env_type,
                                              logger=logger,
                                              cfg=cfg,
                                              influxdb_client=influxdb_client)
    mssql_producer = mssql.MSSQLProducer(env_type=env_type,
                                         logger=logger,
                                         cfg=cfg,
                                         influxdb_client=influxdb_client,
                                         root_path=root_path,
                                         email_notification=email_notification)
    file_consumer = fs_consumer.FSConsumer(logger=logger,
                                           cfg=cfg,
                                           influxdb_client=influxdb_client,
                                           start_dt=start_dt,
                                           table=table,
                                           email_notification=email_notification)
    json_parser = jp.JsonParser(logger=logger,
                                cfg=cfg,
                                email_notification=email_notification,
                                influxdb_client=influxdb_client,
                                env_type=env_type,
                                attribute_functions=attribute_functions,
                                mode_type=mode_type)

    # формируем список файлов для загрузки
    deals_files_list = file_consumer.prepare_messages_files_list()
    # удаляем сообщения из БД начиная с даты, указанной при запуске
    mssql_producer.delete_from_table_by_date(env_type=env_type,
                                             table=table,
                                             start_dt=start_dt,
                                             tables_metadata=tables_metadata)

    for deals_file in deals_files_list:
        # Читаем сообщение из плоского файла на сервере
        deals_file_result = file_consumer.read_messages_from_file(messages_file=deals_file)
        for deal in deals_file_result:
            result_dict = json_parser.parse_deal(tables_metadata=tables_metadata, deal=deal)
            if result_dict:
                sql = result_dict['sql_query']
                table = result_dict['table_name'].lower()
                if not mssql_producer.execute_row(table, sql):
                    influxdb_client.write_error(module="BATCH_UNPARSED_MESSAGE")

    logger.info("{0}: end parsing process of typology {1}. Elapsed time: ".format(mp.current_process().name, table) +
                str(round(time() - start_time, 2)) + " seconds")


def create_processes():
    """Запуск процесса чтения данных из файловой системы и запись сообщений в MSSQL
        в режиме мультипроцессинга.
        Количество процессов равно количеству загружаемых типологий.

    """
    start_time = time()
    processes = list()

    for table in batch_tables_set:
        process_args = [
            env_type,
            cfg,
            tables_metadata,
            mode_type,
            start_dt,
            table,
            root_path
        ]

        process = mp.Process(target=batch, args=process_args)
        processes.append(process)

        logger.info("{0}: start parsing process of typology {1}".format(process.name, table))

        process.start()

    for process in processes:
        process.join()

    logger.info("Total elapsed time: " + str(round(time() - start_time, 2)) + " seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    env_list = ['dev', 'sit', 'prd']
    mode_list = ['stream', 'batch', 'develop']

    parser.add_argument(
        '-env',
        choices=env_list,
        metavar=str(env_list),
        required=True,
        help='parser environment type'
    )

    parser.add_argument(
        '-mode',
        choices=mode_list,
        metavar=str(mode_list),
        required=True,
        help='parser startup mode'
    )

    parser.add_argument(
        '-start_dt',
        metavar='YYYYMMDD',
        required=False,
        type=lambda d: datetime.strptime(d, '%Y%m%d'),
        help='start date for batch startup mode'
    )

    group_batch = parser.add_mutually_exclusive_group(required=False)

    group_batch.add_argument(
        '-all',
        required=False,
        action='store_true',
        help='parse all table in batch mode'
    )

    group_batch.add_argument(
        '-list',
        metavar='TABLE1,TABLE2',
        required=False,
        nargs='?',
        help='parse selected table in batch mode'
    )

    # Парсинг аргументов
    args = vars(parser.parse_args())
    # тип среды запуска программы
    env_type = args['env']
    # режим запуска парсера
    mode_type = args['mode']
    # дата с которой загружаем сообщения
    start_dt = args['start_dt']
    # спиисок загружаемых таблиц
    is_all = args['all']
    input_list = args['list']

    # полный корневой путь к программе
    root_path = os.path.dirname(os.path.realpath(__file__))
    # словарь с параметрами
    cfg = helpers.parse_config(env_type=env_type, root_path=root_path)
    # объект логгер
    logger = helpers.create_timed_rotating_log(mode_type=mode_type, cfg=cfg, root_path=root_path)
    # словарь метаданных по типологиям
    tables_metadata = helpers.parse_tables_metadata(root_path=root_path)

    # Парсим список типологий, который пришёл на вход
    input_tables_set = set()
    if input_list:
        input_tables_set = set(input_list.split(','))
    # Список типологий из метаданных
    metadata_tables_set = set(tables_metadata.keys())

    # Проверка аргументов
    if mode_type == 'batch' and not start_dt:
        parser.error("Start date in batch mode is not set. "
                     "Please, set start date for batch startup mode - format YYYYMMDD.")
    elif mode_type == 'batch' and is_all is False and not input_list:
        parser.error("Typology in batch mode is not set. "
                     "Please, set typology for batch startup mode.")
    elif mode_type == 'batch' and not input_tables_set.issubset(metadata_tables_set):
        parser.error("Wrong typologies is set for batch startup mode. "
                     "Please, set right typologies for batch startup mode: {0}".format(tables_metadata.keys()))
    elif mode_type == 'stream' and start_dt:
        parser.error("Start date in stream mode is set. "
                     "Please, unset start date for stream startup mode.")

    if mode_type == 'stream':
        try:
            stream()
        except KeyboardInterrupt:
            logger.error("Program in stream mode was aborted by user\n")
    elif mode_type == 'develop':
        try:
            develop()
        except KeyboardInterrupt:
            logger.error("Program in stream mode was aborted by user\n")
    elif mode_type == 'batch':
        # Готовим список типологий для режима batch
        batch_tables_set = set()
        if is_all:
            batch_tables_set = metadata_tables_set
        elif input_list:
            batch_tables_set = input_tables_set

        try:
            create_processes()
        except KeyboardInterrupt:
            logger.error("Program in batch mode was aborted by user\n")
