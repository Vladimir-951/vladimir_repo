# coding=utf-8
"""Класс для парсинга сообщений из kafka.

"""
# Standard library imports
from datetime import datetime
import json
from functools import reduce
import sys
import traceback

# Third party imports

# Local application imports
import helpers


class JsonParser:

    def __init__(self, logger, cfg, email_notification, influxdb_client,
                 env_type, attribute_functions, mode_type, file_producer=None):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            email_notification (EmailNotification): объект для отправки email уведомлений
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB
            env_type (str): префикс среды
            attribute_functions (AttributeFunctions): объект класса функций атрибутов
            mode_type (str): режим запуска программы
            file_producer (FSProducer): объект класса FSProducer для записи сообщений в файл

        """
        self.logger = logger
        self.cfg = cfg
        self.email_notification = email_notification
        self.influxdb_client = influxdb_client
        self.file_producer = file_producer
        self.env_type = env_type
        self.attribute_functions = attribute_functions
        self.mode_type = mode_type

    def get_attribute(self, json_record, path):
        """Функция для извлечения одного атрибута

        Args:
            json_record (str): текст сообщения
            path (dict): путь к атрибутам и функция(если есть)

        Returns:
            (str): возвращает атрибут

        """
        if 'function' in path:
            result = getattr(self.attribute_functions,
                             path['function'][0])(reduce(lambda x, y: x.get(y, {}) if isinstance(x, dict) else x[y]
                                                         , path['function'][1]
                                                         , json_record))
        elif 'path' in path:
            result = reduce(lambda x, y: x.get(y, {}) if isinstance(x, dict) else x[y], path['path'], json_record)

        else:
            result = None

        if result:
            return result
        else:
            return None

    def parse_deal(self, tables_metadata, deal):
        """Функция парсит сделку и возвращает текст запроса

        Args:
            tables_metadata (dict): словарь метаданных
            deal (str): сделка из kafka

        Returns:
            result_tuple (tuple): текст запроса, наименование таблицы, content

        """
        try:
            # Парсим сделку
            if self.mode_type in ['stream', 'develop']:
                deal_str = deal.value().decode('utf-8').replace("\r", "").replace("\n", "")
                deal = json.loads(deal_str)
            else:
                deal = json.loads(deal)

            table_name = deal['descriptor']['name']
            # получаем заголовок и текст сообщения для записи в ошибочную очередь
            content = self.get_attribute(deal, {'path': ['data']})
            if table_name in tables_metadata.keys() and content:
                metadata = tables_metadata[table_name]
                # получаем словарь со значением атрибутов
                attrs_dict = self.prepare_attributes(metadata, deal)
                # записываем сделку в файл
                if self.mode_type in ['stream', 'develop']:
                    execute_timestamp = helpers.get_execute_timestamp(table_name, attrs_dict)
                    messages_dir = self.cfg['directories']['messages_dir']
                    self.file_producer.write_deal(deal_str, table_name, execute_timestamp, messages_dir)
                # готовим текст запроса
                sql_query = self.parse_query(metadata, attrs_dict)
                # формируем словарь с текстом запроса, наименованием таблицы, текстом сообщения для очереди с ошибками
                result_dict = {'sql_query': sql_query, 'table_name': table_name, 'content': content}
                return result_dict

            elif table_name in tables_metadata.keys():
                self.logger.info("Technical deal \n {}".format(deal))
                return None
            else:
                self.logger.info("Unexpected deal was recorded in a file for history")
                execute_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                messages_dir = self.cfg['directories']['messages_dir_other']
                self.file_producer.write_deal(deal_str, table_name, execute_timestamp, messages_dir)
                return None
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while parsing attributes \n{0}\n{1}\n{2}"
                              .format(traceback.extract_tb(exc_traceback), e, deal))
            self.influxdb_client.write_error(module="JSON_PARSER")
            self.email_notification.send_error_notification()
            sys.exit(1)

    def prepare_attributes(self, metadata, deal):
        """Функция готовит атрибуты для запроса

        Args:
            metadata (dict): словарь метаданных по таблице
            deal (str): сделка из kafka

        Returns:
            attrs_dict (dict): словарь со значением атрибутов

        """
        attrs_dict = dict()
        attrs = metadata['path']
        # парсим атрибуты
        for attr in attrs:
            attrs_dict[attr] = self.get_attribute(deal, attrs[attr])
        return attrs_dict

    def parse_query(self, metadata, attrs_dict):
        """Функция для подготовки запроса в БД

        Args:
            metadata (dict): словарь метаданных по таблице
            attrs_dict (str): словарь с распаршенными атрибутами

        Returns:
            sql_query (str): текст запроса

        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        sql_stub = metadata['sql']
        fields_list = list()
        values_list = list()
        update_fields_list = list()
        for key, value in attrs_dict.items():
            if not value:
                value = 'Null'
            fields_list.append(key)
            if key in ['insert_date_time', 'modified_date_time']:
                values_list.append(str(now))
            else:
                values_list.append(str(value))
            if key != 'efx_order_id' and key != 'insert_date_time':
                update_fields_list.append('tgt.' + key + ' = src.' + key)

        fields = ", ".join(fields_list)
        values = "'" + "', '".join(values_list) + "'"
        update_values = ", ".join(update_fields_list)
        values = values.replace('\'Null\'', 'Null')  # Заменяем строку на Null (чтобы sql понимал)

        sql_query = sql_stub.replace('{env}', self.env_type) \
            .replace('{source_values}', values) \
            .replace('{source_attributes}', fields) \
            .replace('{update_attributes}', update_values)

        return sql_query
