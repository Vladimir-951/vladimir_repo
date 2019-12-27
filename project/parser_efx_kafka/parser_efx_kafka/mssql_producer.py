# coding=utf-8
"""Producer для записи данных в MSSQL.

"""
# Standard library imports
import multiprocessing as mp
import sys
import traceback

# Third party imports
import pyodbc

# Local application imports
import helpers


class MSSQLProducer:

    def __init__(self, env_type, logger, cfg, influxdb_client, root_path, email_notification):
        """Конструктор класса

        Args:
            env_type (str): тип среды запуска программы
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB
            root_path (str): полный корневой путь к программе
            email_notification (EmailNotification): объект для отправки email уведомлений

        """
        self.logger = logger
        self.cfg = cfg
        self.influxdb_client = influxdb_client
        self.root_path = root_path
        self.env_type = env_type
        self.email_notification = email_notification

        try:
            self.connection = pyodbc.connect('TRUSTED_CONNECTION=Yes' + ';'
                                             'DRIVER={' + self.cfg['mssql']['driver'] + '};'
                                             'SERVER=' + self.cfg['mssql']['server'] + ';'
                                             'DATABASE=' + self.cfg['mssql']['database'] + ';'
                                             'UID=' + self.cfg['mssql']['username'] + ';'
                                             'PWD=' + helpers.decipher_password('mssql', env_type, root_path),
                                             autocommit=self.cfg['mssql']['autocommit'])
        except pyodbc.InterfaceError as ie:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while connecting to MSSQL database: wrong driver or user or database\n{0}\n{1}"
                .format(ie, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        except pyodbc.OperationalError as oe:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while connecting to MSSQL database: network or server not accessible\n{0}\n{1}"
                .format(oe, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        self.cursor = self.connection.cursor()

    def execute_row(self, table, sql):
        """Обработка одной строки в базе

        Args:
            table (str): таблица сделки
            sql (str): выполняемый запрос к базе данных

        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            self.influxdb_client.write_deal(table=table)
            return True
        except pyodbc.DatabaseError as dbe:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while executing row into database by table {0}\n{1}\n{2}\n{3}"
                .format(table, sql, dbe, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.connection.rollback()
            return False
        except pyodbc.Error as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while executing row into database by table {0}\n{1}\n{2}\n{3}"
                .format(table, sql, e, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)

    def delete_from_table_by_date(self, env_type, table, start_dt, tables_metadata):
        """Удаление записей из таблиц типологий по дате.
        Происходит удаление всех записей, которые >= дате start_dt.
        Если записи не удалились, выходим из программы.

        Args:
            env_type (str): тип среды запуска программы
            table (str): таблица для парсинга из файлов
            start_dt (date): дата - нижняя граница чтения сообщений из файлов
            tables_metadata (dict) словарь с атрибутами

        Returns:
            (tuple): строка с атрибутами из базы

        """
        sql_delete_stub = tables_metadata[table]['sql_delete']
        sql_delete_query = sql_delete_stub.replace('{env}', env_type) \
            .replace('{batch_tz_transact_time}', start_dt.strftime("%Y%m%d"))

        try:
            self.cursor.execute(sql_delete_query)
            self.logger.info("{0}: from {1} table {2} deals were deleted".format(mp.current_process().name,
                                                                                 table,
                                                                                 self.cursor.rowcount))
            self.connection.commit()
        except pyodbc.DatabaseError as dbe:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error("Error occurred while deleting row from table: {0}\n{1}\n{2}"
                              .format(sql_delete_query, dbe, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        except pyodbc.Error as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while deleting row from table: {0}\n{1}\n{2}"
                .format(sql_delete_query, e, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="MSSQL_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
