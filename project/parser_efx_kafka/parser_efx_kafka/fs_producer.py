# coding=utf-8
"""Producer для записи данных на файловую систему.

"""
# Standard library imports
from datetime import datetime
import os
import sys
import traceback

# Third party imports

# Local application imports


class FSProducer:

    def __init__(self, logger, influxdb_client, email_notification):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB
            email_notification (EmailNotification): объект для отправки email уведомлений

        """
        self.logger = logger
        self.influxdb_client = influxdb_client
        self.email_notification = email_notification

    def write_deal(self, deal, table, execute_timestamp, messages_dir):
        """Функция записи сделки в файл.
        Группировка сделок идёт по дате изменения сделки и по типологии.

        Args:
            deal (str): сделка
            table (str): таблица сделки
            execute_timestamp (datetime): дата время изменения
            messages_dir (str): директория для сохраняемых файлов

        """

        try:
            date_dir = datetime.strptime(execute_timestamp, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y%m%d")
            message_dir = os.path.join(messages_dir, date_dir)
        except TypeError as te:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while converting datetime field: {0} to name of directory\n{1}\n{2}"
                .format(execute_timestamp, te, traceback.extract_tb(exc_traceback))
            )
            self.influxdb_client.write_error(module="FS_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)

        try:
            if not os.path.exists(message_dir):
                os.makedirs(message_dir)
        except OSError as ose:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while checking and creating directory\n{0}\n{1}"
                .format(ose, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="FS_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)

        try:
            if not table:
                table = "UNKNOWN"
                self.logger.error("Deal has unknown table")
                self.influxdb_client.write_error(module="FS_PRODUCER_UNKNOWN")
            message_file_name = os.path.join(message_dir, table)
            with open(message_file_name, 'a', encoding='utf-8') as table_file:
                table_file.write(str(deal) + "\n")
        except IOError as ioe:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while writing deals into file {0}\n{1}\n{2}"
                .format(message_file_name, ioe, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="FS_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
        except UnicodeError as ue:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while writing deals into file {0}\n{1}\n{2}"
                .format(message_file_name, ue, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="FS_PRODUCER")
            self.email_notification.send_error_notification()
            sys.exit(1)
