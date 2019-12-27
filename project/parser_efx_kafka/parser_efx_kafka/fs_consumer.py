# coding=utf-8
"""Consumer для чтения данных из плоских файлов.

"""
# Standard library imports
from datetime import datetime
import os
from pathlib import Path
import sys

# Third party imports

# Local application imports


class FSConsumer:

    def __init__(self, logger, cfg, influxdb_client, email_notification, start_dt, table):
        """Конструктор класса

        Args:
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB
            email_notification (EmailNotification): объект для отправки email уведомлений
            start_dt(str): дата начала периода загрузки сообщений из файлов
            table(str): название таблицы

        """
        self.logger = logger
        self.cfg = cfg
        self.influxdb_client = influxdb_client
        self.email_notification = email_notification
        self.start_dt = start_dt
        self.table = table

    def prepare_messages_files_list(self):
        """Подготавливаем список директорий и файлов, которые необходимо прочитать.
        Список готовим с фильтром по дате и типологии

        Returns:
            (list): список файлов с сообщениями, которые необходимо распарсить

        """
        start_date_dir = self.start_dt.strftime("%Y%m%d")
        messages_dir = Path(self.cfg['directories']['messages_dir'])
        messages_files_list = list()

        try:
            if not messages_dir.exists():
                raise OSError
        except OSError as ose:
            self.logger.error(
                "Error occurred while checking directory\n{0}{1}\n".format(ose, messages_dir))
            self.influxdb_client.write_error(module="FS_CONSUMER")
            self.email_notification.send_error_notification()
            sys.exit(1)

        for messages_dir_path, dirs, files in os.walk(messages_dir, topdown=False):
            if not dirs:
                message_dir_date = datetime.strptime(Path(messages_dir_path).stem, "%Y%m%d")
                if message_dir_date >= datetime.strptime(start_date_dir, "%Y%m%d"):
                    for f in files:
                        if f == self.table:
                            messages_files_list.append(os.path.join(messages_dir_path, f))

        return messages_files_list

    def read_messages_from_file(self, messages_file):
        """Читаем сообщения из плоского файла

        Args:
            messages_file (str): путь к файлу сообщений

        Returns:
            (list): прочитаный список сообщений из файла, которые необходимо распарсить

        """
        try:
            with open(messages_file, "r", encoding='utf-8') as message_file:
                return message_file.readlines()
        except IOError as ioe:
            self.logger.error(
                "Error occurred while reading deals from file\n{0}".format(ioe))
            self.influxdb_client.write_error(module="FS_CONSUMER")
            self.email_notification.send_error_notification()
            sys.exit(1)
