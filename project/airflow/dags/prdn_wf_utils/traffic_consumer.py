# coding=utf-8
"""Consumer для чтения данных из Traffic (Apache QPID 0.10).

"""
# Standard library imports
import argparse
import os
import sys

# Third party imports

# Local application imports
from qpid.messaging import *


# Клас для обработки пустых файлов
class NotDirectoryException(Exception):
    """Класс для обработки ошибки "отсутстует директория".
    """

    def __init__(self, text):
        self.text = text


class TrafficConsumer(object):
    """Класс для тения сообщений из Traffic и записи их в плоский файл.
    """

    @staticmethod
    def acknowledge(session, msg_dict):
        """Подтверждаем сообщения

        Args:
            session (obj): Сессия
            msg_dict (dict): Лист сообщений

        """
        for msg in msg_dict['messages_list']:
            session.acknowledge(message=msg, sync=True)

    @staticmethod
    def write_messages(content_list, file_dir, file_name):
        """Записываем сообщения в файл

        Args:
            content_list (list): Лист текста сообщений
            file_dir (str): Директория для сохранения файла
            file_name (str): Наименование файла

        """
        file_name = os.path.normpath(os.path.join(file_dir, file_name))
        with open(file_name, 'a') as msg_file:
            for content in content_list:
                msg_file.write(content + '\n')

    @staticmethod
    def receive_messages(receiver, fetch_timeout_sec, connection):
        """Читаем и отдаём все сообщения из очереди со всеми атрибутами

        Args:
            receiver (obj): Приемник сообщений
            fetch_timeout_sec (int): Время ожидания сообщений
            connection (obj): Соединение с очередью

        Returns:
            (dict): Словарь с листом сообщений и листом содержимого сообщений

        """
        messages_list = list()
        content_list = list()
        while True:
            try:
                message = receiver.fetch(timeout=fetch_timeout_sec)
                messages_list.append(message)
                content_list.append(message.content.encode('utf-8').replace('\r', '').replace('\n', ''))
            except FetchError:
                sys.stdout.write('No more messages, end of the queue')
                break
            except MessagingError, me:
                sys.stdout.write('Error occurred while reading the message\n{0}'.format(me))
                connection.close()
                sys.exit(1)

        msg_dict = {'messages_list': messages_list, 'content_list': content_list}

        return msg_dict

    @staticmethod
    def consume(server, port, queue, file_dir, file_name, protocol, timeout_sec, interval_sec,
                reconnect_limit, heartbeat_sec, fetch_timeout_sec):
        """Функция для чтения всех сообщений из очереди и записи в файл

        Args:
            server (str): Сервер для подключения
            port (int): Порт для подключения
            queue (str): Название очереди для подключения
            file_dir (str): Директория для сохранения файла
            file_name (str): Наименование файла для сохранения сообщений
            protocol (str): Протокол для подключения
            timeout_sec (int): Время ожидания переподключения
            interval_sec (int): Интервал между переподключениями
            reconnect_limit (int): Количество попыток подключения
            heartbeat_sec (int): Интервал пинга очереди
            fetch_timeout_sec (int): Время ожидания между попытками считать сообщение

        """

        try:
            if not os.path.exists(file_dir) or not os.path.isdir(file_dir):
                raise NotDirectoryException('directory is not existing')
        except NotDirectoryException, ex:
            sys.stdout.write(ex.text)
            sys.exit(1)

        connection = Connection(host=server,
                                port=port,
                                transport=protocol,
                                reconnect=True,
                                reconnect_timeout=timeout_sec,
                                reconnect_interval=interval_sec,
                                reconnect_limit=reconnect_limit,
                                heartbeat=heartbeat_sec
                                )

        try:
            connection.open()
        except Timeout, to:
            sys.stdout.write('Error occurred while connecting to Traffic\n{0}'.format(to))
            sys.exit(1)

        session = connection.session()

        try:
            receiver = session.receiver(queue)
        except NotFound, nf:
            sys.stdout.write('Error occurred while connecting to queue\n{0}'.format(nf))
            sys.exit(1)

        # Вычитываем все сообщения
        msg_dict = TrafficConsumer.receive_messages(receiver, fetch_timeout_sec, connection)

        # Записываем все сообщения в файл
        TrafficConsumer.write_messages(msg_dict['content_list'], file_dir, file_name)

        # Подтверждаем сообщения
        TrafficConsumer.acknowledge(session, msg_dict)

        connection.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('-server', required=True)
    parser.add_argument('-port', required=True)
    parser.add_argument('-queue', required=True)
    parser.add_argument('-file_dir', required=True)
    parser.add_argument('-file_name', required=True)
    parser.add_argument('-protocol', default='tcp')
    parser.add_argument('-timeout_sec', default=300)
    parser.add_argument('-interval_sec', default=20)
    parser.add_argument('-reconnect_limit', default=15)
    parser.add_argument('-heartbeat_sec', default=10)
    parser.add_argument('-fetch_timeout_sec', default=30)

    # Парсинг аргументов
    args = vars(parser.parse_args())

    server = args['server']
    port = args['port']
    queue = args['queue']
    file_dir = args['file_dir']
    file_name = args['file_name']
    protocol = args['protocol']
    timeout_sec = args['timeout_sec']
    interval_sec = args['interval_sec']
    reconnect_limit = args['reconnect_limit']
    heartbeat_sec = args['heartbeat_sec']
    fetch_timeout_sec = args['fetch_timeout_sec']

    TrafficConsumer.consume(server=server, port=port, queue=queue, file_dir=file_dir, file_name=file_name,
                            protocol=protocol, timeout_sec=timeout_sec, interval_sec=interval_sec,
                            reconnect_limit=reconnect_limit, heartbeat_sec=heartbeat_sec,
                            fetch_timeout_sec=fetch_timeout_sec)
