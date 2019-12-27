# coding=utf-8
"""Класс логгера

"""
# Standard library imports
import logging
from logging.handlers import TimedRotatingFileHandler
import multiprocessing
import os
import sys
import threading
import traceback

# Third party imports

# Local application imports


class TimedRotatingLogger(logging.Handler):
    """Класс логера.
    Каждую ночь происходит создания нового файла лога.
    Для лога стриминга и для лога батча разные имена для файлов.
    Данный класс предназначен для работы в режиме мультипроцессинга.

    """
    def __init__(self, mode_type, cfg, root_path):
        """Конструктор класса

        Args:
            mode_type (str): режим запуска программы
            cfg (dict): словарь параметров
            root_path (str): полный корневой путь к программе

        """
        logging.Handler.__init__(self)

        self.mode_type = mode_type
        self.cfg = cfg
        self.root_path = root_path

        if self.mode_type in ['stream', 'develop']:
            self.log_file_path = os.path.join(os.path.join(self.root_path, self.cfg['logger_stream']['log_dir']),
                                              cfg['logger_stream']['log_file'])
        elif self.mode_type in ['batch']:
            self.log_file_path = os.path.join(os.path.join(self.root_path, self.cfg['logger_batch']['log_dir']),
                                              cfg['logger_batch']['log_file'])

        self._handler = TimedRotatingFileHandler(filename=self.log_file_path,
                                                 when="midnight",
                                                 interval=1,
                                                 backupCount=14,
                                                 encoding='utf-8')
        self.queue = multiprocessing.Queue(-1)

        t = threading.Thread(target=self.receive)
        t.daemon = True
        t.start()

    def setFormatter(self, fmt):
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                record = self.queue.get()
                self._handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            dummy = self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self._handler.close()
        logging.Handler.close(self)
