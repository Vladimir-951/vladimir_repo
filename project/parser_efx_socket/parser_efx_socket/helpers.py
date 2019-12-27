# coding=utf-8
"""Вспомогательные функции и классы

"""
# Standard library imports
from email.mime.text import MIMEText
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import smtplib

# Third party imports
from cryptography.fernet import Fernet
import yaml

# Local application imports


def decipher_password(password_type, env_type, root_path):
    """Функция дешифровки пароля.

    Args:
        password_type (str): программа для которой нужно расшифровать пароль (influxdb, mssql)
        env_type (str): префикс среды (dev, sit, prd)
        root_path (str): корневая директория запуска программы

    Returns:
        plain_text_encrypted_password (str): Расшифрованный пароль

    """
    folder_path = os.path.join(root_path, "security")
    with open(os.path.join(folder_path, '{}_key.bin'.format(password_type)), 'rb') as file_object:
        for line in file_object:
            key = line
    cipher_suite = Fernet(key)
    with open(os.path.join(folder_path, '{0}_{1}_password.bin'.format(env_type, password_type)), 'rb') as file_object:
        for line in file_object:
            encrypted_pwd = line
    uncipher_text = (cipher_suite.decrypt(encrypted_pwd))
    plain_text_encrypted_password = bytes(uncipher_text).decode("utf-8")
    return plain_text_encrypted_password


def create_timed_rotating_log(cfg, root_path):
    """Функция создания логера.
    Каждую ночь происходит создания нового файла лога.

    Args:
        cfg (dict): словарь параметров
        root_path (str): корневая директория запуска программы

    Returns:
        (logger): объект логера

    """
    logger = logging.getLogger("time_rotating_log")
    formatter = logging.Formatter(fmt='%(asctime)-22s %(levelname)-7s %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
    logger.setLevel(logging.INFO)

    log_file_path = os.path.join(os.path.join(root_path, cfg['logger_stream']['log_dir']),
                                 cfg['logger_stream']['log_file'])
    handler = TimedRotatingFileHandler(filename=log_file_path,
                                       when="midnight",
                                       interval=1,
                                       backupCount=14,
                                       encoding='utf-8')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    handler.suffix = '%Y%m%d'

    logger.addHandler(handler)

    return logger


def parse_config(env_type, root_path):
    """Функция парсинга конфигурационного файла

    Args:
        env_type (str): тип среды запуска программы
        root_path (str): корневая директория запуска программы

    Returns:
        (dict): набор параметров из конфигурационного файла

    """
    with open(os.path.join(root_path, "config.yml"), "r") as config_file:
        return yaml.load(config_file)[env_type]


def send_email(message, subject, email_from, email_to, email_list, logger, cfg, influxdb_client):
    """Функция отправки email сообщения

    Args:
        message (str): текст сообщения
        subject (str): тема сообщения
        email_from (str): заполнение поля from
        email_to (str): заполнение поля to
        email_list (list): список адресатов
        logger (TimedRotatingLogger): логер
        cfg (dict): словарь параметров
        influxdb_client (InfluxBDProducer): объект influxdb producer

    """
    try:
        server = smtplib.SMTP(cfg['email']['smtp_host'], cfg['email']['smtp_port'])
        msg = MIMEText(message, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to
        server.ehlo()
        server.sendmail(email_from, email_list, str(msg))
    except Exception as e:
        logger.error(
            "EMAIL: Error occurred while sending email\n{0}".format(e))
        influxdb_client.write_error(module="EMAIL_SENDER")
