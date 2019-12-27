# coding=utf-8
"""Вспомогательные функции и классы

"""
# Standard library imports
import logging
import os

# Third party imports
from cryptography.fernet import Fernet
import yaml

# Local application imports
import logger as lg


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


def create_timed_rotating_log(mode_type, cfg, root_path):
    """Функция создания логера.
    Каждую ночь происходит создания нового файла лога.

    Args:
        mode_type (str): режим запуска программы
        cfg (dict): словарь параметров
        root_path (str): корневая директория запуска программы

    Returns:
        (logger): объект логера

    """
    logger = logging.getLogger("time_rotating_log")
    formatter = logging.Formatter(fmt='%(asctime)-22s %(levelname)-7s %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
    logger.setLevel(logging.INFO)

    handler = lg.TimedRotatingLogger(mode_type, cfg, root_path)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    handler.suffix = '%Y%m%d_%H%M%S'

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


def parse_tables_metadata(root_path):
    """Функция парсинга файлов метаданных по типологиям

    Args:
        root_path (str): корневая директория запуска программы

    Returns:
        (dict): набор различных метаданных по типологиям

    """
    tables_metadata = dict()
    for metadata_dir_path, dirs, files in os.walk(os.path.join(root_path, "tables_metadata"), topdown=False):
        for f in files:
            with open(os.path.join(metadata_dir_path, f), "r") as sql_metadata_file:
                yaml_data = yaml.load(sql_metadata_file)
                for section in yaml_data:
                    tables_metadata.update({section: yaml_data.get(section)})

    return tables_metadata


def get_execute_timestamp(table_name, attrs_dict):
    """Функция отправки email сообщения

    Args:
        table_name (str): наименование таблицы
        attrs_dict (dict): словарь с распаршенными артибутами

    Returns:
        execute_timestamp (str): датавремя в виде строки

    """
    if table_name == 'ClientTrades':
        return attrs_dict['execute_timestamp_human_readable']
    elif table_name == 'ClientOrders':
        return attrs_dict['update_time_human_readable']
    elif table_name == 'PositionTransferServiceData':
        return attrs_dict['timestamp_human_readable']
    elif table_name in ['SORParentOrder', 'SORChildOrder', 'SORExecution']:
        return attrs_dict['time_stamp_human_readable']
    elif table_name == 'ILPTrades':
        return attrs_dict['trade_timestamp_human_readable']
