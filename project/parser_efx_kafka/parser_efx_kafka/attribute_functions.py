# coding=utf-8
"""Функции и классы для расчётных атрибутов

"""
# Standard library imports
from datetime import datetime

# Third party imports

# Local application imports


class AttributeFunctions:

    def __init__(self):
        """Конструктор класса"""
        pass

    @staticmethod
    def date_utc_convert(date):
        """Функция преобразования utc даты в читаемый фомат

        Args:
            date (float): дата

        Returns:
            (str): дата в нужном формате

        """
        if date:
            return str(datetime.utcfromtimestamp(date).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        else:
            return None

    @staticmethod
    def date_convert(date):
        """Функция преобразования даты в стандартный формат

        Args:
            date (str): дата

        Returns:
            (str): дата в нужном формате

        """
        if date:
            if len(date) == 17:
                return datetime.strptime(date, "%Y%m%d-%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if len(date) in range(19, 25):
                return datetime.strptime(date, "%Y%m%d-%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            elif len(date) > 24:
                return datetime.strptime(date[:21 - len(date)],
                                         "%Y%m%d-%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            else:
                return None
        else:
            return None

    @staticmethod
    def replace_comma(string):
        """Функция преобразования даты в читаемый фомат

        Args:
            string (str): значение атрибута

        Returns:
            (str): строка с замененными кавычками

        """
        if string:
            return string.replace("'", "''")
        else:
            return None
