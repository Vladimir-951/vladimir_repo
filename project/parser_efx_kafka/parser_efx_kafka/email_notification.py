# coding=utf-8

"""Email notification класс для отправления уведомлений на почту по различным событиям

"""
# Standard library imports
from email.mime.text import MIMEText
import smtplib
import sys
import traceback

# Third party imports

# Local application imports
import helpers


class EmailNotification:

    def __init__(self, env_type, logger, cfg, influxdb_client):
        """Конструктор класса

        Args:
            env_type (str): тип среды запуска программы
            logger (TimedRotatingLogger): логер
            cfg (dict): словарь параметров
            influxdb_client (InfluxBDProducer): объект для логирования в базу InfluxDB

        """
        self.env_type = env_type
        self.logger = logger
        self.cfg = cfg
        self.influxdb_client = influxdb_client

    def send_error_notification(self):
        """Функция отправки email сообщения в случае возникающих ошибок

        """
        logo = """
        """
        message = (
            "<html><body>"
            "<p><font color='red'><b>Critical error</b></font> "
            "occurred while running the <b>parser_efx_kafka</b> microservice.<br>"
            "Check Grafana dashboards.<br>"
            "For a detailed report, check the server logs.</p>"
            "<p>{0}</p>"
            "</body></html>"
            .format(logo)
        )
        subject = "Parser eFX Kafka ({0}) was stopped with error".format(self.env_type.upper())
        email_from = self.cfg['email']['smtp_email_from']
        email_to = ", ".join(self.cfg['email']['smtp_email_to'])
        email_list = self.cfg['email']['smtp_email_to']

        try:
            self.send_email(message=message, subject=subject, email_from=email_from,
                            email_to=email_to, email_list=email_list)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(
                "Error occurred while sending email\n{0}\n{1}"
                .format(e, traceback.extract_tb(exc_traceback)))
            self.influxdb_client.write_error(module="EMAIL_SENDER")

    def send_email(self, message, subject, email_from, email_to, email_list):
        """Функция отправки email сообщения

        Args:
            message (str): текст сообщения
            subject (str): тема сообщения
            email_from (str): заполнение поля from
            email_to (str): заполнение поля to
            email_list (list): список адресатов

        """
        server = smtplib.SMTP(self.cfg['email']['smtp_host'], self.cfg['email']['smtp_port'])
        msg = MIMEText(message, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to
        server.ehlo()
        server.sendmail(email_from, email_list, str(msg))

