# coding=utf-8

"""Email notification класс для отправления уведомлений на почту по различным событиям

"""
# Standard library imports

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
            "occurred while running the <b>parser_efx</b> microservice.<br>"
            "Check Grafana dashboards.<br>"
            "For a detailed report, check the server logs.</p>"
            "<p>{0}</p>"
            "</body></html>"
            .format(logo)
        )
        subject = "Parser EFX ({0}) was stopped with error".format(self.env_type.upper())
        email_from = self.cfg['email']['smtp_email_from']
        email_to = ", ".join(self.cfg['email']['smtp_email_to'])
        email_list = self.cfg['email']['smtp_email_to']

        helpers.send_email(message=message, subject=subject, email_from=email_from,
                           email_to=email_to, email_list=email_list, logger=self.logger,
                           cfg=self.cfg, influxdb_client=self.influxdb_client)
