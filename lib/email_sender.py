import logging

from lib.conn_email_server import ConnEmailServer

log = logging.getLogger(__name__)


class EmailSender:
    """Отправка отчета на email"""

    def __init__(self, settings, report):
        """Инициализация экземпляра класса.

        :param dict settings: Настройки приложения.
        :param str report: Путь к отчету.
        """
        self.report_path = report
        self.server = settings['MAIL_SERVER']
        self.port = settings['MAIL_PORT']
        self.login = settings['MAIL_LOGIN']
        self.password = settings['MAIL_PASSWORD']
        self.ssl = True if settings['MAIL_SSL'] == 'yes' else False
        self.email_to = [i.strip() for i in settings['EMAIL_TO'].split(',')]
        self.email_from = settings['EMAIL_FROM']
        self.email_subject = settings['EMAIL_SUBJECT']

    def send_email(self):
        """Отправка отчета на email"""

        conn = ConnEmailServer(self.server, self.port, self.login, self.password, imap=False, ssl=self.ssl)

        email_text = ''

        conn.send_email(self.email_from, self.email_to, self.email_subject, email_text, self.report_path)
        conn.disconnect('SMTP')
