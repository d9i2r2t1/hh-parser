import base64
import email
import imaplib
import logging
import mimetypes
import os
import quopri
import re
import smtplib
import zipfile
from datetime import datetime
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, Union, List

log = logging.getLogger(__name__)


class ConnEmailServer:
    """Коннектор к почтовому серверу."""

    def __init__(self, server: str, port: Optional[int] = None, login: Optional[str] = None,
                 password: Optional[str] = None, imap: bool = True, ssl: bool = True,
                 smtp: bool = True, smtp_server: Optional[str] = None, smtp_port: Optional[int] = None,
                 smtp_login: Optional[str] = None, smtp_password: Optional[str] = None,
                 smtp_ssl: Optional[bool] = None) -> None:
        """
        :param server: Почтовый сервер
        :param port: Порт
        :param login: Логин
        :param password: Пароль
        :param imap: Подключение по протоколу IMAP
        :param ssl: Использовать SSL
        :param smtp: Подключение по протоколу SMTP
        :param smtp_server: Почтовый сервер для подключения по SMTP. Если не указан - используется server
        :param smtp_port: Порт для подключения по SMTP. Если не указан - используется port
        :param smtp_login: Логин для подключения по SMTP. Если не указан - используется login
        :param smtp_password: Пароль для подключения по SMTP. Если не указан - используется password
        :param smtp_ssl: Использовать SSL для подключения по SMTP. Если не указан - используется ssl
        """
        self.server = server
        self.smtp_server = server if smtp_server is None else smtp_server
        self.ssl = ssl
        self.smtp_ssl = ssl if smtp_ssl is None else smtp_ssl
        smtp_port = port if smtp_port is None else smtp_port
        smtp_login = login if smtp_login is None else smtp_login
        smtp_password = password if smtp_password is None else smtp_password

        if imap:
            log.debug(f'Connecting to {self.server}{":" + str(port) if port is not None else ""} via '
                      f'IMAP{" using SSL" if self.ssl else ""}...')
            if self.ssl and port is not None:
                imap = imaplib.IMAP4_SSL(self.server, port)
            elif self.ssl and port is None:
                imap = imaplib.IMAP4_SSL(self.server)
            elif not self.ssl and port is not None:
                imap = imaplib.IMAP4(self.server, port)
            else:
                imap = imaplib.IMAP4(self.server)
            self.imap = imap
            log.debug('Connected via IMAP')

            log.debug(f'Login to {login}...')
            self.imap.login(login, password)
            log.debug('Login success')

        if smtp:
            log.debug(f'Connecting to {self.smtp_server}{":" + str(smtp_port) if smtp_port is not None else ""} '
                      f'via SMTP{" using SSL" if self.smtp_ssl else ""}...')
            if self.smtp_ssl and smtp_port is not None:
                smtp = smtplib.SMTP_SSL(self.smtp_server, smtp_port)
            elif self.smtp_ssl and smtp_port is None:
                smtp = smtplib.SMTP_SSL(self.smtp_server)
            elif not self.smtp_ssl and smtp_port is not None:
                smtp = smtplib.SMTP(self.smtp_server, smtp_port)
            else:
                smtp = smtplib.SMTP(self.smtp_server)
            self.smtp = smtp
            log.debug('Connected via SMTP')

            log.debug(f'Login to {smtp_login}...')
            self.smtp.login(smtp_login, smtp_password)
            log.debug('Login success')

        if not imap and not smtp:
            log.error('No protocol selected for connecting to the mail server')
            raise RuntimeError('No protocol selected for connecting to the mail server')

    def disconnect(self, protocol: str = 'IMAP') -> None:
        """
        Отключись от почтового сервера.
        :param protocol: Протокол: IMAP или SMTP
        """
        if protocol == 'IMAP' and hasattr(self, 'imap'):
            self.imap.logout()
            log.debug(f'Disconnected from {self.server} via IMAP')
        elif protocol == 'IMAP' and hasattr(self, 'imap') is False:
            log.error(f'Failed to disconnect from {self.server} via IMAP: no IMAP connection found')
            raise RuntimeError('Failed to disconnect via IMAP: no IMAP connection found')

        elif protocol == 'SMTP' and hasattr(self, 'smtp'):
            self.smtp.quit()
            log.debug(f'Disconnected from {self.smtp_server} via SMTP')
        elif protocol == 'SMTP' and hasattr(self, 'smtp') is False:
            log.error(f'Failed to disconnect from {self.server} via SMTP: no SMTP connection found')
            raise RuntimeError('Failed to disconnect via SMTP: no SMTP connection found')

        else:
            log.error('Protocol specified incorrectly or not supported')
            raise RuntimeError('Protocol specified incorrectly or not supported')

    def get_emails_by_subject(self, email_subject: str, folder: str) -> List[str]:
        """
        Найди сообщение в почте по теме письма.
        :param email_subject: Тема письма
        :param folder: Папка почтового ящика, в которой будут искаться письма
        :return: Список найденных uid писем
        """
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found')
            raise RuntimeError('No IMAP connection found')

        log.debug(f'Looking for mails on the topic "{email_subject}"...')
        self.imap.select(folder)
        email_subject_formated = '_'.join(email_subject.split())
        result_search, raw_msg_uids = self.imap.uid('search', None, 'SUBJECT', email_subject_formated)
        msg_uids = raw_msg_uids[0].decode('utf-8').split()

        if len(msg_uids) == 0:
            log.error(f'No mails found by topic "{email_subject}"')
            raise RuntimeError('No mails found')

        # Проверяем совпадение темы найденных писем с темой из аргумента email_subject
        strict_msg_uids = []
        for uid in msg_uids:
            result_fetch, raw_email_header = self.imap.uid('fetch', uid, 'BODY.PEEK[HEADER]')
            email_header = email.message_from_string(raw_email_header[0][1].decode('utf-8'))
            email_subject_form_header = email_header.get('subject')
            if email_subject_form_header == email_subject:
                strict_msg_uids.append(uid)

        return strict_msg_uids

    def get_email_text_string(self, uid: str, html_clean: bool = True) -> str:
        """
        Получи текст письма по его uid.
        :param uid: Уникальный идентификатор письма (uid)
        :param html_clean: Отчистка текста письма от html-разметки
        """
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found')
            raise RuntimeError('No IMAP connection found')

        result_fetch, raw_email_text = self.imap.uid('fetch', uid, 'BODY.PEEK[TEXT]')
        email_text = raw_email_text[0][1].decode('utf-8')

        try:
            email_text = base64.b64decode(email_text).decode('utf-8')
        except Exception as e:
            log.debug(f'No base64 in email text: {e}')

        try:
            email_text = quopri.decodestring(email_text).decode('utf-8')
        except Exception as e:
            log.debug(f'No MIME in email text: {e}')

        email_text = email_text.replace('\r', '').replace('\n', '')
        return re.sub(r'<[^>]*>', '', email_text) if html_clean else email_text

    def get_email_received_date_and_time(self, uid: str) -> str:
        """
        Получи время доставки письма.
        :param uid: Уникальный идентификатор письма (uid)
        :return: Время получения письма в формате '%Y-%m-%d %H:%M:%S'
        """
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found')
            raise RuntimeError('No IMAP connection found')

        result_fetch, raw_email_header = self.imap.uid('fetch', uid, 'BODY.PEEK[HEADER]')
        email_header_encode = email.message_from_string(raw_email_header[0][1].decode('utf-8'))
        email_received = email_header_encode.get('received')
        email_time_str = ','.join(email_received.split()[-5:-1])
        return datetime.strptime(email_time_str, '%d,%b,%Y,%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

    def send_email(self, email_from: str, email_to: Union[str, List[str]], subject: str,
                   text: str, files: Optional[Union[str, List[str]]] = None, add_to_zip: bool = True,
                   folder_recursion: bool = True) -> None:
        """
        Отправь письмо.
        :param email_from: Email отправителя
        :param email_to: Email получателя (получателей)
        :param subject: Тема письма
        :param text: Текст письма
        :param files: Путь к файлу (файлам) или папке (папкам)
        :param add_to_zip: Архивация вложений, если их больше 1
        :param folder_recursion: Рекурсивно обходить папки с вложениями
        """
        if type(email_to) is not str and type(email_to) is not list:
            log.error('Invalid email_to type')
            raise TypeError('Invalid email_to type')
        elif type(email_to) is str and re.search(r'.+@.+\..+', email_to) is None:
            log.error(f'Invalid email "{email_to}"')
            raise RuntimeError('Invalid email')
        elif type(email_to) is list:
            for i in email_to:
                if re.search(r'.+@.+\..+', i) is None:
                    log.error(f'Invalid email "{i}"')
                    raise RuntimeError('Invalid email')
        if re.search(r'.+@.+\..+', email_from) is None:
            log.error(f'Invalid email "{email_from}"')
            raise RuntimeError('Invalid email')
        if hasattr(self, 'smtp') is False:
            log.error('No SMTP connection found')
            raise RuntimeError('No SMTP connection found')
        if files is not None and type(files) is not str and type(files) is not list:
            log.error('Invalid files type')
            raise TypeError('Invalid files type')

        def attach_process(message: email.mime.multipart.MIMEMultipart, attach: Union[str, List[str]]) -> None:
            """
            Прикрепи вложения к письму.
            :param message: Тело письма
            :param attach: Список путей к файлам и (или) папкам
            """
            if type(attach) is str:
                attach = [attach]

            attachments = []
            for a in attach:
                if os.path.isfile(a):
                    attachments.append(
                        {'full_path': a,
                         'folder_path': os.path.basename(a)}
                    )
                elif os.path.exists(a):
                    if not folder_recursion:
                        for f in os.listdir(a):
                            if os.path.isfile(os.path.join(a, f)) and f[0] != '.':
                                attachments.append(
                                    {'full_path': os.path.join(a, f),
                                     'folder_path': os.path.join(os.path.basename(a), f)}
                                )
                    else:
                        full_main_dir = None
                        main_dir = None
                        for t, d, f in os.walk(a):
                            full_main_dir = t if full_main_dir is None else full_main_dir
                            main_dir = os.path.basename(t) if main_dir is None else main_dir
                            for file_name in f:
                                if file_name[0] != '.':
                                    folder_path = os.path.join(main_dir, file_name) \
                                        if t == full_main_dir \
                                        else os.path.join(main_dir, os.path.relpath(t, start=full_main_dir), file_name)
                                    attachments.append(
                                        {'full_path': os.path.join(t, file_name),
                                         'folder_path': folder_path}
                                    )
                else:
                    log.warning(f'Invalid attachment: "{a}"')

            if len(attachments) > 1 and add_to_zip is True:
                add_to_zip(attachments)
                attach_add(message, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'))
                os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'))
            elif len(attachments) == 0:
                log.error(f'Invalid attachments: "{attach}"')
                raise RuntimeError('Invalid attachments')
            else:
                for att in attachments:
                    attach_add(message, att.get('full_path'))

        def attach_add(message: email.mime.multipart.MIMEMultipart, filepath: str) -> None:
            """
            Добавь вложения к телу письма.
            :param message: Тело письма
            :param filepath: Путь к вложению
            """
            filename = os.path.basename(filepath)
            ctype, encoding = mimetypes.guess_type(filepath)
            if ctype is None or encoding is not None:
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)

            if maintype == 'text':
                with open(filepath) as fp:
                    file = MIMEText(fp.read(), _subtype=subtype)
            elif maintype == 'image':
                with open(filepath, 'rb') as fp:
                    file = MIMEImage(fp.read(), _subtype=subtype)
            elif maintype == 'audio':
                with open(filepath, 'rb') as fp:
                    file = MIMEAudio(fp.read(), _subtype=subtype)
            else:
                with open(filepath, 'rb') as fp:
                    file = MIMEBase(maintype, subtype)
                    file.set_payload(fp.read())
                    encoders.encode_base64(file)

            file.add_header('Content-Disposition', 'attachment', filename=filename)
            message.attach(file)

        def add_to_zip(attach_files: List[dict]) -> None:
            """
            Заархивируй файлы.
            :param attach_files: Список словарей файлов, которые необходимо добавить в архив
            """
            log.debug('Archiving files...')
            with zipfile.ZipFile(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'), 'w') as z:
                if type(attach_files) is list:
                    for f in attach_files:
                        z.write(f.get('full_path'), arcname=f.get('folder_path'), compress_type=zipfile.ZIP_DEFLATED)
                    log.debug('Files successfully added to archive')
                else:
                    log.error('Invalid type argument "attach_files"')
                    raise TypeError('Invalid type argument "attach_files"')

        log.info(f'Sending email with the subject "{subject}"...')
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to if type(email_to) is str else ', '.join(email_to)
        msg.attach(MIMEText(text, 'plain'))
        attach_process(msg, files)

        try:
            self.smtp.send_message(msg)
            log.info(f'Email "{subject}" sent to {", ".join(email_to)} from {email_from}')
        except Exception as e:
            log.error(f'Failed to send email "{subject}" to {", ".join(email_to)} from {email_from}. Error: {e}')
            raise e

    def status(self) -> dict:
        """Покажи состояние подключений."""
        conn = {}
        if not hasattr(self, "imap"):
            conn['imap'] = 'not exist'
        else:
            try:
                self.imap.noop()
                conn['imap'] = f'connected{" (SSL)" if self.ssl else ""}'
            except Exception as e:
                log.debug(f'imap.noop(): {str(e)}')
                conn['imap'] = f'disconnected{" (SSL)" if self.ssl else ""}'

        if not hasattr(self, "smtp"):
            conn['smtp'] = 'not exist'
        else:
            try:
                self.smtp.noop()
                conn['smtp'] = f'connected{" (SSL)" if self.smtp_ssl else ""}'
            except Exception as e:
                log.debug(f'smtp.noop(): {str(e)}')
                conn['smtp'] = f'disconnected{" (SSL)" if self.smtp_ssl else ""}'

        log.info(f'Current {self.server} connection status: {conn}')
        return conn
