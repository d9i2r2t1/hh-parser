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
from typing import Optional, Union, List, NamedTuple, Iterator

log = logging.getLogger(__name__)


class ConnEmailServer:
    """Подключение к почтовому серверу."""

    def __init__(self, host: str, port: int, login: str, password: str, use_ssl: bool) -> None:
        self.__host = host
        self.__port = port
        self.__login = login
        self.__password = password
        self.__use_ssl = use_ssl
        self.__imap_client = None
        self.__smtp_client = None

    host = property(lambda self: self.__host)
    port = property(lambda self: self.__port)
    login = property(lambda self: self.__login)
    password = property(lambda self: base64.b64encode(self.__password.encode('utf-8')))
    use_ssl = property(lambda self: self.__use_ssl)


class ConnImapEmailServer(ConnEmailServer):
    """Подключение к почтовому серверу по IMAP."""

    def __init__(self, host: str,  login: str, password: str, port: int = 993, use_ssl: bool = True) -> None:
        super().__init__(host, port, login, password, use_ssl)
        self.connect()

    def _create_imap_client(self) -> None:
        """Создай клиент подключения к почтовому серверу."""
        log.debug(f'Connecting to {self.host}:{self.port} via IMAP{" using SSL" if self.use_ssl else ""}...')
        self.__imap_client = imaplib.IMAP4_SSL(self.host, self.port) if self.use_ssl else \
            imaplib.IMAP4(self.host, self.port)
        log.debug('Connected via IMAP')

    def _authenticate(self) -> None:
        """Аутентифицируйся на сервере."""
        log.debug(f'Authenticating with login "{self.login}"...')
        self.__imap_client.login(self.login, base64.b64decode(self.password).decode('utf-8'))
        log.debug('Authenticating success')

    def connect(self):
        """Подключись к почтовому серверу."""
        self._create_imap_client()
        self._authenticate()

    def disconnect(self) -> None:
        """Отключись от сервера."""
        log.debug(f'Disconnecting from {self.host}:{self.port}...')
        self.__imap_client.logout()
        log.debug('Disconnected')

    def get_emails_uid_in_folder_by_subject(self, subject: str, folder: str = 'INBOX',
                                            use_strict_subject: bool = True) -> Optional[List[str]]:
        """
        Найди сообщения в почтовом ящике по теме письма.
        :param subject: Тема письма
        :param folder: Папка почтового ящика, в которой нужно найти письма
        :param use_strict_subject: Строгое соответствие найденных писем заданной теме
        :return: Список найденных uid писем
        """
        log.debug(f'Looking for mails in folder "{folder}" by subject "{subject}"...')
        self.__imap_client.select(folder)
        email_subject_formated = '_'.join(subject.split())
        result_search, raw_msg_uids = self.__imap_client.uid('search', None, 'SUBJECT', email_subject_formated)
        msg_uids = raw_msg_uids[0].decode('utf-8').split()

        if len(msg_uids) == 0:
            log.info(f'No mails found in folder "{folder}" by subject "{subject}"')
            return

        if not use_strict_subject:
            log.info(f'Found {len(msg_uids)} mails in folder "{folder}" by subject "{subject}"')
            return msg_uids

        strict_msg_uids = []
        for uid in msg_uids:
            result_fetch, raw_email_header = self.__imap_client.uid('fetch', uid, 'BODY.PEEK[HEADER]')
            email_subject_by_uid = email.message_from_string(raw_email_header[0][1].decode('utf-8')).get('subject')
            if email_subject_by_uid == subject:
                strict_msg_uids.append(uid)
        log.info(f'Found {len(strict_msg_uids)} mails in folder "{folder}" with strict match subject "{subject}"')
        return strict_msg_uids

    def get_email_text_by_uid(self, uid: str, folder: str = 'INBOX', clean_up_html_markup: bool = True) -> str:
        """
        Получи текст письма по его uid.
        :param uid: Уникальный идентификатор письма
        :param folder: Папка почтового ящика, в которой находится письмо с нужным uid
        :param clean_up_html_markup: Очистка текста письма от html-разметки
        """
        log.debug(f'Getting email text by uid "{uid}"...')
        self.__imap_client.select(folder)
        result_fetch, raw_email_text = self.__imap_client.uid('fetch', uid, 'BODY.PEEK[TEXT]')
        email_text = raw_email_text[0][1].decode('utf-8')
        try:
            email_text = base64.b64decode(email_text).decode('utf-8')
        except ValueError:
            pass
        try:
            email_text = quopri.decodestring(email_text).decode('utf-8')
        except ValueError:
            pass
        email_text = email_text.replace('\r', '').replace('\n', '')
        return re.sub(r'<[^>]*>', '', email_text) if clean_up_html_markup else email_text

    def get_email_received_datetime(self, uid: str, folder: str = 'INBOX') -> datetime:
        """
        Получи время доставки письма.
        :param uid: Уникальный идентификатор письма
        :param folder: Папка почтового ящика, в которой находится письмо с нужным uid
        """
        log.debug(f'Getting email received datetime by uid "{uid}"...')
        self.__imap_client.select(folder)
        result_fetch, raw_email_header = self.__imap_client.uid('fetch', uid, 'BODY.PEEK[HEADER]')
        email_received = email.message_from_string(raw_email_header[0][1].decode('utf-8')).get('received')
        email_time_str = ','.join(email_received.split()[-5:-1])
        return datetime.strptime(email_time_str, '%d,%b,%Y,%H:%M:%S')


class ConnSmtpEmailServer(ConnEmailServer):
    """Подключение к почтовому серверу по SMTP."""

    def __init__(self, host: str,  login: str, password: str, port: int = 465, use_ssl: bool = True) -> None:
        super().__init__(host, port, login, password, use_ssl)
        self.connect()

    def _create_smtp_client(self) -> None:
        """Создай клиент подключения к почтовому серверу."""
        log.debug(f'Connecting to {self.host}:{self.port} via SMTP{" using SSL" if self.use_ssl else ""}...')
        self.__smtp_client = smtplib.SMTP_SSL(self.host, self.port) if self.use_ssl else \
            smtplib.SMTP(self.host, self.port)
        log.debug('Connected via SMTP')

    def _authenticate(self) -> None:
        """Аутентифицируйся на сервере."""
        log.debug(f'Authenticating with login "{self.login}"...')
        self.__smtp_client.login(self.login, base64.b64decode(self.password).decode('utf-8'))
        log.debug('Authenticating success')

    def connect(self):
        """Подключись к почтовому серверу."""
        self._create_smtp_client()
        self._authenticate()

    def disconnect(self) -> None:
        """Отключись от сервера."""
        log.debug(f'Disconnecting from {self.host}:{self.port}...')
        self.__smtp_client.quit()
        log.debug('Disconnected')

    def send_email(self, email_from: str, email_to: Union[str, List[str]], subject: str, text: str,
                   attachments: Optional[Union[str, List[str]]] = None, add_files_to_zip: bool = True,
                   attach_files_using_folder_recursion: bool = True) -> None:
        """
        Отправь письмо.
        :param email_from: Email отправителя
        :param email_to: Email получателя (получателей)
        :param subject: Тема письма
        :param text: Текст письма
        :param attachments: Путь к файлу (файлам) или папке (папкам)
        :param add_files_to_zip: Архивация вложений, если их больше 1
        :param attach_files_using_folder_recursion: Рекурсивно обходить папки с вложениями
        """
        log.debug('Sending email...')
        self.__check_args_for_send_email(email_to, email_from, attachments)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to if type(email_to) is str else ', '.join(email_to)
        msg.attach(MIMEText(text, 'plain'))
        if attachments:
            attachments = [attachments] if type(attachments) is str else attachments
            msg = self._attach_files(msg, attachments, attach_files_using_folder_recursion, add_files_to_zip)
        self.__smtp_client.send_message(msg)
        log.info(f'Email "{subject}" sent to {", ".join(email_to)} from {email_from}')

    @staticmethod
    def __check_args_for_send_email(email_to: Union[str, List[str]], email_from: str,
                                    attachments: Optional[Union[str, List[str]]] = None) -> None:
        """Проверь аргументы для отправки письма."""
        if type(email_to) not in (str, list,):
            log.error(f'Invalid "email_to" type: {type(email_to)}. It should be str or list')
            raise TypeError(f'Invalid "email_to" type: {type(email_to)}')
        email_to = [email_to] if type(email_to) is str else email_to
        for i in email_to:
            if not re.search(r'.+@.+\..+', i):
                log.error(f'Invalid "email_to" address: "{i}"')
                raise RuntimeError(f'Invalid "email_to" address: "{i}"')
        if not re.search(r'.+@.+\..+', email_from):
            log.error(f'Invalid "email_from" address: "{email_from}"')
            raise RuntimeError(f'Invalid "email_from" address: "{email_from}"')
        if attachments and type(attachments) not in (str, list,):
            log.error(f'Invalid "attachments" type: {type(email_to)}. It should be str or list')
            raise TypeError(f'Invalid "attachments" type: {type(email_to)}')

    def _attach_files(self, mail_body: email.mime.multipart.MIMEMultipart, attachments: List[str],
                      attach_files_using_folder_recursion: bool = True,
                      add_files_to_zip: bool = True) -> email.mime.multipart.MIMEMultipart:
        """
        Прикрепи вложения к письму.
        :param mail_body: Тело письма
        :param attachments: Список путей к файлам и (или) папкам
        :param attach_files_using_folder_recursion: Рекурсивно обходить папки с вложениями
        """
        attachments_prepared_for_zip = [
            a for a in self._prepare_attachments_for_zip(attachments, attach_files_using_folder_recursion)
        ]
        if len(attachments_prepared_for_zip) > 1 and add_files_to_zip:
            zip_file_path = self._add_to_zip(attachments_prepared_for_zip)
            mail_body = self._add_file_to_mail_body(mail_body, zip_file_path)
            os.remove(zip_file_path)
        else:
            for attach in attachments_prepared_for_zip:
                mail_body = self._add_file_to_mail_body(mail_body, attach.full_path)
        return mail_body

    class AttachForZip(NamedTuple):
        """Файл, подготовленный для добавления в zip-архив."""
        full_path: str
        folder_path: str

    def _prepare_attachments_for_zip(self, attachments: List[str],
                                     attach_files_using_folder_recursion: bool = True) -> Iterator[AttachForZip]:
        """Подготовь вложения для добавления в zip-архив."""
        prepared_attachments = []
        for attach in attachments:
            if os.path.isfile(attach):
                prepared_attachments.append(self.AttachForZip(full_path=attach, folder_path=os.path.basename(attach)))
            elif os.path.exists(attach):
                if not attach_files_using_folder_recursion:
                    for file in os.listdir(attach):
                        file_path = os.path.join(attach, file)
                        if os.path.isfile(file_path) and not file.startswith('.'):
                            prepared_attachments.append(self.AttachForZip(
                                full_path=file_path,
                                folder_path=os.path.join(os.path.basename(attach), file))
                            )
                else:
                    full_main_dir = None
                    main_dir = None
                    for current_dir, dirs, files in os.walk(attach):
                        full_main_dir = current_dir if not full_main_dir else full_main_dir
                        main_dir = os.path.basename(current_dir) if not main_dir else main_dir
                        for file_name in files:
                            if not file_name.startswith('.'):
                                folder_path = os.path.join(main_dir, file_name) if \
                                    current_dir == full_main_dir else \
                                    os.path.join(main_dir, os.path.relpath(current_dir, start=full_main_dir), file_name)
                                prepared_attachments.append(self.AttachForZip(
                                    full_path=os.path.join(current_dir, file_name),
                                    folder_path=folder_path)
                                )
            else:
                log.warning(f'Invalid attachment: "{attach}"')
        yield from prepared_attachments

    @staticmethod
    def _add_to_zip(files: List[AttachForZip]) -> str:
        """
        Заархивируй файлы.
        :param files: Файлы, которые необходимо добавить в архив
        :return: Путь к созданному архиву
        """
        files_count = len(files)
        log.debug(f'Adding {files_count} files to zip...')
        zip_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip')
        with zipfile.ZipFile(zip_path, 'w') as z:
            for file in files:
                z.write(file.full_path, arcname=file.folder_path, compress_type=zipfile.ZIP_DEFLATED)
            log.debug(f'{files_count} files successfully added to archive')
        return zip_path

    @staticmethod
    def _add_file_to_mail_body(mail_body: email.mime.multipart.MIMEMultipart,
                               filepath: str) -> email.mime.multipart.MIMEMultipart:
        """
        Добавь вложение к телу письма.
        :param mail_body: Тело письма
        :param filepath: Путь к файлу
        """
        filename = os.path.basename(filepath)
        ctype, encoding = mimetypes.guess_type(filepath)
        if not ctype or encoding:
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
        mail_body.attach(file)
        return mail_body
