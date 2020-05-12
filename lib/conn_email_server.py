import base64
import datetime
import email
import imaplib
import logging
import mimetypes
import os
import quopri
import re
import smtplib
import zipfile
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger(__name__)


class ConnEmailServer:
    """Подключение и работа с почтовым сервером"""

    def __init__(self, server, port=None, login=None, password=None, imap=True, ssl=True, smtp=True, smtp_server=None,
                 smtp_port=None, smtp_login=None, smtp_password=None, smtp_ssl=None):
        """Инициализация экземпляра класса.

        :param srt server: Почтовый сервер.
        :param srt port: Порт (опционально).
        :param srt login: Логин (опционально).
        :param srt password: Пароль (опционально).
        :param bool imap: Подключение по протоколу IMAP. По умолчанию - True.
        :param bool ssl: Использовать SSL. По умолчанию - True.
        :param bool smtp: Подключение по протоколу SMTP. По умолчанию - True.
        :param srt smtp_server: Почтовый сервер для подключения по SMTP. Если не указан - используется server.
        :param srt smtp_port: Порт для подключения по SMTP. Если не указан - используется port.
        :param srt smtp_login: Логин для подключения по SMTP. Если не указан - используется login.
        :param srt smtp_password: Пароль для подключения по SMTP. Если не указан - используется password.
        :param bool smtp_ssl: Использовать SSL для подключения по SMTP. Если не указан - используется ssl.
        :raise: RuntimeError если не выбран хотя бы один протокол.
        :raise: RuntimeError если подключение к серверу не удалось.
        :raise: RuntimeError если неверный логин/пароль.
        """
        self.server = server
        self.smtp_server = server if smtp_server is None else smtp_server
        self.ssl = ssl
        self.smtp_ssl = ssl if smtp_ssl is None else smtp_ssl
        smtp_port = port if smtp_port is None else smtp_port
        smtp_login = login if smtp_login is None else smtp_login
        smtp_password = password if smtp_password is None else smtp_password

        # Подключаемся по IMAP
        if imap:
            log.info(f'Connecting to {self.server}{":" + port if port is not None else ""} via '
                     f'IMAP{" using SSL" if self.ssl else ""}')
            try:
                if self.ssl and port is not None:
                    imap = imaplib.IMAP4_SSL(self.server, port)
                elif self.ssl and port is None:
                    imap = imaplib.IMAP4_SSL(self.server)
                elif not self.ssl and port is not None:
                    imap = imaplib.IMAP4(self.server, port)
                else:
                    imap = imaplib.IMAP4(self.server)
                self.imap = imap
                log.info('Connected')
            except Exception as e:
                log.error(f'Failed to connect to {self.server}{":" + port if port is not None else ""} '
                          f'via IMAP{" using SSL" if self.ssl else ""}. Error: {str(e)}')
                raise RuntimeError('Failed to connect')
            log.info(f'Login to {login}')
            try:
                self.imap.login(login, password)
                log.info('Login success')
            except Exception as e:
                log.error(f'Invalid username/password. Error: {str(e)}')
                raise RuntimeError('Invalid username/password')

        # Подключаемся по SMTP
        if smtp:
            log.info(f'Connecting to {self.smtp_server}{":" + smtp_port if smtp_port is not None else ""} '
                     f'via SMTP{" using SSL" if self.smtp_ssl else ""}')
            try:
                if self.smtp_ssl and smtp_port is not None:
                    smtp = smtplib.SMTP_SSL(self.smtp_server, smtp_port)
                elif self.smtp_ssl and smtp_port is None:
                    smtp = smtplib.SMTP_SSL(self.smtp_server)
                elif not self.smtp_ssl and smtp_port is not None:
                    smtp = smtplib.SMTP(self.smtp_server, smtp_port)
                else:
                    smtp = smtplib.SMTP(self.smtp_server)
                self.smtp = smtp
                log.info('Connected')
            except Exception as e:
                log.error(f'Failed to connect to {self.smtp_server}'
                          f'{":" + smtp_port if smtp_port is not None else ""} via SMTP'
                          f'{" using SSL" if self.smtp_ssl else ""}. Error: {str(e)}')
                raise RuntimeError('Failed to connect')
            log.info(f'Login to {smtp_login}')
            try:
                self.smtp.login(smtp_login, smtp_password)
                log.info('Login success')
            except Exception as e:
                log.error(f'Invalid username/password. Error: {str(e)}')
                raise RuntimeError('Invalid username/password')

        # Если не указано хотя бы одно подключение
        if not imap and not smtp:
            log.error('No protocol selected for connecting to the mail server')
            raise RuntimeError('No protocol selected for connecting to the mail server')

    def disconnect(self, protocol='IMAP'):
        """Отключение от почтового сервера.

        :param srt protocol: Протокол. Поддерживается IMAP и SMTP. По умолчанию - IMAP.
        :raise: RuntimeError если не обнаружены активные подключения в экземпляре класса.
        :raise: RuntimeError если протокол указан неверно.
        :raise: RuntimeError если не удалось отключиться от сервера.
        """
        # Отключаемся по IMAP
        if protocol == 'IMAP' and hasattr(self, 'imap'):
            try:
                log.info(f'Disconnecting from {self.server} via IMAP')
                self.imap.logout()
                log.info('Disconnected')
            except Exception as e:
                log.error(f'Failed to disconnect from {self.server} via IMAP. Error: {str(e)}')
                raise RuntimeError('Failed to disconnect')
        elif protocol == 'IMAP' and hasattr(self, 'imap') is False:
            log.error('No IMAP connection was found in the instance of the ConnEmailServer class')
            raise RuntimeError('No IMAP connection')

        # Отключаемся по SMTP
        elif protocol == 'SMTP' and hasattr(self, 'smtp'):
            try:
                log.info(f'Disconnecting from {self.smtp_server} via SMTP')
                self.smtp.quit()
                log.info('Disconnected')
            except Exception as e:
                log.error(f'Failed to disconnect from {self.smtp_server} via SMTP. Error: {str(e)}')
                raise RuntimeError('Failed to disconnect')
        elif protocol == 'SMTP' and hasattr(self, 'smtp') is False:
            log.error('No SMTP connection was found in the instance of the ConnEmailServer class')
            raise RuntimeError('No SMTP connection')
        else:
            log.error('Protocol specified incorrectly or not supported')
            raise RuntimeError('Protocol specified incorrectly or not supported')

    def get_emails_by_subject(self, email_subject, folder):
        """Поиск сообщений в почте по теме письма.

        :param srt email_subject: Тема письма.
        :param srt folder: Папка почтового ящика, в которой будут искаться письма.
        :raise: RuntimeError если не обнаружено активное подключение по протоколу IMAP.
        :raise: RuntimeError если не удалось выбрать папку "Входящие".
        :raise: RuntimeError если возникла ошибка при выполнении поиска.
        :raise: RuntimeError если сообщения с темой не найдены.
        :raise: RuntimeError если не удалось найти письмо по uid.
        :return: Cписок найденных uid писем.
        :rtype: list.
        """
        # Проверка
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found for this instance of the ConnEmailServer class')
            raise RuntimeError('No IMAP connection')
        log.info(f'Looking for mails on the topic "{email_subject}"')

        # Выбираем папку сообщений в которой будем искать
        try:
            self.imap.select(folder)
        except Exception as e:
            log.error(f'Failed to select folder "{folder}". Error: {str(e)}')
            raise RuntimeError('Failed to select folder')

        # Заменяем пробелы на '_' в теме письма
        email_subject_formated = '_'.join(email_subject.split())

        # Ищем уникальные индексы всех сообщений с нужной темой
        try:
            result_search, raw_msg_uids = self.imap.uid('search', None, 'SUBJECT', email_subject_formated)
        except Exception as e:
            log.error(f'Failed to search mails with a subject "{email_subject}". Error: {str(e)}')
            raise RuntimeError('Failed to search mails')

        # Обрабатываем получившийся список
        msg_uids = raw_msg_uids[0].decode('utf-8').split()
        if len(msg_uids) == 0:
            log.error(f'No mails found by topic "{email_subject}"')
            raise RuntimeError('No mails found')
        else:
            # Проверяем темы найденных uid на полное совпадение с email_subject
            strict_msg_uids = []
            for uid in msg_uids:
                # Находим письмо и его header по uid
                try:
                    result_fetch, raw_email_header = self.imap.uid('fetch', uid, 'BODY.PEEK[HEADER]')
                except Exception as e:
                    log.error(f'No emails found by uid. Error: {str(e)}')
                    raise RuntimeError('No emails found by uid')
                # Преобразуем в читаемый вид
                email_header = email.message_from_string(raw_email_header[0][1].decode('utf-8'))
                # Получаем тему письма
                email_subject_form_header = email_header.get('subject')
                # Если тема письма совпадает с темой из аргумента - добавляем в финальный список
                if email_subject_form_header == email_subject:
                    strict_msg_uids.append(uid)
            return strict_msg_uids

    def get_email_text_string(self, uid, html_clean=True):
        """Получение текста письма по его uid.

        :param srt uid: Уникальный идентификатор письма (uid).
        :param bool html_clean: Отчистка текста письма от html-разметки. По умолчанию True.
        :raise: RuntimeError если не обнаружено активное подключение по протоколу IMAP.
        :raise: RuntimeError если не удалось найти письмо по uid.
        :return: Текст письма.
        :rtype: str.
        """
        # Проверка
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found for this instance of the ConnEmailServer class')
            raise RuntimeError('No IMAP connection')

        # Находим письмо и его текст по uid
        try:
            result_fetch, raw_email_text = self.imap.uid('fetch', uid, 'BODY.PEEK[TEXT]')
        except Exception as e:
            log.error(f'No email found by uid. Error: {str(e)}')
            raise RuntimeError('No email found by uid')

        # Преобразуем в читаемый вид
        email_text = raw_email_text[0][1].decode('utf-8')

        # Декодируем base64
        try:
            email_text = base64.b64decode(email_text).decode('utf-8')
        except Exception as e:
            log.debug(f'No base64 in email text: {str(e)}')

        # Декодируем MIME
        try:
            email_text = quopri.decodestring(email_text).decode('utf-8')
        except Exception as e:
            log.debug(f'No MIME in email text: {str(e)}')

        # Убираем из текста письма переносы
        email_text = email_text.replace('\r', '')
        email_text = email_text.replace('\n', '')

        # Если необходимо, убираем html-разметку
        if html_clean is True:
            email_text = re.sub(r'<[^>]*>', '', email_text)
            return email_text
        else:
            return email_text

    def get_email_recieved_date_and_time(self, uid):
        """Получение времени доставки письма.

        :param str uid: Уникальный идентификатор письма (uid).
        :raise: RuntimeError если не обнаружено активное подключение по протоколу IMAP.
        :raise: RuntimeError если не удалось найти письмо по uid.
        :return: Время получения письма в формате '%Y-%m-%d %H:%M:%S'.
        :rtype: str.
        """
        # Проверка
        if hasattr(self, 'imap') is False:
            log.error('No IMAP connection found for this instance of the ConnEmailServer class')
            raise RuntimeError('No IMAP connection')

        # Находим письмо и его header по uid
        try:
            result_fetch, raw_email_header = self.imap.uid('fetch', uid, 'BODY.PEEK[HEADER]')
        except Exception as e:
            log.error(f'No email found by uid. Error: {str(e)}')
            raise RuntimeError('No email found by uid')

        # Преобразуем в читаемый вид
        email_header_encode = email.message_from_string(raw_email_header[0][1].decode('utf-8'))

        # Получаем данные о доставке письма
        email_received = email_header_encode.get('received')

        # Находим дату и время доставки письма
        email_time_str = ','.join(email_received.split()[-5:-1])
        email_time = datetime.datetime.strptime(email_time_str, '%d,%b,%Y,%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        return email_time

    def send_email(self, email_from, email_to, subject, text, files=None, add_to_zip=True, folder_recursion=True):
        """Отправка письма.

        :param str email_from: Email отправителя.
        :param str/list email_to: Email получателя (получателей).
        :param str subject: Тема письма.
        :param str text: Текст письма.
        :param str/list files: Путь к файлу (файлам) или папке (папкам).
        :param bool add_to_zip: Архивация вложений, если их больше 1. По умолчанию True.
        :param bool folder_recursion: Рекурсивно обходить папки с вложениями. По умолчанию True.
        :raise: RuntimeError если неправильно указан email получателя.
        :raise: RuntimeError если неправильно указан email отправителя.
        :raise: TypeError если аргумент вложения неправильного типа.
        :raise: RuntimeError если все вложения некорректны.
        :raise: TypeError если в архиватор передан аргумент неверного типа.
        :raise: RuntimeError если не обнаружено активное подключение по протоколу SMTP.
        :raise: RuntimeError если не удалось отправить письмо.
        """
        # Проверка
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
            log.error('No SMTP connection found for this instance of the ConnEmailServer class')
            raise RuntimeError('No SMTP connection')
        if files is not None and type(files) is not str and type(files) is not list:
            log.error('Invalid files type')
            raise TypeError('Invalid files type')

        def attach_process(message, attach):
            """Обработка вложения.

            :param email.mime.multipart.MIMEMultipart message: Тело письма.
            :param list attach: Список путей к файлам и (или) папкам.
            :raise: RuntimeError если все вложения некорректны.
            """
            attachments = []
            for a in attach:
                # Если вложение - файл
                if os.path.isfile(a):
                    attachments.append({'full_path': a,
                                        'folder_path': os.path.basename(a)})
                # Если вложение - папка
                elif os.path.exists(a):
                    # Нерекурсивный обход
                    if not folder_recursion:
                        for f in os.listdir(a):
                            if os.path.isfile(os.path.join(a, f)) and f[0] != '.':
                                attachments.append({'full_path': os.path.join(a, f),
                                                    'folder_path': os.path.join(os.path.basename(a), f)})
                    # Рекурсивный обход
                    else:
                        full_main_dir = None
                        main_dir = None
                        for t, d, f in os.walk(a):
                            full_main_dir = t if full_main_dir is None else full_main_dir
                            main_dir = os.path.basename(t) if main_dir is None else main_dir
                            for file_name in f:
                                if file_name[0] != '.':
                                    attachments.append({'full_path': os.path.join(t, file_name),
                                                        'folder_path': os.path.join(main_dir, file_name) if
                                                        t == full_main_dir else os.path
                                                       .join(main_dir, os.path.relpath(t, start=full_main_dir),
                                                             file_name)})
                # Если вложение не файл и не папка
                else:
                    log.warning(f'Invalid attachment: "{a}"')

            # Если больше одного вложения и включена архивация - добавляем их все в zip-архив
            if len(attachments) > 1 and add_to_zip is True:
                add_to_zip(attachments)
                attach_add(message, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'))
                os.remove(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'))

            # Если вложения отсутствуют
            elif len(attachments) == 0:
                log.error(f'Invalid attachments: "{attach}"')
                raise RuntimeError('Invalid attachments')

            # Просто прикладываем вложения
            else:
                for att in attachments:
                    attach_add(message, att['full_path'])

        def attach_add(message, filepath):
            """Добавление вложения к письму.

            :param email.mime.multipart.MIMEMultipart message: Тело письма.
            :param str filepath: Путь к вложению.
            """
            # Получаем имя файла
            filename = os.path.basename(filepath)

            # Определяем тип файла на основе его расширения
            ctype, encoding = mimetypes.guess_type(filepath)

            # Если тип файла не определяется - используем общий тип
            if ctype is None or encoding is not None:
                ctype = 'application/octet-stream'

            # Получаем тип и подтип
            maintype, subtype = ctype.split('/', 1)

            # Если текстовый файл
            if maintype == 'text':
                with open(filepath) as fp:
                    file = MIMEText(fp.read(), _subtype=subtype)
                    fp.close()

            # Если изображение
            elif maintype == 'image':
                with open(filepath, 'rb') as fp:
                    file = MIMEImage(fp.read(), _subtype=subtype)
                    fp.close()

            # Если аудио
            elif maintype == 'audio':
                with open(filepath, 'rb') as fp:
                    file = MIMEAudio(fp.read(), _subtype=subtype)
                    fp.close()

            # Неизвестный тип файла
            else:
                with open(filepath, 'rb') as fp:
                    file = MIMEBase(maintype, subtype)
                    file.set_payload(fp.read())
                    fp.close()
                    encoders.encode_base64(file)

            # Добавляем заголовки
            file.add_header('Content-Disposition', 'attachment', filename=filename)

            # Присоединяем вложение к сообщению
            message.attach(file)

        def add_to_zip(attach_files):
            """Архивирование файлов.

            :param list attach_files: Список словарей файлов, которые необходимо добавить в архив.
            :raise: TypeError если аргумент files неверного типа.
            """
            log.debug('Start to archive files')
            with zipfile.ZipFile(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'attachments.zip'), 'w') as z:
                if type(attach_files) is list:
                    for f in attach_files:
                        z.write(f['full_path'], arcname=f['folder_path'], compress_type=zipfile.ZIP_DEFLATED)
                    log.debug('Files successfully added to archive')
                else:
                    log.error('Invalid type argument "attach_files"')
                    raise TypeError('Invalid type argument "attach_files"')

        log.info(f'Sending email with the subject "{subject}"')

        # Формируем письмо
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to if type(email_to) is str else ', '.join(email_to)
        msg.attach(MIMEText(text, 'plain'))
        if type(files) is str:
            attach_process(msg, [files])
        elif type(files) is list:
            attach_process(msg, files)

        # Отправляем письмо
        try:
            self.smtp.send_message(msg)
            log.info(f'Email "{subject}" sent to {email_to} from {email_from}')
        except Exception as e:
            log.error(f'Failed to send email "{subject}" to {email_to} from {email_from}. Error: {str(e)}')
            raise RuntimeError('Failed to send email')

    def status(self):
        """Состояние подключений в данном экземпляре класса.

        :return: Состояние подключений.
        :rtype: dict.
        """
        conn = {}

        # Проверяем IMAP
        if not hasattr(self, "imap"):
            conn['imap'] = 'not exist'
        else:
            try:
                self.imap.noop()
                conn['imap'] = f'connected{" (SSL)" if self.ssl else ""}'
            except Exception as e:
                log.debug(f'imap.noop(): {str(e)}')
                conn['imap'] = f'disconnected{" (SSL)" if self.ssl else ""}'

        # Проверяем SMTP
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
