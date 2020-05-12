import json
import logging
import os
import sys
import traceback
from datetime import datetime

from requests_oauthlib import OAuth2Session

from lib.conn_email_server import ConnEmailServer

log = logging.getLogger(__name__)


class ServiceSlave:
    """Сервисные функции"""

    def __init__(self, flags, set_log=True, log_stream=True, log_file=True, log_level='INFO', google_token_check=False):
        """Инициализация экземпляра класса.

        :param list flags: Флаги с которыми запущен проект.
        :param bool set_log: Включить логирование. По умолчанию - True.
        :param bool log_stream: Вывод логов в консоль. По умолчанию - True.
        :param bool log_file: Сохранение логов в файл. По умолчанию - True.
        :param srt log_level: Уровень логов. По умолчанию - INFO.
        :param bool google_token_check: Нужна ли проверка наличия токена Google OAuth2. По умолчанию - False.
        """
        self.flags = flags
        self.settings = 'settings_prod.cfg' if '-p' in flags else 'settings.cfg'
        self.root_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        # Получаем название лог-файла и включаем логирование
        if set_log:
            if 'log_file_name' not in globals():
                global log_file_name
                log_file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.log'
            self.log_stream = log_stream
            self.log_file = log_file
            self.log_level = log_level
            self._set_logging()

        # Проверяем наличие Google OAuth2 токена
        if google_token_check:
            self.scopes = []
            self.app_type = 'installed'
            self._get_oauth2_google_token()

    def get_settings(self, logs=True):
        """Чтение файла настроек.

        :param bool logs: Вывод настроек в логи. По умолчанию True.
        :return: Словарь с настройками.
        :rtype: dict.
        """
        with open(os.path.join(self.root_path, self.settings)) as f:
            settings = [i.split('=') for i in f.read().splitlines() if i != '' and i.strip()[0] != '#']
            settings = {settings[i][0].strip(): settings[i][1].strip() for i in range(len(settings))}
        if logs is True:
            logging.info(f'Settings: {settings}')
        return settings

    def _set_logging(self):
        """Настройка логирования"""

        root_logger = logging.getLogger()

        # Устанавливаем уровень логов
        if self.log_level == 'DEBUG':
            root_logger.setLevel(logging.DEBUG)
        elif self.log_level == 'INFO':
            root_logger.setLevel(logging.INFO)
        elif self.log_level == 'WARNING':
            root_logger.setLevel(logging.WARNING)
        elif self.log_level == 'ERROR':
            root_logger.setLevel(logging.ERROR)
        elif self.log_level == 'CRITICAL':
            root_logger.setLevel(logging.CRITICAL)
        else:
            root_logger.setLevel(logging.NOTSET)

        # Настраиваем форматтер
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] [%(filename)s %(funcName)s '
                                      'row:%(lineno)d] %(message)s')

        # Вывод логов в консоль
        if self.log_stream:
            sh = logging.StreamHandler(stream=sys.stdout)
            sh.setFormatter(formatter)
            root_logger.addHandler(sh)

        # Вывод логов в файл
        if self.log_file:
            fh = logging.FileHandler(os.path.join(self.root_path, 'logs', log_file_name), encoding='utf-8')
            fh.setFormatter(formatter)
            root_logger.addHandler(fh)

        log.info(f'{os.path.basename(self.root_path)} running with {self.settings} and flags: {self.flags[1:]}')

    def fail_notification(self, error):
        """Отбивка при падении скрипта.

        :param str error: Текст ошибки.
        """
        if error != 'Non-notification exception':
            log.info(f'ERROR: {error}')
            log.info('Start sending fail notification')

            # Получаем настройки из файла
            settings = self.get_settings(logs=False)

            # Инициализируем коннектор к почтовому серверу
            conn = ConnEmailServer(settings['SERVICE_MAIL_SERVER'], settings['SERVICE_MAIL_PORT'],
                                   settings['SERVICE_MAIL_LOGIN'], settings['SERVICE_MAIL_PASSWORD'],
                                   ssl=True if settings['SERVICE_MAIL_SSL'] == 'yes' else False, imap=False)

            # Генерируем письмо отбивки с трейсбеком
            email_to = [i.strip() for i in settings['SERVICE_EMAIL_TO'].split(',')]
            subject = 'EXCEPTION: ' + os.path.basename(self.root_path)
            text = datetime.now().strftime("%d.%m.%Y %H:%M:%S") + '\n\n' + traceback.format_exc()
            log_attach = os.path.join(self.root_path, 'logs', log_file_name)

            # Отправляем письмо с трейсбеком и логами
            if os.path.isfile(log_attach):
                conn.send_email(settings['EMAIL_FROM'], email_to, subject, text, log_attach)
            else:
                conn.send_email(settings['EMAIL_FROM'], email_to, subject, text)

            # Отключаемся от почтового сервера
            conn.disconnect('SMTP')
        else:
            log.info('Non-notification exception, fail notification is not required')

    @staticmethod
    def get_cred_file_path(file_name):
        """Поиск по имени файла креденшалсов в проекте.

        :param str file_name: Имя файла.
        :raise: FileNotFoundError если не удалось найти файл.
        :return: Путь к файлу.
        :rtype: str.
        """
        file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file_name)
        log.debug(f'Searching for file in dir: "{os.path.abspath(os.path.dirname(__file__))}"')
        if os.path.isfile(file_path):
            log.debug(f'File found in dir: "{os.path.abspath(os.path.dirname(__file__))}"')
            return file_path
        else:
            file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'credentials', file_name)
            log.debug(f'Searching for file in dir: '
                      f'"{os.path.join(os.path.abspath(os.path.dirname(__file__)), "credentials")}"')
            if os.path.isfile(file_path):
                log.debug(f'File found in dir: '
                          f'"{os.path.join(os.path.abspath(os.path.dirname(__file__)), "credentials")}"')
                return file_path
            else:
                root_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
                file_path = os.path.join(root_path, 'credentials', file_name)
                log.debug(f'Searching for file in dir: "{os.path.join(root_path, "credentials")}"')
                if os.path.isfile(file_path):
                    log.debug(f'File found in dir: "{os.path.join(root_path, "credentials")}"')
                    return file_path
                else:
                    log.error(f'File not found: "{file_name}"')
                    raise FileNotFoundError('File not found')

    @staticmethod
    def get_oauth2_google_token_info():
        """Получение информации о файле с токеном Google OAuth2.

        :return: Кортеж: (название файла с токеном, путь к файлу с токеном)
        :rtype: tuple.
        """
        token_file_name = f'token_google_{os.path.basename(os.path.dirname(os.path.dirname(__file__)))}.json'
        token_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'credentials', token_file_name)
        return tuple([token_file_name, token_path])

    @staticmethod
    def refresh_oauth2_google_token():
        """Обновление токена Google OAuth2 в проекте"""

        # Получаем путь к файлу токена
        log.info('Start refreshing Google OAuth2 token')
        token_path = ServiceSlave.get_oauth2_google_token_info()[1]

        # Читаем файл токена
        with open(token_path, 'r') as f:
            data_token = json.loads(f.read())
            data_token['expires_in'] = '-30'

        # Обновляем токен
        client_id = data_token['client_id']
        refresh_url = data_token['token_uri']
        protected_url = data_token['auth_provider_x509_cert_url']
        extra = {'client_id': client_id,
                 'client_secret': data_token['client_secret']}

        def token_saver(refreshed_token):
            """Сохранение обновленного токена Google OAuth2.

            :param str refreshed_token: Полученный обновленный токен Google OAuth2 в JSON.
            """
            full_token = json.loads(json.dumps(refreshed_token))
            full_token.update({'token_uri': refresh_url,
                               'client_id': client_id,
                               'client_secret': extra['client_secret'],
                               'auth_provider_x509_cert_url': protected_url})
            token_file = open(token_path, 'w')
            token_file.write(json.dumps(full_token))
            token_file.close()

        client = OAuth2Session(client_id, token=data_token, auto_refresh_url=refresh_url, auto_refresh_kwargs=extra,
                               token_updater=token_saver)
        client.get(protected_url)
        log.info('Google OAuth2 token successfully refreshed')

    def _get_oauth2_google_token(self):
        """Получение токена Google OAuth2.

        :raise: RuntimeError если указан тип приложения отличный от 'installed' или 'web'.
        """
        # Названия файлов client_secret.json и файла с токеном
        client_secret_file_name = 'client_secret.json'
        token_file_name, token_file_path = self.get_oauth2_google_token_info()
        if os.path.isfile(token_file_path):
            log.info(f'Google OAuth2 token found: {token_file_path}')
            return

        # Проверка на тип приложения
        if self.app_type != 'installed' and self.app_type != 'web':
            log.error(f'Application type not supported: {self.app_type}')
            raise RuntimeError('Application type not supported')

        # Читаем файл client_secret.json
        log.info(f'Start fetching {token_file_name}')
        client_secret_path = self.get_cred_file_path(client_secret_file_name)
        with open(client_secret_path, 'r') as f:
            data_client_secret = json.loads(f.read())
        client_id = data_client_secret[self.app_type]['client_id']
        client_secret = data_client_secret[self.app_type]['client_secret']
        redirect_uri = data_client_secret[self.app_type]['redirect_uris'][0]
        auth_uri = data_client_secret[self.app_type]['auth_uri']
        token_uri = data_client_secret[self.app_type]['token_uri']
        log.info('client_secret.json read')

        # Открываем OAuth2 сессию
        oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=self.scopes)
        authorization_url, state = oauth.authorization_url(auth_uri, access_type="offline", prompt="consent")
        print(f'\nGo to URL below and authorize access:\n{authorization_url}')

        # Ждем подтверждения от пользователя
        authorization_code = input(
            f'\nEnter {"auth code" if self.app_type == "installed" else "full callback URL and space after it"}:\n')

        # Получаем токен
        if self.app_type == "installed":
            token = oauth.fetch_token(token_uri, code=authorization_code, client_secret=client_secret)
        else:
            token = oauth.fetch_token(token_uri, authorization_response=authorization_code,
                                      client_secret=client_secret)
        log.info(f'Token fetched: {token}')

        # Формируем полный токен
        full_token = json.loads(json.dumps(token))
        full_token.update({'token_uri': data_client_secret[self.app_type]['token_uri'],
                           'client_id': client_id,
                           'client_secret': client_secret,
                           'auth_provider_x509_cert_url': data_client_secret[self.app_type][
                               'auth_provider_x509_cert_url']})
        log.info(f'Full token: {full_token}')

        # Сохраняем полный токен
        token_file = open(token_file_path, 'w')
        token_file.write(json.dumps(full_token))
        token_file.close()
        log.info(f'Full token saved to {token_file_path}')
        print('\nGoogle OAuth2 token fetched, now you can use the app')
        sys.exit(0)
