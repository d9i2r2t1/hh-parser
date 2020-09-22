import argparse
import logging
import os
import sys
import traceback
from collections import namedtuple
from datetime import datetime
from typing import Any, Union, Tuple, Optional

import yaml

from hh_parser.lib import ConnEmailServer

log = logging.getLogger(__name__)


class ServiceSlave:
    """Сервисные функции."""

    def __init__(self, args: argparse.Namespace, set_log: bool = True, log_stream: bool = True,
                 log_file: bool = True, log_level: str = 'INFO') -> None:
        """
        :param args: Флаги с которыми запущено приложение
        :param set_log: Включить логирование
        :param log_stream: Вывод логов в консоль
        :param log_file: Сохранение логов в файл
        :param log_level: Уровень логов
        """
        self.args = vars(args)
        self.root_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        self.cfgs_path = os.path.join(self.root_path, 'cfgs')
        self.cfg = None

        if set_log:
            if 'LOG_FILE_NAME' not in globals():
                global LOG_FILE_NAME
                LOG_FILE_NAME = f'{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
            self.log_stream = log_stream
            self.log_file = log_file
            self.log_level = log_level
            self._set_logging()

    def get_settings(self, file_name: str) -> namedtuple:
        """
        Прочитай yml-файл конфигурации.
        :param file_name: Название файла конфигурации
        """
        def to_namedtuple(value: Any, key: Any = 'obj') -> namedtuple:
            """Сконвертируй настройки в именнованный кортеж."""
            if type(value) is dict:
                for k in value.keys():
                    value[k] = to_namedtuple(value.get(k), k)
                return namedtuple(key, value.keys())(**value)
            elif type(value) is list:
                for i, item in enumerate(value):
                    value[i] = to_namedtuple(item, key)
                return value
            else:
                return value

        with open(os.path.join(self.cfgs_path, file_name)) as f:
            settings_raw = yaml.safe_load(f)
        return to_namedtuple(settings_raw, 'settings')

    def _set_logging(self) -> None:
        """Настрой логирование."""
        root_logger = logging.getLogger()

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

        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(process)d-%(threadName)s] [%(filename)s '
                                      '%(funcName)s row:%(lineno)d] %(message)s')

        if self.log_stream:
            sh = logging.StreamHandler(stream=sys.stdout)
            sh.setFormatter(formatter)
            root_logger.addHandler(sh)
        if self.log_file:
            fh = logging.FileHandler(os.path.join(self.root_path, 'logs', LOG_FILE_NAME), encoding='utf-8')
            fh.setFormatter(formatter)
            root_logger.addHandler(fh)

        log.info(f'{os.path.basename(self.root_path)} running with args: {self.args}')

    def fail_notification(self, error: str, cfg_file_name: Optional[str] = None) -> None:
        """
        Отправь письмо на email при падении приложения.
        :param error: Текст ошибки
        :param cfg_file_name: Название файла с конфигурацией приложения
        """
        if error != 'Non-notification exception':
            self.cfg = self.get_settings(cfg_file_name)
            log.info(f'ERROR: {error}')
            log.info('Start sending fail notification')
            email_conn = ConnEmailServer(self.cfg.service_mail.server, self.cfg.service_mail.port,
                                         self.cfg.service_mail.login, self.cfg.service_mail.password,
                                         ssl=self.cfg.service_mail.ssl, imap=False)

            email_to = [i.strip() for i in self.cfg.service_mail.email_to]
            subject = f'EXCEPTION: {os.path.basename(self.root_path)}'
            text = f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}\n\n{traceback.format_exc()}'
            log_attach = os.path.join(self.root_path, 'logs', LOG_FILE_NAME)

            if os.path.isfile(log_attach):
                email_conn.send_email(self.cfg.service_mail.email_from, email_to, subject, text, log_attach)
            else:
                email_conn.send_email(self.cfg.service_mail.email_from, email_to, subject, text)

            email_conn.disconnect('SMTP')
        else:
            log.info('Non-notification exception, fail notification is not required')

    @staticmethod
    def remove_old_files(folder_path: str, lifetime_days: int, file_type: Union[str, Tuple[str, ...]]) -> None:
        """
        Удали "старые" файлы.
        :param folder_path: Путь к директории, в которой нужно удалить "старые" файлы
        :param lifetime_days: Количество дней, после которого файл становится "старым"
        :param file_type: Расширения файлов, которые необходимо удалить, например: .csv
        """
        raw_files = []
        for path, d, files in [i for i in os.walk(folder_path)]:
            for file in files:
                raw_files.append(os.path.join(path, file))
        files_with_expansions = list(filter(lambda x: x.endswith(file_type), raw_files))

        removed_files = []
        date_today = datetime.now().date()
        for file in files_with_expansions:
            file_date = datetime.fromtimestamp(os.path.getctime(file)).date()
            if abs((date_today - file_date).days) >= lifetime_days:
                os.remove(file)
                removed_files.append(os.path.basename(file))

        if len(removed_files) == 0:
            log.info(f'No old (over {lifetime_days} days) {file_type} files found in {folder_path}')
        else:
            log.info(f'Deleted {len(removed_files)} old (over {lifetime_days} days) {file_type} files '
                     f'in {folder_path}: {", ".join(removed_files)}')

    @staticmethod
    def get_cred_file_path(file_name: str) -> str:
        """
        Найди файл креденшиалс в проекте.
        :param file_name: Имя файла
        :return: Путь к файлу
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
