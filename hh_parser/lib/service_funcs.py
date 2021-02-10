import logging
import os
import sys
import traceback
from collections import namedtuple
from datetime import datetime
from functools import wraps
from typing import Any, Union, Optional, List, Tuple, Callable

import yaml

from lib.conn_email_server import ConnSmtpEmailServer

log = logging.getLogger(__name__)


def exception_notify(smtp_host: str, smtp_login: str, smtp_password: str, email_to: Union[str, List[str]],
                     email_from: str, smtp_port: int = 465, smtp_use_ssl: bool = True,
                     log_file_path: Optional[str] = None) -> Callable:
    """
    Декоратор. Залогируй ошибку и отправь оповещение на email.
    :param smtp_host: Хост smtp-сервера
    :param smtp_port: Порт smtp-сервера
    :param smtp_login: Логие smtp-сервера
    :param smtp_password: Пароль smtp-сервера
    :param smtp_use_ssl: Использовать SSL при подключении к smtp-серверу
    :param email_to: Email получателя (получателей)
    :param email_from: Email отправителя
    :param log_file_path: Путь к файлу с логами
    """
    def wrapper(func):
        @wraps(func)
        def log_error_and_notify(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.error(e, exc_info=True)
                smtp_conn = ConnSmtpEmailServer(host=smtp_host, port=smtp_port, login=smtp_login,
                                                password=smtp_password, use_ssl=smtp_use_ssl)
                smtp_conn.send_email(
                    email_from=email_from,
                    email_to=email_to,
                    subject='EXCEPTION OCCURRED',
                    text=f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}\n\n{traceback.format_exc()}',
                    attachments=log_file_path
                )
                smtp_conn.disconnect()
                raise e
        return log_error_and_notify
    return wrapper


def read_yml_config(file_path: str) -> namedtuple:
    """
    Прочитай yml-файл конфигурации.
    :param file_path: Путь к файлу конфигурации
    """
    with open(file_path) as file:
        config_raw = yaml.safe_load(file)

    def to_namedtuple(value: Any, key: Any = 'obj') -> namedtuple:
        """Приведи конфиг в namedtuple."""
        if type(value) is dict:
            for k in value.keys():
                value[k] = to_namedtuple(value.get(k), k)
            return namedtuple(key, value.keys())(**value)
        if type(value) is list:
            for i, item in enumerate(value):
                value[i] = to_namedtuple(item, key)
            return value
        return value

    config = to_namedtuple(config_raw, 'config')
    log.debug(f'Config: {config}')
    return config


def set_logging(level: str = 'INFO', log_to_stream: bool = True, log_to_file: bool = True,
                logs_folder: str = os.path.abspath(os.path.dirname(__file__))) -> Optional[str]:
    """
    Настрой логирование.
    :param level: Уровень логов: ['DEBUG'|'INFO'|'WARNING'|'ERROR'|'CRITICAL']
    :param log_to_stream: Вывод логов в консоль
    :param log_to_file: Запись логов в файл
    :param logs_folder: Путь к директории, где хранятся логи
    :return: Путь к log-файлу, если включена запись логов в файл
    """
    root_logger = logging.getLogger()
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }
    root_logger.setLevel(log_levels.get(level))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(process)d-%(threadName)s] [%(filename)s '
                                  '%(funcName)s row:%(lineno)d] %(message)s')
    if log_to_stream:
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setFormatter(formatter)
        root_logger.addHandler(sh)
    if log_to_file:
        if not os.path.exists(logs_folder):
            default_logs_folder = os.path.abspath(os.path.dirname(__file__))
            log.warning(f'Invalid logs_dir: {logs_folder}. Default logs_dir will be used: {default_logs_folder}')
            logs_folder = default_logs_folder
        log_file_name = f'{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
        log_file_path = os.path.join(logs_folder, log_file_name)
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setFormatter(formatter)
        root_logger.addHandler(fh)
        return log_file_path


def remove_old_files(folder_path: str, lifetime_days: int, file_type: Union[str, Tuple[str, ...]]) -> None:
    """
    Удали "старые" файлы.
    :param folder_path: Путь к директории, в которой нужно удалить "старые" файлы
    :param lifetime_days: Количество дней, после которых файл становится "старым"
    :param file_type: Расширения файлов, которые необходимо удалить, например: .csv
    """
    raw_files = []
    for path, dirs, files in [i for i in os.walk(folder_path)]:
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

    if not removed_files:
        log.debug(f'Old (over {lifetime_days} days) *{file_type} files not found in {folder_path}')
    else:
        log.info(f'Removed {len(removed_files)} old (over {lifetime_days} days) *{file_type} files '
                 f'from {folder_path}: {removed_files}')


def get_creds_file_path(file_name: str) -> str:
    """
    Найди файл креденшиалс в проекте.
    :param file_name: Имя файла
    :return: Путь к файлу
    """
    search_folders = [
        os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'credentials'),
        os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
        os.path.abspath(os.path.dirname(__file__)),
        os.path.join(os.path.abspath(os.path.dirname(__file__)), 'credentials'),
    ]
    for folder in search_folders:
        log.debug(f'Searching for creds file in {folder}...')
        file_path = os.path.join(folder, file_name)
        if os.path.isfile(file_path):
            log.debug(f'File found: {file_path}')
            return file_path
    log.error(f'Creds file "{file_name}" not found')
    raise FileNotFoundError(f'Creds file "{file_name}" not found')
