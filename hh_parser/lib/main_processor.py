import argparse
from collections import namedtuple
from datetime import datetime

import pandas as pd
from psycopg2 import OperationalError

from .conn_email_server import ConnSmtpEmailServer
from .conn_postgresql import ConnPostgreSQL
from .parser import HhParser, HhParserResultsProcessor
from .report_file_processor import ReportFileProcessor


class MainProcessor:
    """Логика работы сервиса."""

    def __init__(self, cfg: namedtuple, args: argparse.Namespace) -> None:
        """
        :param cfg: Конфигурация приложения
        :param args: Параметры, с которыми запущено приложение
        """
        self.__args = args
        self.__cfg = cfg
        self.__conn_pg = None
        self.__conn_email = ConnSmtpEmailServer(host=cfg.email.server, port=cfg.email.port,
                                                login=cfg.email.login, password=cfg.email.password,
                                                use_ssl=cfg.email.ssl)

    def run(self) -> None:
        """Запусти сервис."""
        self._create_database()

        raw_results = HhParser(area=self.__cfg.parser.area,
                               search_period=self.__cfg.parser.search_period,
                               search_text=self.__cfg.parser.search_text,
                               search_regex=self.__cfg.parser.search_regex).run()
        results = HhParserResultsProcessor(hh_parsed_data=raw_results, pg_conn=self.__conn_pg).run()

        self._load_df_to_postgres(df=results.df_parsing_results, table_name='parsing_results', if_exists='append')
        self._load_df_to_postgres(df=results.df_current_jobs, table_name='current_jobs', if_exists='replace')
        self._load_df_to_postgres(df=results.df_unique_jobs, table_name='unique_jobs', if_exists='append')
        self._load_df_to_postgres(df=results.df_unique_closed_jobs, table_name='unique_closed_jobs', if_exists='append')

        report_path = ReportFileProcessor(hh_parsed_data=results).create_report_file()
        if self.__args.send_email:
            self.__conn_email.send_email(email_from=self.__cfg.email.email_from, email_to=self.__cfg.email.email_to,
                                         subject=self.__cfg.email.email_subject, attachments=report_path,
                                         text=self._get_email_text(search_text=results.search_text))

    def stop(self) -> None:
        """Отключись от всех соединений."""
        self.__conn_pg.disconnect()
        self.__conn_email.disconnect()

    def _create_database(self) -> None:
        """Создай базу данных."""
        db_params = {'host': self.__cfg.postgres.host,
                     'port': self.__cfg.postgres.port,
                     'user': self.__cfg.postgres.user,
                     'password': self.__cfg.postgres.password}
        try:
            self.__conn_pg = ConnPostgreSQL(dbname=self.__cfg.postgres.name, **db_params)
        except OperationalError as e:
            if f'database "{self.__cfg.postgres.name}" does not exist' in str(e):
                self.__conn_pg = ConnPostgreSQL(dbname='postgres', **db_params)
                self.__conn_pg.create_database(self.__cfg.postgres.name)
                self.__conn_pg.disconnect()
                self.__conn_pg = ConnPostgreSQL(dbname=self.__cfg.postgres.name, **db_params)
            else:
                raise e

    def _load_df_to_postgres(self, df: pd.DataFrame, table_name: str, schema_name: str = 'public', **kwargs) -> None:
        """
        Загрузи датафрейм в Postgres.
        :param df: Датафрейм с данными
        :param table_name: Название таблицы
        """
        pg_table = self.__conn_pg.PgTable(table_name=table_name, table_data=df, pg_schema_name=schema_name)
        self.__conn_pg.set_table(table=pg_table, **kwargs)

    @staticmethod
    def _get_email_text(search_text: str) -> str:
        """
        Получи текст письма для рассылки.
        :param search_text: Поисковый запрос на hh.ru
        """
        return f'Во вложении отчет по вакансиям на hh.ru по запросу "{search_text}" ' \
               f'за {datetime.now().strftime("%d.%m.%Y")}.\n'
