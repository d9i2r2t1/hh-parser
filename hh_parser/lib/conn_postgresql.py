import logging
import os
import time
from functools import wraps
from typing import Optional, Union, List, Iterator

import pandas as pd
import psycopg2
from psycopg2 import extensions
from sqlalchemy import create_engine

log = logging.getLogger(__name__)


def timeit(f):
    """Декоратор. Измерь время выполнения функции и запиши его в лог."""
    @wraps(f)
    def log_time(*args, **kwargs):
        time_start = time.monotonic()
        result = f(*args, **kwargs)
        time_stop = time.monotonic()
        if 'log_time' in kwargs and 'log_time':
            log.info(f'{f.__name__.upper()} completed in {round((time_stop - time_start), 3)} sec')
        return result
    return log_time


class ConnPostgreSQL:
    """Коннектор к PostgreSQL."""

    def __init__(self, dbname: str, host: str, user: str, password: str, port: Optional[int] = None,
                 application_name: str = os.path.basename(os.path.dirname(os.path.dirname(__file__)))) -> None:
        """
        :param dbname: Название базы данных
        :param host: Хост
        :param user: Имя пользователя
        :param password: Пароль
        :param port: Порт
        :param application_name: Название приложения
        """
        self.__application_name = application_name
        self.__dbname = dbname
        self.__user = user
        port_str = ':' + str(port) if port else ''
        log.debug(f'Connecting to PostgreSQL database "{dbname}" on the host {host}{port_str} under user "{user}"...')
        self.__conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password,
                                       application_name=application_name)
        self.__conn.autocommit = True
        self.__engine = create_engine(f'postgresql://{user}:{password}@{host}{port_str}/{dbname}')
        log.debug(f'Connected to PostgreSQL database "{dbname}"')

    dbname = property(lambda self: self.__dbname)
    user = property(lambda self: self.__user)
    application_name = property(lambda self: self.__application_name)

    class PgTable:
        """Таблица PostgreSQL."""

        def __init__(self, table_name: str, table_data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
                     pg_schema_name: Optional[str] = None) -> None:
            """
            :param table_name: Название таблицы
            :param table_data: Данные таблицы
            :param pg_schema_name: Схема БД, в которой лежит таблица
            """
            self.__table_name = table_name
            self.__table_data = table_data
            self.__pg_schema_name = pg_schema_name

        pg_schema_name = property(lambda self: self.__pg_schema_name)
        table_name = property(lambda self: self.__table_name)
        table_data = property(
            lambda self: (self.__table_data,) if type(self.__table_data) is pd.DataFrame else self.__table_data
        )

    def create_database(self, db_name: str, revoke_connect_from_public: bool = True) -> None:
        """
        Создай новую базу данных.
        :param db_name: Название базы данных
        :param revoke_connect_from_public: Отозвать доступ к созданной БД у привилегии Public
        """
        with self.__conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE {db_name} OWNER {self.__user}')
        log.info(f'Database "{db_name}" created. Owner: {self.__user}')

        if revoke_connect_from_public:
            with self.__conn.cursor() as cur:
                cur.execute(f'REVOKE CONNECT ON DATABASE {db_name} FROM PUBLIC')
            log.debug(f'Connection access to "{db_name}" revoked from Public')

    def delete_database(self, db_name: str) -> None:
        """
        Удали базу данных.
        :param db_name: Название базы данных
        """
        with self.__conn.cursor() as cur:
            cur.execute(f'DROP DATABASE {db_name}')
        log.info(f'Database "{db_name}" deleted')

    def disconnect(self) -> None:
        """Отключись от базы данных."""
        self.__engine.dispose()
        self.__conn.close()
        log.debug(f'Disconnected from PostgreSQL database "{self.__dbname}" under user "{self.__user}"')

    def create_pg_schema(self, pg_schema_name: str) -> None:
        """
        Создай схему базы данных.
        :param pg_schema_name: Название схемы
        """
        with self.__conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {pg_schema_name}')
        log.info(f'Schema "{pg_schema_name}" created (if not existed) in database "{self.__dbname}"')

    def delete_pg_schema(self, pg_schema_name: str, cascade: bool = False) -> None:
        """
        Удали схему базы данных.
        :param pg_schema_name: Название схемы
        :param cascade: Автоматически удалять объекты, содержащиеся в этой схеме, и все зависящие от них объекты
        """
        with self.__conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS {pg_schema_name} {"CASCADE" if cascade else ""}')
        log.info(f'Schema "{pg_schema_name}" deleted (if existed) from database "{self.__dbname}"')

    def create_partition_table(self, master_table_name: str, partition_table_name: str) -> str:
        """
        Создай партиционную таблицу.
        :param master_table_name: Название таблицы, для которой создаем партиционную таблицу
        :param partition_table_name: Название партиционной таблицы
        :return: Название созданной партиционной таблицы
        """
        log.debug(f'Creating partition table "{partition_table_name}" for table "{master_table_name}"...')
        with self.__conn.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS {partition_table_name} ( LIKE {master_table_name} INCLUDING ALL )')
            cur.execute(f'ALTER TABLE {partition_table_name} INHERIT {master_table_name}')
        log.info(f'Partition table "{partition_table_name}" created')
        return partition_table_name

    def get_db_size(self) -> str:
        """Получи размер базы данных."""
        with self.__conn.cursor() as cur:
            cur.execute('SELECT pg_size_pretty(pg_database_size(current_database()))')
            result = [r for r in cur][0][0]
        log.debug(f'Database "{self.__dbname}" size: {result}')
        return result

    def get_all_tables_names(self) -> List[str]:
        """Получи названия всех таблиц базы данных."""
        with self.__conn.cursor() as cur:
            cur.execute("SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables WHERE "
                        "table_schema NOT IN ('information_schema','pg_catalog')")
            result = [r[0] for r in cur]
        log.debug(f'Database "{self.__dbname}" table\'s names: {result}')
        return result

    def count_table_rows(self, table: str) -> int:
        """
        Посчитай количество строк в таблице.
        :param table: Название таблицы в формате название_схемы.название_таблицы
        """
        schema_name = table.split('.')[0]
        table_name = table.split('.')[1]
        with self.__conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            result = [r[0] for r in cur][0]
        log.debug(f'Table "{table}" rows count: {result}')
        return result

    def get_table_schema(self, table: str) -> Union[List[dict], None]:
        """
        Получи схему таблицы.
        :param table: Название таблицы в формате название_схемы.название_таблицы
        :return: Схема таблицы, если таблица существует, в противном случае - None
        """
        schema_name = table.split('.')[0]
        table_name = table.split('.')[1]
        with self.__conn.cursor() as cur:
            cur.execute(f"SELECT column_name, column_default, data_type FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'")
            result = [{'column_name': r[0], 'column_default_value': r[1], 'column_type': r[2]} for r in cur]
        if not result:
            log.error(f'Table "{table}" not found in "{self.__dbname}" PostgreSQL database')
            return
        log.debug(f'Table "{table}" schema: {result}')
        return result

    @timeit
    def run_sql_query(self, query: str, **kwargs) -> extensions.cursor:
        """
        Выполни SQL-запрос.
        :param query: SQL-запрос
        """
        log.info(f'Running PostgreSQL query: {" ".join(query.split())}')
        with self.__conn.cursor() as cur:
            cur.execute(query)
            return cur

    @timeit
    def run_sql_query_to_df(self, query: str, chunksize: int = 10000, **kwargs) -> Iterator[pd.DataFrame]:
        """
        Выполни SQL-запрос и получи результат в датафрейме.
        Можно передавать любые аргументы метода pandas.read_sql_query.
        :param query: SQL-запрос
        :param chunksize: Размер чанков
        """
        # Аргумент 'log_time' нужен только для декоратора @timeit
        kwargs = self._remove_log_time_from_kwargs(kwargs)

        log.info(f'Running PostgreSQL query: {" ".join(query.split())}')
        return pd.read_sql_query(query, con=self.__engine, chunksize=chunksize, **kwargs)

    @timeit
    def get_table(self, table_name: str, pg_schema_name: str = 'public', chunksize: int = 10000,
                  **kwargs) -> Union[PgTable, None]:
        """
        Получи таблицу из базы данных.
        Можно передавать любые аргументы метода pandas.read_sql_table.
        :param table_name: Название таблицы
        :param pg_schema_name: Название схемы БД
        :param chunksize: Размер чанков
        :return: Таблица, если она существует, в противном случае - None
        """
        # Аргумент 'log_time' нужен только для декоратора @timeit
        kwargs = self._remove_log_time_from_kwargs(kwargs)

        log.info(f'Fetching table "{pg_schema_name}.{table_name}" from PostgreSQL database "{self.__dbname}"...')
        try:
            table_data = pd.read_sql_table(
                table_name, con=self.__engine, schema=pg_schema_name, chunksize=chunksize, **kwargs
            )
            return self.PgTable(pg_schema_name=pg_schema_name, table_name=table_name, table_data=table_data)
        except ValueError as e:
            if 'not found' in str(e):
                return
            raise e

    @timeit
    def set_table(self, table: PgTable, if_exists: str = 'fail', chunksize: Optional[int] = 10000, index: bool = False,
                  **kwargs) -> None:
        """
        Запиши датафрейм в таблицу PostgreSQL.
        Можно передавать любые аргументы метода df.to_sql.
        :param table: Таблица
        :param if_exists: Действие если таблица существует: 'fail', 'replace' или 'append'
        :param chunksize: Количество строк таблицы для записи в одном запросе
        :param index: Записать индекс датафрейма как отдельную колонку
        """
        # Аргумент 'log_time' нужен только для декоратора @timeit
        kwargs = self._remove_log_time_from_kwargs(kwargs)

        pg_schema_name = table.pg_schema_name
        table_name = table.table_name
        table_data = table.table_data

        counter = 1
        for df in table_data:
            log.info(f'Writing {len(df)} rows to table "{pg_schema_name}.{table_name}"...')
            df.to_sql(table_name, schema=pg_schema_name, con=self.__engine, chunksize=chunksize, method='multi',
                      index=index, if_exists=if_exists if counter == 1 else 'append', **kwargs)
            log.info(f'{len(df)} rows successfully written to table "{pg_schema_name}.{table_name}"')
            counter += 1

    @staticmethod
    def _remove_log_time_from_kwargs(kwargs: dict) -> dict:
        """
        Удали аргумент 'log_time' из kwargs.
        :param kwargs: Словарь аргументов.
        """
        try:
            kwargs.pop('log_time')
        except KeyError:
            pass
        finally:
            return kwargs
