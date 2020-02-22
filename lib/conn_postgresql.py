import psycopg2
import time
import logging
import pandas as pd
from sqlalchemy import create_engine

log = logging.getLogger(__name__)


class ConnPostgreSQL:
    """Подключение и работа с PostgreSQL"""

    def __init__(self, dbname, host, user, password, port=None):
        """Инициализация экземпляра класса.

        :param str dbname: Название базы данных.
        :param str host: Хост.
        :param str user: Имя пользователя.
        :param str password: Пароль.
        :param str port: Порт (опционально).
        :raise: RuntimeError если не получилось подключиться к базе данных.
        """
        self._dbname = dbname
        self._user = user
        try:
            log.info(f'Connecting to PostgreSQL database "{dbname}" on the host {host}'
                     f'{":" + port if port is not None else ""} under user "{user}"...')
            self._conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
            self._conn.autocommit = True
            self._cur = self._conn.cursor()
            self._engine = create_engine(f'postgresql://{user}:{password}@{host}'
                                         f'{":" + port if port is not None else ""}/{dbname}')
            log.info(f'Connected to PostgreSQL database "{dbname}"')
        except Exception as e:
            log.error(f'Failed to connect to PostgreSQL database "{self._dbname}" under user "{self._user}". '
                      f'Host: {host}{":" + port if port is not None else ""}. Error: {e}')
            raise RuntimeError('Failed to connect to PostgreSQL database')

    def create_database(self, db_name):
        """Создание новой базы данных.

        :param str db_name: Название новой базы данных.
        :raise: RuntimeError если не удалось создать новую базу данных.
        """
        try:
            self._cur.execute(f'CREATE DATABASE {db_name} OWNER {self._user}')
            log.info(f'Database "{db_name}" created. Owner: {self._user}')
        except Exception as e:
            log.error(f'Failed to create new PostgreSQL database "{db_name}" under user "{self._user}". Error: {e}')
            raise RuntimeError('Failed to create new PostgreSQL database')

    def delete_database(self, db_name):
        """Удаление базы данных.

        :param str db_name: Название удаляемой базы данных.
        :raise: RuntimeError если не удалось удалить базу данных.
        """
        try:
            self._cur.execute(f'DROP DATABASE {db_name}')
            log.info(f'Database "{db_name}" deleted')
        except Exception as e:
            log.error(f'Failed to delete PostgreSQL database "{db_name}" under user "{self._user}". Error: {e}')
            raise RuntimeError('Failed to delete PostgreSQL database')

    def disconnect(self):
        """Отключение от базы данных"""

        self._cur.close()
        self._conn.close()
        log.info(f'Disconnected from PostgreSQL database "{self._dbname}" under user "{self._user}"')

    def get_db_size(self):
        """Получение размера базы данных.

        :return: Размер базы данных.
        :rtype: str.
        """
        self._cur.execute('SELECT pg_size_pretty(pg_database_size(current_database()))')
        result = [r for r in self._cur][0][0]
        log.info(f'PostgreSQL database "{self._dbname}" size: {result}')
        return result

    def get_all_tables_names(self):
        """Получение названий всех таблиц базы данных.

        :return: Названия таблиц базы данных.
        :rtype: list.
        """
        self._cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN "
                          "('information_schema','pg_catalog')")
        result = [r[0] for r in self._cur]
        log.info(f'PostgreSQL database "{self._dbname}" table\'s names: {result}')
        return result

    def get_table_schema(self, table):
        """Получение схемы таблицы.

        :param str table: Название таблицы.
        :raise: RuntimeError если таблица не найдена.
        :return: Список словарей со схемой таблицы (название столбца, значение по умолчанию и тип данных).
        :rtype: list.
        """
        self._cur.execute(f"SELECT column_name, column_default, data_type FROM INFORMATION_SCHEMA.COLUMNS "
                          f"WHERE table_name = '{table}'")
        result = [{'column_name': r[0], 'column_default_value': r[1], 'column_type': r[2]} for r in self._cur]
        if not result:
            log.error(f'Table "{table}" not found in "{self._dbname}" PostgreSQL database')
            raise RuntimeError('Table not found in PostgreSQL database')
        else:
            log.info(f'Table "{table}" schema: {result}')
            return result

    def run_sql_query(self, query):
        """Выполнение SQL-запроса.

        :param str query: SQL-запрос.
        :raise: RuntimeError если не удалось выполнить SQL-запрос.
        :return: Объект курсора с выполненным запросом.
        """
        try:
            log.info(f'Running PostgreSQL query: {" ".join(query.split())}')
            time_start = time.time()
            self._cur.execute(query)
            log.info(f'PostgreSQL query completed in {round(time.time() - time_start, 2)} seconds')
            return self._cur
        except Exception as e:
            log.error(f'Failed to execute PostgreSQL query. Error: {e}')
            raise RuntimeError('Failed to execute PostgreSQL query')

    def run_sql_query_to_df(self, query):
        """Выполенние SQL-запроса и получение результата в датафрейме.

        :param str query: SQL-запрос.
        :raise: RuntimeError если не удалось выполнить запрос.
        :return: Результат SQL-запроса.
        :rtype: DataFrame.
        """
        try:
            log.info(f'Running PostgreSQL query: {" ".join(query.split())}')
            time_start = time.time()
            df = pd.read_sql_query(query, con=self._engine)
            log.info(f'PostgreSQL query completed in {round(time.time() - time_start, 2)} seconds. '
                     f'Number of result rows: {len(df)}')
            return df
        except Exception as e:
            log.error(f'Failed to execute PostgreSQL query. Error: {e}')
            raise RuntimeError('Failed to execute PostgreSQL query')

    def set_table(self, df, table, if_exists='fail', index=False, chunksize=None):
        """Запись датафрейма в таблицу PostgreSQL.

        :param DataFrame df: Датафрейм, который необходимо записать в таблицу.
        :param str table: Название таблицы.
        :param str if_exists: Действие если таблица существует. Может быть 'fail', 'replace' или 'append'.
        По умолчанию - 'fail'.
        :param bool index: Записать индекс датафрейма как отдельную колонку. По умолчанию - False.
        :param int chunksize: Количество строк таблицы для записи в одном запросе.
        По умолчанию - записывается вся таблица целиком в одном запросе.
        :raise: RuntimeError если не удалось записать датафрейм в таблицу PostgreSQL.
        """
        try:
            log.info(f'Writing {len(df)} rows to PostgreSQL table "{table}"...')
            time_start = time.time()
            df.to_sql(table, con=self._engine, if_exists=if_exists, index=index, chunksize=chunksize)
            log.info(f'{len(df)} rows successfully written to PostgreSQL database "{self._dbname}" to table "{table}" '
                     f'in {round(time.time() - time_start, 2)} seconds')
        except Exception as e:
            log.error(f'Failed to write dataframe to PostgreSQL database "{self._dbname}" to table "{table}". '
                      f'Error: {e}')
            raise RuntimeError('Failed to write dataframe to PostgreSQL table')

    def get_table(self, table, columns=None):
        """Получение таблицы из базы данных PostgreSQL.

        :param str table: Название таблицы.
        :param list columns: Названия колонок, которые хотим получить из таблицы (опционально).
        :raise: RuntimeError если не удалось получить таблицу.
        :return: Таблица из базы данных PostgreSQL.
        :rtype: DataFrame.
        """
        try:
            log.info(f'Fetching table "{table}" from PostgreSQL database "{self._dbname}"...')
            time_start = time.time()
            df = pd.read_sql_table(table, con=self._engine, columns=columns)
            log.info(f'Table "{table}" successfully fetched in {round(time.time() - time_start, 2)} seconds. '
                     f'Number of rows: {len(df)}')
            return df
        except Exception as e:
            log.error(f'Failed to fetch table "{table}" from PostgreSQL database "{self._dbname}". Error: {e}')
            raise RuntimeError('Failed to fetch table from PostgreSQL database')
