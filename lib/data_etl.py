import logging
import os
from datetime import datetime

import pandas as pd

from lib.conn_postgresql import ConnPostgreSQL
from lib.report_file_handler import ReportFileHandler

log = logging.getLogger(__name__)


class DataEtl:
    """Обработка данных парсера"""

    def __init__(self, parsing_results, settings):
        """Инициализация экземпляра класса.

        :param tuple parsing_results: Результаты парсинга (данные, время).
        :param dict settings: Настройки приложения.
        """
        self._df = parsing_results[0]
        self._parsing_time = parsing_results[1]
        self._report_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
        self._search_text = settings['SEARCH_TEXT']
        self._pg_conn = ConnPostgreSQL(settings['DB_NAME'], settings['DB_HOST'], settings['DB_USER'],
                                       settings['DB_PASSWORD'], settings['DB_PORT'])

    def get_report_file_name(self):
        """Получение названия для файла с результатами парсинга.

        :return: Название файла.
        :rtype: str.
        """
        return f'{self._search_text}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'.replace(' ', '-')

    def save_report_file(self):
        """Сохранение результатов парсинга в xlsx файл"""

        file_path = os.path.join(self._report_folder, self.get_report_file_name())
        self._df.to_excel(file_path, index=False)
        log.info(f'Report file saved: {file_path}')
        ReportFileHandler(file_path).format_file()

    def find_jobs_without_salary(self):
        """Нахождения % вакансий без указания зарплаты.

        :return: % вакансий без указания зарплаты.
        :rtype: dict.
        """
        # Находим количество вакансий без указания зарплаты
        unknown_salary_count = self._df.loc[self._df['salary'] == 'Не указано']['salary'].count()

        # Считаем % от общего количества вакансий
        unknown_salary_percent = round((unknown_salary_count / len(self._df)) * 100, 2)

        log.info(f'Jobs without salary: {unknown_salary_percent}%')
        return {'jobs_without_salary': unknown_salary_percent}

    def find_salary_mean_and_median(self):
        """Нахождения медианной, средней, средней максимальной и средней минимальной зарплаты

        :return: Средние и медианная зарплаты.
        :rtype: dict.
        """
        # Минимальные и максимальные зарплаты
        salaries_min = []
        salaries_max = []

        for i in range(len(self._df)):
            # Указанная зарплата "от"
            if self._df.loc[i, 'salary'].split()[0] == 'от':
                salaries_min.append(int(self._df.loc[i, 'salary'].split()[1]))
            # Указанная зарплата "до"
            elif self._df.loc[i, 'salary'].split()[0] == 'до':
                salaries_max.append(int(self._df.loc[i, 'salary'].split()[1]))
            # Указанная вилка зарплаты
            elif len(self._df.loc[i, 'salary'].split()[0].split('-')) == 2:
                fork = self._df.loc[i, 'salary'].split()[0].split('-')
                salaries_min.append(int(fork[0]))
                salaries_max.append(int(fork[1]))
            # Вакансии без указания зарплаты
            elif self._df.loc[i, 'salary'] == 'Не указано':
                pass
            # Фиксированная зарплата
            else:
                salaries_min.append(int(self._df.loc[i, 'salary'].split()[0]))
                salaries_max.append(int(self._df.loc[i, 'salary'].split()[0]))

        # Все зарплаты
        salaries_all = salaries_min + salaries_max

        # Считаем статистику
        salary_mean = round(pd.Series(salaries_all).mean())
        salary_median = round(pd.Series(salaries_all).median())
        min_salary_mean = round(pd.Series(salaries_min).mean())
        max_salary_mean = round(pd.Series(salaries_max).mean())

        log.info(f'Mean salary: {salary_mean}, median salary: {salary_median}, mean min salary: {min_salary_mean}, '
                 f'mean max salary: {max_salary_mean}')
        return {'salary_mean': salary_mean,
                'salary_median': salary_median,
                'min_salary_mean': min_salary_mean,
                'max_salary_mean': max_salary_mean}

    def _get_df_for_main_table(self):
        """Формирование датафрейма для основной таблицы.

        :return: Датафрейм с данными парсинга для загрузки в основную таблицу.
        :rtype: DataFrame.
        """
        data_for_update = {}
        data_for_update.update(self.find_jobs_without_salary())
        data_for_update.update(self.find_salary_mean_and_median())
        data_for_update.update({'jobs_count': len(self._df),
                                'date': datetime.now().strftime("%Y-%m-%d"),
                                'time_parse': self._parsing_time})
        df = pd.DataFrame([data_for_update])
        df['date'] = pd.to_datetime(df['date'])

        log.info(f'DataFrame for main table generated')
        return df

    def _get_df_for_jobs_table(self):
        """Формирование датафрейма для таблицы со всеми сегодняшними вакансиями.

        :return: Датафрейм с данными по всем сегодняшним вакансиям.
        :rtype: DataFrame.
        """
        # Сортируем вакансии в зависимости от указанной зарплаты
        min_salary = []
        max_salary = []

        df = self._df.copy().reset_index(drop=True)
        for i in range(len(df)):
            # Указанная зарплата "от"
            if df.loc[i, 'salary'].split()[0] == 'от':
                min_salary.append(int(df.loc[i, 'salary'].split()[1]))
                max_salary.append(int(df.loc[i, 'salary'].split()[1]))
            # Указанная зарплата "до"
            elif df.loc[i, 'salary'].split()[0] == 'до':
                min_salary.append(0)
                max_salary.append(int(df.loc[i, 'salary'].split()[1]))
            # Указанная вилка зарплаты
            elif len(df.loc[i, 'salary'].split()[0].split('-')) == 2:
                fork = df.loc[i, 'salary'].split()[0].split('-')
                min_salary.append(int(fork[0]))
                max_salary.append(int(fork[1]))
            # Вакансии без указания зарплаты
            elif df.loc[i, 'salary'] == 'Не указано':
                min_salary.append(0)
                max_salary.append(0)
            # Фиксированная зарплата
            else:
                min_salary.append(int(df.loc[i, 'salary'].split()[0]))
                max_salary.append(int(df.loc[i, 'salary'].split()[0]))

        df['min_salary'] = min_salary
        df['max_salary'] = max_salary
        df['mean_salary'] = (df['min_salary'] + df['max_salary']) / 2
        df = df.sort_values(['mean_salary', 'max_salary', 'min_salary'], ascending=False).reset_index(drop=True)
        df['row'] = list(range(1, len(df) + 1))

        log.info(f'DataFrame for today jobs table generated')
        return df[['row', 'date', 'title', 'company', 'salary', 'href']]

    def _get_df_for_unique_jobs_table(self):
        """Формирование датафрейма для таблицы с уникальными вакансиями.

        :return: Датафрейм для таблицы с уникальными вакансиями.
        :rtype: DataFrame.
        """
        # Получаем данные из таблицы
        try:
            df = self._pg_conn.get_table('unique_jobs')

        # Если таблицы нет в БД - создаем пустой датафрейм
        except RuntimeError:
            df = pd.DataFrame.from_dict({'date': [], 'href': []})
            df['date'] = pd.to_datetime(df['date'])
            df['href'] = df['href'].astype(str)

        # Находим уникальные вакансии
        df_merged = pd.merge(df, self._df[['date', 'href']], on='href', how='outer')
        df_merged = df_merged[pd.isnull(df_merged['date_x'])][['date_y', 'href']].reset_index(drop=True)
        df_merged.columns = ['date', 'href']

        log.info(f'DataFrame for unique jobs table generated')
        return df_merged

    def _get_df_for_closed_unique_jobs_table(self):
        """Формирование датафрейма для таблицы с закрытыми уникальными вакансиями.

        :return: Датафрейм для таблицы с закрытыми уникальными вакансиями.
        :rtype: DataFrame.
        """
        # Получаем данные из таблицы с уникальными вакансиями
        try:
            df_unique_jobs = self._pg_conn.get_table('unique_jobs')

        # Если таблицы нет в БД - создаем пустой датафрейм
        except RuntimeError:
            df_unique_jobs = pd.DataFrame().from_dict({'date': [], 'href': []})
            df_unique_jobs['date'] = pd.to_datetime(df_unique_jobs['date'])
            df_unique_jobs['href'] = df_unique_jobs['href'].astype(str)

        # Находим новые уникальные закрытые вакансии
        df_merged = pd.merge(df_unique_jobs, self._df[['date', 'href']], on='href', how='outer')
        df_merged = df_merged[pd.isnull(df_merged['date_y'])].reset_index(drop=True)
        df_merged.columns = ['publication_date', 'href', 'closing_date']
        df_merged['closing_date'] = datetime.now().strftime("%Y-%m-%d")
        df_merged['href'] = df_merged['href'].astype(str)

        # Преобразуем столбцы с датами в формат даты
        df_merged['closing_date'] = pd.to_datetime(df_merged['closing_date'])
        df_merged['publication_date'] = pd.to_datetime(df_merged['publication_date'])

        # Считаем время жизни вакансий
        df_merged['date_diff'] = (df_merged['closing_date'] - df_merged['publication_date']).dt.days.astype(int)

        # Получаем данные из таблицы с уникальными закрытыми вакансиями
        try:
            df_unique_closed_jobs = self._pg_conn.get_table('unique_closed_jobs')

        # Если таблицы нет в БД - создаем пустой датафрейм
        except RuntimeError:
            df_unique_closed_jobs = pd.DataFrame().from_dict({'href': [], 'publication_date': [],
                                                              'closing_date': [], 'date_diff': []})
            df_unique_closed_jobs['closing_date'] = pd.to_datetime(df_unique_closed_jobs['closing_date'])
            df_unique_closed_jobs['publication_date'] = pd.to_datetime(df_unique_closed_jobs['publication_date'])
            df_unique_closed_jobs['date_diff'] = df_unique_closed_jobs['date_diff'].astype(int)
            df_unique_closed_jobs['href'] = df_unique_closed_jobs['href'].astype(str)

        # Находим закрытые вакансии которых еще нет в БД
        df_merged_closed_jobs = pd.merge(df_unique_closed_jobs, df_merged, on='href', how='outer')
        df_merged_closed_jobs = df_merged_closed_jobs[pd.isnull(df_merged_closed_jobs['closing_date_x'])]
        df_merged_closed_jobs = df_merged_closed_jobs[['href', 'publication_date_y', 'closing_date_y', 'date_diff_y']]\
            .reset_index(drop=True)
        df_merged_closed_jobs.columns = ['href', 'publication_date', 'closing_date', 'date_diff']
        df_merged_closed_jobs['date_diff'] = df_merged_closed_jobs['date_diff'].astype(int)

        log.info(f'DataFrame for closed unique jobs table generated')
        return df_merged_closed_jobs

    def set_etl_data_to_bd(self):
        """Загрузка обработанных данных в базу данных"""

        self._pg_conn.set_table(self._get_df_for_main_table(), 'parsing_results', if_exists='append')
        self._pg_conn.set_table(self._get_df_for_jobs_table(), 'current_jobs', if_exists='replace')
        self._pg_conn.set_table(self._get_df_for_unique_jobs_table(), 'unique_jobs', if_exists='append')
        self._pg_conn.set_table(self._get_df_for_closed_unique_jobs_table(), 'unique_closed_jobs', if_exists='append')
