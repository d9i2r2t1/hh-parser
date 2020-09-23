import datetime
import logging
import random
import re
import time
from typing import Iterator, List, Union, Dict
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from hh_parser.lib import ConnPostgreSQL

log = logging.getLogger(__name__)


class HhParser:
    """Парсер hh.ru."""

    def __init__(self, area: int, search_period: int, search_text: str, search_regex: str) -> None:
        """
        :param area: Регион поиска (1 - Москва)
        :param search_period: Период поиска в днях
        :param search_text: Поисквовый запрос
        :param search_regex: Уточняющая регулярка для названия вакансии
        """
        self.__area = area
        self.__search_period = search_period
        self.__search_text = search_text
        self.__search_regex = search_regex
        self.__base_url = 'https://hh.ru/search/vacancy'
        self.__url_params = {
            'search_period': self.__search_period,
            'clusters': 'true',
            'area': self.__area,
            'text': quote(self.__search_text),
            'enable_snippets': 'true',
            'page': 0
        }
        self.__session = requests.Session()
        self.__headers = {
            'accept': '*/*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/76.0.3809.100 Safari/537.36'
        }

    area = property(lambda self: self.__area)
    search_period = property(lambda self: self.__search_period)
    search_text = property(lambda self: self.__search_text)
    search_regex = property(lambda self: self.__search_regex)

    class HhParserResults:
        """Результаты парсинга hh."""

        def __init__(self, data: pd.DataFrame) -> None:
            """
            :param data: Данные парсинга
            """
            self.__data = data
            self.area = None
            self.search_period = None
            self.search_text = None
            self.search_regex = None
            self.parse_duration = None
            self.df_parsing_results = None
            self.df_current_jobs = None
            self.df_unique_jobs = None
            self.df_unique_closed_jobs = None

        data = property(lambda self: self.__data)

    @staticmethod
    def _get_url_with_params(url: str, params: dict) -> str:
        """
        Сформируй URL с параметрами.
        :param url: URL
        :param params: Параметры URL
        """
        return f'{url}?' + '&'.join([f'{k}={v}' for k, v in params.items()])

    def _get_urls_pages_with_vacancies(self) -> Iterator[str]:
        """Получи URL страниц с вакансиями."""
        start_url = self._get_url_with_params(self.__base_url, self.__url_params)
        urls = [start_url]
        response = self.__exponential_backoff(start_url)
        if response is not False:
            result = BeautifulSoup(response.content, 'lxml')
            pages = result.find_all('a', attrs={'data-qa': 'pager-page'})
            page_count = int(pages[-1].text)
            url_params = self.__url_params
            for i in range(page_count):
                url_params['page'] = i + 1
                urls.append(self._get_url_with_params(self.__base_url, url_params))
            log.info(f'Found {len(urls)} pages with "{self.__search_text}" vacancies')
            yield from urls
        else:
            log.error(f'Start request failed')
            raise RuntimeError('Request failed')

    def run(self) -> HhParserResults:
        """Запусти парсер."""
        time_start = time.monotonic()
        log.info(f'Looking for "{self.__search_text}" vacancies on hh.ru...')
        vacancies_pages_urls = self._get_urls_pages_with_vacancies()

        raw_vacancies_data = []
        url_counter = 1
        for url in vacancies_pages_urls:
            log.info(f'Parsing page {url_counter}...')
            response = self.__exponential_backoff(url)
            if response is not False:
                result = BeautifulSoup(response.content, 'lxml')
                vacancies_divs = result.find_all('div', attrs={
                    'data-qa': 'vacancy-serp__vacancy'
                })
                premium_vacancies_divs = result.find_all('div', attrs={
                    'data-qa': 'vacancy-serp__vacancy vacancy-serp__vacancy_premium'
                })
                vacancies_data = self._get_data_from_divs(vacancies_divs)
                premium_vacancies_data = self._get_data_from_divs(premium_vacancies_divs)
                raw_vacancies_data += vacancies_data + premium_vacancies_data
            else:
                log.error(f'Request failed')
                raise RuntimeError('Request failed')
            url_counter += 1

        df = pd.DataFrame(raw_vacancies_data)
        if len(df) == 0:
            log.error(f'No results found for settings: area={self.__area}, period={self.__search_period}, '
                      f'text={self.__search_text}, specifying_regex={self.__search_regex}')
            raise RuntimeError('No results found')
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)
        df = df[['date', 'title', 'salary', 'company', 'href']].sort_values(by='date', ascending=False)

        parse_duration = round(time.monotonic() - time_start, 2)
        log.info(f'Found {len(df)} vacancies in {parse_duration} seconds')
        results = self.HhParserResults(df)
        results.parse_duration = parse_duration
        results.area = self.__area
        results.search_period = self.__search_period
        results.search_text = self.__search_text
        results.search_regex = self.__search_regex
        return results

    def _vacancy_name_check(self, title: str) -> bool:
        """
        Проверь название вакансии уточняющим регулярным выражением.
        :param title: Название вакансии
        """
        if re.search(self.__search_regex, title, flags=re.IGNORECASE):
            return True
        return False

    @staticmethod
    def _process_date(raw_date: str) -> str:
        """
        Преобразуй дату публикации вакансии.
        :param raw_date: Дата из вакансии
        """
        date_dict = {
            'января': '01',
            'февраля': '02',
            'марта': '03',
            'апреля': '04',
            'мая': '05',
            'июня': '06',
            'июля': '07',
            'августа': '08',
            'сентября': '09',
            'октября': '10',
            'ноября': '11',
            'декабря': '12'
        }
        date_arr = raw_date.split(' ')
        for i in range(len(date_arr)):
            try:
                date_arr[i] = date_dict[date_arr[i]]
            except KeyError:
                pass

        # Добавляем год к дате
        date_arr.append(str(datetime.datetime.now().year))
        if datetime.datetime.strptime('.'.join(date_arr), '%d.%m.%Y') > datetime.datetime.now():
            date_arr[-1] = str(datetime.datetime.now().year - 1)

        return '.'.join(date_arr)

    def _get_data_from_divs(self, divs: List) -> List[dict]:
        """
        Получи данные из блоков с вакансиями.
        :param divs: Блоки с вакансиями
        """
        results = []
        for div in divs:
            title = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-title'}).text
            if not self._vacancy_name_check(title):
                continue

            company_data = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-employer'})
            company = company_data.text if company_data else 'Не определено'
            href = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-title'}).get('href')
            date = self._process_date(
                div.find('span', attrs={'class': 'vacancy-serp-item__publication-date'}).text.replace('\xa0', ' ')
            )
            salary_data = div.find('span', attrs={'data-qa': 'vacancy-serp__vacancy-compensation'})
            salary = salary_data.text.replace('\xa0', '') if salary_data else 'Не указано'

            results.append({'title': title, 'company': company, 'salary': salary, 'date': date, 'href': href})

        return results

    def __exponential_backoff(self, url: str) -> Union[requests.Response, bool]:
        """
        Экспоненциальная выдержка для 403, 500 и 503 ошибки.
        :param url: URL запроса
        :return: Ответ сервера или False при ошибке
        """
        for n in range(0, 5):
            log.debug(f'GET request to URL {url}')
            response = self.__session.get(url, headers=self.__headers)
            if response.status_code in [403, 500, 503]:
                log.debug(f'HTTP error: {response.status_code}. Trying again. Attempt {n + 1}')
                time.sleep((2 ** n) + random.random())
            elif response.status_code == 200:
                return response
            else:
                log.error(f'HTTP error {response.status_code} during requesting URL: {url}')
                return False
        log.error(f'Failed request URL {url} in 5 attempts')
        return False


class HhParserResultsProcessor:
    """Обработка результатов парсинга."""

    def __init__(self, hh_parsed_data: HhParser.HhParserResults, pg_conn=ConnPostgreSQL) -> None:
        """
        :param hh_parsed_data: Результаты парсинга
        :param pg_conn: Активное подключение к PostgreSQL
        """
        self.__hh_parsed_data = hh_parsed_data
        self.__df = hh_parsed_data.data
        self.__parsing_duration = hh_parsed_data.parse_duration
        self.__pg_conn = pg_conn

    hh_parsed_data = property(lambda self: self.__hh_parsed_data)
    report_folder = property(lambda self: self.__report_folder)

    def run(self) -> HhParser.HhParserResults:
        """Запусти обработку результатов парсинга."""
        self._get_parsing_results_df()
        self._get_current_jobs_df()
        self._get_unique_jobs_df()
        self._get_unique_closed_jobs_df()
        return self.__hh_parsed_data

    def _find_jobs_without_salary(self) -> Dict[str, Union[int, float]]:
        """Найди % вакансий без указания зарплаты."""
        unknown_salary_count = self.__df.loc[self.__df['salary'] == 'Не указано']['salary'].count()
        unknown_salary_percent = round((unknown_salary_count / len(self.__df)) * 100, 2)
        log.info(f'Jobs without salary: {unknown_salary_percent}%')
        return {'jobs_without_salary': unknown_salary_percent}

    def _find_salary_mean_and_median(self) -> Dict[str, Union[int, float]]:
        """Найди медианную, среднюю, среднюю максимальную и средней минимальную зарплаты."""
        salaries_min = []
        salaries_max = []
        for i in range(len(self.__df)):
            # Указана зарплата "от"
            if self.__df.loc[i, 'salary'].split()[0] == 'от':
                salaries_min.append(int(self.__df.loc[i, 'salary'].split()[1]))
            # Указана зарплата "до"
            elif self.__df.loc[i, 'salary'].split()[0] == 'до':
                salaries_max.append(int(self.__df.loc[i, 'salary'].split()[1]))
            # Указана вилка зарплаты
            elif len(self.__df.loc[i, 'salary'].split()[0].split('-')) == 2:
                fork = self.__df.loc[i, 'salary'].split()[0].split('-')
                salaries_min.append(int(fork[0]))
                salaries_max.append(int(fork[1]))
            # Зарплата не указана
            elif self.__df.loc[i, 'salary'] == 'Не указано':
                pass
            # Указана фиксированная зарплата
            else:
                salaries_min.append(int(self.__df.loc[i, 'salary'].split()[0]))
                salaries_max.append(int(self.__df.loc[i, 'salary'].split()[0]))

        salaries_all = salaries_min + salaries_max
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

    def _get_parsing_results_df(self) -> None:
        """Сформируй датафрейм для таблицы "parsing_results"."""
        data_for_update = {}
        data_for_update.update(self._find_jobs_without_salary())
        data_for_update.update(self._find_salary_mean_and_median())
        data_for_update.update({'jobs_count': len(self.__df),
                                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                                'time_parse': self.__parsing_duration})
        df = pd.DataFrame([data_for_update])
        df['date'] = pd.to_datetime(df['date'])
        self.__hh_parsed_data.df_parsing_results = df
        log.info(f'DataFrame for "parsing_results" table generated')

    def _get_current_jobs_df(self) -> None:
        """Сформируй датафрейм для таблицы "current_jobs"."""
        min_salary = []
        max_salary = []
        df = self.__df.copy().reset_index(drop=True)
        for i in range(len(df)):
            # Указана зарплата "от"
            if df.loc[i, 'salary'].split()[0] == 'от':
                min_salary.append(int(df.loc[i, 'salary'].split()[1]))
                max_salary.append(int(df.loc[i, 'salary'].split()[1]))
            # Укащана зарплата "до"
            elif df.loc[i, 'salary'].split()[0] == 'до':
                min_salary.append(0)
                max_salary.append(int(df.loc[i, 'salary'].split()[1]))
            # Указана вилка зарплаты
            elif len(df.loc[i, 'salary'].split()[0].split('-')) == 2:
                fork = df.loc[i, 'salary'].split()[0].split('-')
                min_salary.append(int(fork[0]))
                max_salary.append(int(fork[1]))
            # Зарплата не указана
            elif df.loc[i, 'salary'] == 'Не указано':
                min_salary.append(0)
                max_salary.append(0)
            # Указана фиксированная зарплата
            else:
                min_salary.append(int(df.loc[i, 'salary'].split()[0]))
                max_salary.append(int(df.loc[i, 'salary'].split()[0]))

        df['min_salary'] = min_salary
        df['max_salary'] = max_salary
        df['mean_salary'] = (df['min_salary'] + df['max_salary']) / 2
        df = df.sort_values(['mean_salary', 'max_salary', 'min_salary'], ascending=False).reset_index(drop=True)
        df['row'] = list(range(1, len(df) + 1))
        self.__hh_parsed_data.df_current_jobs = df[['row', 'date', 'title', 'company', 'salary', 'href']]
        log.info(f'DataFrame for "current_jobs" table generated')

    def _get_unique_jobs_merged_df(self) -> pd.DataFrame:
        """Получи сджойненый датафрейм уникальных вакансий из Postgres и результатов парсинга."""
        pg_unique_jobs_raw = self.__pg_conn.get_table(table_name='unique_jobs')
        pg_unique_jobs = self._get_df_from_pgtable(pg_unique_jobs_raw)
        if pg_unique_jobs is None or pg_unique_jobs.empty:
            pg_unique_jobs = pd.DataFrame.from_dict({'date': [], 'href': []})
            pg_unique_jobs['date'] = pd.to_datetime(pg_unique_jobs['date'])
            pg_unique_jobs['href'] = pg_unique_jobs['href'].astype(str)
        r = pd.merge(pg_unique_jobs, self.__df[['date', 'href']], on='href', how='outer')
        return r

    @staticmethod
    def _get_df_from_pgtable(pg_table: ConnPostgreSQL.PgTable) -> Union[pd.DataFrame, None]:
        """
        Получи датафрейм из PgTable.
        :param pg_table: Таблица Postgres
        :return: Датафрейм, если есть данные, в противном случае - None
        """
        if not pg_table:
            return
        df_from_pg = pd.DataFrame()
        for df in pg_table.table_data:
            df_from_pg = df_from_pg.append(df)
        return df_from_pg

    def _get_unique_jobs_df(self) -> None:
        """Сформируй датафрейм для таблицы "unique_jobs"."""
        df_merged = self._get_unique_jobs_merged_df()
        df_merged = df_merged[pd.isnull(df_merged['date_x'])][['date_y', 'href']].reset_index(drop=True)
        df_merged.columns = ['date', 'href']
        self.__hh_parsed_data.df_unique_jobs = df_merged
        log.info(f'DataFrame for "unique_jobs" table generated')

    def _get_unique_closed_jobs_df(self) -> None:
        """Сформируй датафрейм для таблицы "unique_closed_jobs"."""
        df_merged = self._get_unique_jobs_merged_df()
        df_merged = df_merged[pd.isnull(df_merged['date_y'])].reset_index(drop=True)
        df_merged.columns = ['publication_date', 'href', 'closing_date']
        df_merged['closing_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        df_merged['href'] = df_merged['href'].astype(str)
        df_merged['closing_date'] = pd.to_datetime(df_merged['closing_date'])
        df_merged['publication_date'] = pd.to_datetime(df_merged['publication_date'])
        df_merged['date_diff'] = (df_merged['closing_date'] - df_merged['publication_date']).dt.days.astype(int)

        pg_unique_closed_jobs_raw = self.__pg_conn.get_table(table_name='unique_closed_jobs')
        pg_unique_closed_jobs = self._get_df_from_pgtable(pg_unique_closed_jobs_raw)
        if pg_unique_closed_jobs is None or pg_unique_closed_jobs.empty:
            pg_unique_closed_jobs = pd.DataFrame().from_dict({
                'href': [],
                'publication_date': [],
                'closing_date': [],
                'date_diff': []
            })
            pg_unique_closed_jobs['closing_date'] = pd.to_datetime(pg_unique_closed_jobs['closing_date'])
            pg_unique_closed_jobs['publication_date'] = pd.to_datetime(pg_unique_closed_jobs['publication_date'])
            pg_unique_closed_jobs['date_diff'] = pg_unique_closed_jobs['date_diff'].astype(int)
            pg_unique_closed_jobs['href'] = pg_unique_closed_jobs['href'].astype(str)

        df_merged_closed_jobs = pd.merge(pg_unique_closed_jobs, df_merged, on='href', how='outer')
        df_merged_closed_jobs = df_merged_closed_jobs[pd.isnull(df_merged_closed_jobs['closing_date_x'])]
        df_merged_closed_jobs = df_merged_closed_jobs[['href', 'publication_date_y', 'closing_date_y', 'date_diff_y']]\
            .reset_index(drop=True)
        df_merged_closed_jobs.columns = ['href', 'publication_date', 'closing_date', 'date_diff']
        df_merged_closed_jobs['date_diff'] = df_merged_closed_jobs['date_diff'].astype(int)
        self.__hh_parsed_data.df_unique_closed_jobs = df_merged_closed_jobs
        log.info(f'DataFrame for "unique_closed_jobs" table generated')
