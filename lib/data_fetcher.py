import re
import time
import random
import logging
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import quote

log = logging.getLogger(__name__)


class HhParser:
    """Парсер hh.ru"""

    def __init__(self, settings):
        """Инициализация экземпляра класса.

        :param dict settings: Настройки приложения.
        """
        self._search_period = settings['SEARCH_PERIOD']
        self._area = settings['AREA']
        self._search_text = settings['SEARCH_TEXT']
        self._regex = settings['SPECIFYING_REGEX']

    def parse(self):
        """Парсинг hh.ru.

        :raise: RuntimeError если нет результатов парсинга (пустой датафрейм).
        :return: Результаты и время парсинга.
        :rtype: tuple.
        """
        log.info(f'Parsing hh.ru with "{self._search_text}" query...')

        base_url = f'https://hh.ru/search/vacancy?search_period={self._search_period}&clusters=true&area={self._area}' \
                   f'&text={quote(self._search_text)}&enable_snippets=true&page=0'

        time_start = time.time()
        jobs_all = []
        urls = [base_url]
        session = requests.Session()

        # Базовый запрос для определения количества страниц с результатами парсинга
        response = self.__exponential_backoff(base_url, session)
        if response is not False:
            try:
                result = BeautifulSoup(response.content, 'lxml')
                pages = result.find_all('a', attrs={'data-qa': 'pager-page'})
                page_count = int(pages[-1].text)
                for i in range(page_count):
                    url = f'https://hh.ru/search/vacancy?search_period={self._search_period}&clusters=true&area=' \
                          f'{self._area}&text={quote(self._search_text)}&enable_snippets=true&page={i}'
                    if url not in urls:
                        urls.append(url)
                log.info(f'Found {len(urls)} pages with query "{self._search_text}" results')
            except Exception as e:
                log.debug(e)

        # Парсим все страницы в результатах выдачи
        for url in urls:
            response = self.__exponential_backoff(url, session)
            if response is not False:
                try:
                    result = BeautifulSoup(response.content, 'lxml')
                    divs = result.find_all('div', attrs={'data-qa': 'vacancy-serp__vacancy'})
                    divs_premium = result.find_all('div', attrs={
                        'data-qa': 'vacancy-serp__vacancy vacancy-serp__vacancy_premium'})
                    jobs_div = self._get_data_from_divs(divs)
                    jobs_divs_premium = self._get_data_from_divs(divs_premium)
                    jobs_all += jobs_div + jobs_divs_premium
                except Exception as e:
                    log.debug(e)

        # Формируем датафрейм из результатов парсинга
        df = pd.DataFrame(jobs_all)
        if len(df) == 0:
            log.error(f'No results found for settings: area={self._area}, period={self._search_period}, '
                      f'text={self._search_text}, specifying_regex={self._regex}')
            raise RuntimeError('No results found')
        else:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True)
            df = df[['date', 'title', 'salary', 'company', 'href']].sort_values(by='date', ascending=False)
            time_parse = round(time.time() - time_start, 2)
            log.info(f'Found {len(df)} jobs in {time_parse} seconds')
            return tuple([df, time_parse])

    def _job_check(self, title):
        """Проверка вакансии на уточняющее регулярное выражение.

        :param str title: Название вакансии.
        :return: Прошла проверку или нет.
        :rtype: bool.
        """
        if re.search(self._regex, title, flags=re.IGNORECASE):
            return True
        else:
            return False

    @staticmethod
    def _date_str_to_date(date_str):
        """Преобразование даты публикации вакансии.

        :param str date_str: Дата в строковом формате.
        :return: Преобразованная дата.
        :rtype: str.
        """
        date_dict = {'января': '01',
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
                     'декабря': '12'}

        date_arr = date_str.split(' ')
        for i in range(len(date_arr)):
            try:
                date_arr[i] = date_dict[date_arr[i]]
            except KeyError:
                pass

        # Добавляем год к дате
        date_arr.append(str(datetime.datetime.now().year))
        if datetime.datetime.strptime('.'.join(date_arr), '%d.%m.%Y') > datetime.datetime.now():
            date_arr[-1] = str(datetime.datetime.now().year - 1)

        date_string = '.'.join(date_arr)
        return date_string

    def _get_data_from_divs(self, divs):
        """Получение данных из блоков с вакансиями.

        :param list divs: Список блоков с вакансиями.
        :return: Данные о вакансиях.
        :rtype: list.
        """
        result = []
        for div in divs:
            # Название вакансии
            title = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-title'}).text

            # Проверка вакансии на уточняющую регулярку
            if not self._job_check(title):
                continue

            # Название компании
            if div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-employer'}) is None:
                company = 'Не определено'
            else:
                company = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-employer'}).text

            # Ссылка на вакансию
            href = div.find('a', attrs={'data-qa': 'vacancy-serp__vacancy-title'})['href']

            # Дата публикации вакансии
            date = self._date_str_to_date(
                div.find('span', attrs={'class': 'vacancy-serp-item__publication-date'}).text.replace('\xa0', ' '))

            # Зарплата
            if div.find('span', attrs={'data-qa': 'vacancy-serp__vacancy-compensation'}) is None:
                salary = 'Не указано'
            else:
                salary = div.find('span', attrs={'data-qa': 'vacancy-serp__vacancy-compensation'})\
                    .text.replace('\xa0', '')

            result.append({'title': title,
                           'company': company,
                           'salary': salary,
                           'date': date,
                           'href': href})
        return result

    @staticmethod
    def __exponential_backoff(url, session):
        """Экспоненциальная выдержка для 403, 500 и 503 ошибки.

        :param str url: URL, на который идет запрос.
        :param session: Открытая сессия Requests.
        :return: Ответ сервера или False при ошибке.
        """
        headers = {'accept': '*/*',
                   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/76.0.3809.100 Safari/537.36'}
        for n in range(0, 5):
            log.info(f'GET request to URL {url}')
            response = session.get(url, headers=headers)
            if response.status_code in [403, 500, 503]:
                log.info(f'HTTP error: {response.status_code}. Trying again. Attempt {n + 1}')
                time.sleep((2 ** n) + random.random())
            elif response.status_code == 200:
                return response
            else:
                log.error(f'HTTP error {response.status_code} during requesting URL: {url}')
                return False
        log.error(f'Failed request URL {url} in 5 attempts')
        return False
