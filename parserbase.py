"""Парсер сайта
"""

import asyncio
import json
import os.path
import time
from datetime import datetime

import aiohttp
from aiohttp import ServerDisconnectedError
from asyncio import sleep
from bs4 import BeautifulSoup
from fake_useragent import FakeUserAgent
from urllib.parse import urljoin, urlencode
import requests
import pandas as pd
from config import logger


class AnkasLooper:
    def __init__(self):
        self.live = True

    async def run(self):
        while self.live:
            print("Live")
            await sleep(0.1)

    def stop(self):
        self.live = False


class AnkasBase:
    """
    Базовый класс парсера
    """

    base_url = "https://ankas.ru/"

    def __init__(self, url, headers=None):
        self.page = self.get_page(url=url, headers=headers)

    def make_url(self, url):
        return urljoin(self.base_url, url)

    def get_page(self, url, headers):
        response = requests.get(url=url, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.text

    def is_good_page(self):
        # g-content g-content_goodcart
        if self.page is not None:
            soup = BeautifulSoup(self.page, "lxml")
            div = soup.find("div", class_="g-content g-content_goodcart")
            if div:
                return True
            else:
                return False
        else:
            return False


class GoodPageParse(AnkasBase):
    def __init__(self, url=None, page_text=None):
        self.url = url
        self.page = page_text

    def get_info(self) -> dict | None:
        # Характеристики: b-good-specs__wrap spec
        # название b-good-cards__title
        if self.is_good_page():
            soup = BeautifulSoup(self.page, "lxml")
            title = soup.find("h1", class_="b-good-cards__title").text
            specs_block = soup.find("div", class_="b-good-specs__wrap spec")
            specs_raw_list = specs_block.find_all(
                "div", class_="b-good-specs__item row2"
            )

            images_list_block = soup.find("div", class_="b-good-cards__photo-block")
            images_list_block = images_list_block.find_all("div", class_="metaTags")
            images_links = []
            for image_block in images_list_block:
                img_link = image_block.find_all("link", {"itemprop": "contentUrl"})[0][
                    "href"
                ]
                images_links.append(img_link)

            specs_list = []

            for spec_raw in specs_raw_list:
                spec_name = (
                    str(spec_raw.find("span", class_="name g_tool_tip_container").text)
                    .strip()
                    .strip(":")
                )
                spec_value = spec_raw.find(
                    "span", class_="b-good-specs__content"
                ).text.strip()
                specs_list.append({"name": spec_name, "value": spec_value})
            specs_dic = {"title": title, "images": images_links, "specs": specs_list}
            return specs_dic

    async def async_get_page(self, url=None, headers=None, repeats=3):
        try:
            async with aiohttp.ClientSession() as session:
                if url is None:
                    url = self.url
                # TODO: Ставим прокси
                proxy = ""
                r = 0
                while r <= 3:
                    r += 1
                    agent = FakeUserAgent()
                    headers = {
                        "User-Agent": agent.random,
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "DNT": "1",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                        "Sec-Fetch-User": "?1",
                        "Sec-GPC": "1",
                        "Priority": "u=1",
                        "TE": "trailers",
                    }
                    # print(headers)
                    response = await session.get(url=url, headers=headers, proxy=proxy)
                    if response.status == 200:
                        # print(response.headers)
                        return await response.text()
                    else:
                        logger.debug(
                            f"Ошибка запроса к {url}, попытка №{r} из {repeats}, код {response.status}. Повторяем запрос через 0.5 сек."
                        )
                        await asyncio.sleep(0.5)
                logger.warning(
                    f"Не удалось обратиться к {url}. Исчерапаны попытки {r} из {repeats}"
                )
        except ServerDisconnectedError as e:
            logger.error(f"Ошибка запроса: {e}")


class CategoryPageParser(AnkasBase):

    def get_products_on_page(self):
        soup = BeautifulSoup(self.page, "lxml")
        goods_list = soup.find("div", class_="goods-list")
        inside_good = goods_list.find_all("div", class_="b-good__inside good")
        logger.debug(f"На странице найдено {len(inside_good)} товаров")
        for ig in inside_good:
            link = ig.find("a", class_="b-good__title-link")
            title: str = link["title"]
            href: str = link["href"]
            count = len(inside_good)
            yield title, self.make_url(href), count


class CategoryParser(AnkasBase):
    """
    Парсит страницу категории и получает перечень товаров с неё при помощи метода get_goods().
    """

    def __init__(self, category_url):
        self.category_url = category_url
        super().__init__(category_url)
        self.pages_num = self.__get_pages_count()

    def __get_pages_count(self):
        soup = BeautifulSoup(self.page, "lxml")
        pagination = soup.find_all("a", class_="b-pagination__num")
        if pagination is not None:
            try:
                pagination = pagination[-1].text
                return int(pagination)
            except IndexError as e:
                logger.error(
                    f"Ошибка при получении количества страниц категории {self.category_url}. {e}. Получили {pagination}"
                )
        else:
            logger.error(f"Не удалось получить пагинацию: {self.category_url}")

    def get_category_pages(self):
        if type(self.pages_num) is int:
            for page in range(1, self.pages_num + 1):
                page_url = ""
                if page == 1:
                    page_url = self.category_url
                else:
                    params = urlencode({"page": page})
                    page_url = f"{self.category_url}/?{params}"
                yield page_url

    def get_goods(self, sleep_=0.1):
        """http://hpg4rsjvgw-res-country-RU-state-524894-city-524901-hold-session-session-669be64a60e1a:e8dKQi3pak4n5mAY@93.190.138.107:9999

        :param sleep_: задержка между запросами.
        :return: Возвращает генератор с двумя полями: t: str и h: str, где: t - название товара h - ссылка на страницу товара.
        """
        for link in self.get_category_pages():
            cpp = CategoryPageParser(link)
            time.sleep(sleep_)
            logger.info(f"Парсим страницу: {link}")
            for t, h, c in cpp.get_products_on_page():
                yield t, h, c

            return


class Parser(AnkasBase):

    def __init__(self):
        self.base_url = "https://ankas.ru/"
        self.categories = pd.DataFrame(["Title", "Url", "GoodsList"])
        self.filename = f"goods_raw_{datetime.now().strftime("%Y%m%d-%H%M%S%f")}.csv"

    def load_categories(self, filename="data/good_cats.csv"):
        self.categories = pd.read_csv(filename, sep="|")
        print(self.categories)

    def gather_categories(
        self, refer_title: str = None, refer_url: str = None, url=None, sleep_=0.1
    ):
        """
        Собирает с сайта ссылки на страницы категорий, которые содержат товары, а не категории (с повторениями), в рекурсии
        :param refer_title:
        :param refer_url:
        :param url:
        :param sleep:
        :return:
        """
        time.sleep(sleep_)
        if url is None:
            url = "https://ankas.ru/"
        else:
            url = urljoin(self.base_url, url)
        with requests.session() as session:
            response = session.get(url=url)
            soup = BeautifulSoup(response.text, "lxml")
            categories_block = soup.find(
                "div",
                class_="b-goods-list b-goods-list_type_wide b-goods-list_bottom-line",
            )
            if categories_block is not None:
                category_elements = categories_block.find_all(
                    "div", class_="b-goods-list__item"
                )
                for element in category_elements:
                    a = element.find("a", class_="b-good__title-link")
                    title = a["title"]
                    href = a["href"]
                    self.gather_categories(refer_title=title, refer_url=url, url=href)
            else:
                with open("data/good_cats_raw.csv", "a+", encoding="utf-8") as f:
                    logger.info(f"Страница с товарами:{refer_title}|{url}")
                    f.write(f"{refer_title}|{url}\n")


async def main():
    p = Parser()
    p.load_categories()

    # exit()
    for cat in p.categories["Url"]:
        tasks = []
        pool = 10
        i = 0
        for t, u, c in CategoryParser(cat).get_goods(sleep_=0):
            good = GoodPageParse(u)
            tasks.append(asyncio.create_task(good.async_get_page()))
            i += 1
        result = await asyncio.gather(*tasks)
        tasks = []
        start = time.perf_counter()

        for x, r in enumerate(result):
            good = GoodPageParse(page_text=r).get_info()
            if good is not None:
                with open(os.path.join("data", p.filename), "a+", encoding='utf-8') as f:
                    line = f"{good['title']}|{json.dumps(good['specs'], ensure_ascii=False)}|{json.dumps(good['images'])}\n"
                    logger.debug(f"{x}: {good['title']}")
                    f.write(line)
        logger.debug(
            f"Парсинг страницы товаров занял {(time.perf_counter() - start):.02f}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Принудительное завершение приложения")
