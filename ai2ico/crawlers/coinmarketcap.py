import numpy as np
import pandas as pd
import time

import multiprocessing
from multiprocessing.pool import ThreadPool as Pool
from ai2ico.crawlers.table_parser import fetch_html, parse_urls, BaseTableParser


def pool_exec(func, vector, processes=None):
    processes = processes or multiprocessing.cpu_count()
    pool = Pool(processes=processes)
    data = pool.map(func, vector)
    pool.close()
    pool.join()
    return data


def parse_one_history(url):
    print("Parsing {}".format(url))
    time.sleep(np.random.random() + 0.1)
    table_parser = TableParser(url=url)
    return table_parser.parse_data(), url


class TableParser(BaseTableParser):

    table_url = "https://coinmarketcap.com/all/views/all/"
    table_id = "currencies-all"
    columns = ["index", "name",	"symbol", "market_cap",	"price", "circulating_supply",
               "volume", "change_1h", "change_24h", "change_7d"]

    def __init__(self, url=None):
        url = url if url is not None else self.table_url
        super(TableParser, self).__init__(url=url,
                                          columns=self.columns,
                                          numeric_columns=self.columns[3:])


class HistoryLinkParser:

    site_url = "https://coinmarketcap.com"
    default_url = "https://coinmarketcap.com/historical/"
    class_name = "col-sm-4 col-xs-6"

    def __init__(self, url=None, class_name=None):
        self.url = url or self.default_url
        self.class_name = class_name or self.class_name
        self.parent = fetch_html(self.url)
        self.soups = self._get_containers()
        self.history_urls = {}


    def _get_containers(self):
        return self.parent.findAll(name="div", attrs={"class": self.class_name})

    def _parse_urls(self):
        url_lists = [parse_urls(soup) for soup in self.soups]
        urls = []
        for url_list in url_lists:
            for url in url_list:
                urls.append(url)
        return urls

    def create_urls(self):
        return [self.site_url + url for url in self._parse_urls()]


class HistoryDataParser:

    def __init__(self):
        self.link_parser = HistoryLinkParser()
        self.history_links = None

    def _parse_links(self):
        self.history_links = self.link_parser.create_urls()

    def _fetch_history(self):
        data = pool_exec(parse_one_history, list(self.history_links))
        history_dict = {url: data for data, url in data}
        return history_dict
