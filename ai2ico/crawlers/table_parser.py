import os
from typing import Callable, Iterable
from urllib.request import urlopen
from bs4 import BeautifulSoup
import urllib
import pandas as pd
from ai2ico.text_processing import text_to_numeric


def fetch_html(target_url, verbose=False):
    """Loads and html page frmo the internet and returns it as a BeautifulSoup object"""
    if os.path.isfile(target_url):
        with open(target_url, "rb") as f:
            html = f.read()
    else:
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64;'
                                 ' rv:57.0) Gecko/20100101 Firefox/57.0',

                   }
        if verbose:
            print("Crawling {} ".format(target_url))
        req = urllib.request.Request(url=target_url, headers=headers)
        html = urlopen(req)
    if verbose:
        print("Creating soups...")
    soup = BeautifulSoup(html, "lxml")
    return soup


def parse_urls(soup: BeautifulSoup) -> list:
    """Parse all the urls from a BeautifulSoup object."""
    links = soup.findAll(name="a")
    return [link.attrs.get("href") for link in links]

def parse_tables(table_parser: Callable, resources: Iterable) -> pd.DataFrame:
    """
    parses
    :param table_parser: function that takes a resource as input and returns a DataFrame containing
     the data retrieved from the resource.
    :param resources: Iterable containing strings representing the resources to be parsed.
    :return: DataFrame containing the data gathered from all the resources.
    """
    return pd.concat([table_parser(res) for res in resources])


class BaseTableParser:

    def __init__(self, url: str, columns: (list, tuple)=None, numeric_columns: (list, tuple)=None,
                 table_id: str=None, table_class: str=None):
        self.numeric_cols = numeric_columns or []
        self.table_url = None
        self.table_id = None
        self.columns = columns
        self.url = url
        self.parent = fetch_html(self.url)
        self.table_id = table_id
        self.table_class = table_class
        self.child = self._get_table()

    def _get_table(self):
        attrs = {}
        if self.table_id is not None:
            attrs["id"] = self.table_id
        if self.table_class is not None:
            attrs["class"] = self.table_class
        return self.parent.find(name="table", attrs=attrs)

    def _create_df(self):
        """Transforms and html table into a DataFrame with the specified columns"""
        df = pd.read_html(self.child.decode())[0]
        # df = df.dropna(axis=1, how="all")  # Remove empty columns
        if self.columns is not None:
            df.columns = self.columns
        return df

    def _clean_data(self, df):
        df.loc[:,
               self.numeric_cols] = df.loc[:,
                                           self.numeric_cols].applymap(text_to_numeric).values
        return df

    def parse_table(self, url=None) -> pd.DataFrame:
        self.url = url or self.url
        self.parent = fetch_html(self.url)
        self.child = self._get_table()
        raw_data = self._create_df()
        data = self._clean_data(raw_data)
        return data
