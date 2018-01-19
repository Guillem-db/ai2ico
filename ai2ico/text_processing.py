import re
import numpy as np
import pandas as pd
from nltk import word_tokenize
from gensim import corpora
from nltk.corpus import stopwords
from collections import defaultdict
from bs4 import BeautifulSoup
import multiprocessing
from multiprocessing.pool import ThreadPool as Pool

STOPWORDS_ENGLISH = stopwords.words("english")


def text_to_numeric(text: str, dtype=np.float64):
    """Removes non numeric characters from a string and transforms it into the selected data type.
    Returns np.nan in case of ValueError when casting the string to numeric dtype.
    """
    if isinstance(text, str):
        only_number_chars = "[^0-9\.\-]"
        clean_str = re.sub(only_number_chars, "", text)
        try:
            return dtype(clean_str)
        except ValueError:
            return np.nan
    else:
        return np.nan


def only_one_space(text: str) -> str:
    """Transforms multiple spaces into a single space."""
    return " ".join(text.split())


def remove_urls(text: str) -> str:
    """Removes urls from a given text."""
    match_url = r"(https?://([-\w\.]+[-\w])+(:\d+)?(/([\w/_\.#-]*(\?\S+)?[^\.\s])?).*$)"
    words = [re.sub(match_url, "", word) for word in text.split()]
    return " ".join(words)


def remove_non_letters(text: str, keep_points: bool=False) -> str:
    """Removes any non letter character. optionally keep the point for separating into sentences"""
    keep_letters = "[^a-zA-Z.]" if keep_points else "[^a-zA-Z]"
    letters_only = re.sub(keep_letters, " ", text)
    return letters_only


def remove_hex(text: str) -> str:
    """Removes parsed hex values that follow a given pattern. default \\"""
    remove_hex = "\\\\[a-z][0-9a-f]{1,}"
    return re.sub(remove_hex, "", text).replace("\\n", "")


def remove_html(text: str) -> str:
    """Removes html syntax from a string representing a text."""
    return BeautifulSoup(text, "xmlx").text


def remove_word_len(text: str, min_lenght: int = 3) -> str:
    """Remove words from a string that do not meet the minimum length required."""
    return " ".join([word for word in text.split() if len(word) >= min_lenght])


def remove_stopwords(text: str, stops=None) -> str:
    """Removes the stopwords from a given text."""
    stops = stops or STOPWORDS_ENGLISH
    stops = set(stops)
    meaningful_words = [w for w in text.split() if w not in stops]
    return " ".join(meaningful_words)


def clean_string(text: str, min_lenght=None,
                 keep_points=False,
                 keep_stopwords=False,
                 stopwords=None,
                 lower=True,
                 remove_html=False,
                 only_letters=True) -> str:
    """
    Clean a string with several filtering techniques.
    :param text: the string to be cleaned.
    :param min_lenght: minimum word length that will be kept.
    :param keep_points: do not remove "." from the text str.
    :param keep_stopwords: do not remove stopwords from the text str.
    :param stopwords: list of stopwords that will be removed.
    :param lower: remove capital letters from the text str
    :param remove_html: remove html code from the text str.
    :param only_letters: remove any character not belonging to the english alphabet.
     If keep_points is True, the "." character will also be kept.
    :return: filtered text string.
    """
    non_url = remove_urls(text)
    non_hex = remove_hex(non_url)
    lowered = non_hex if not lower else non_hex.lower()

    min_len = remove_word_len(lowered, min_lenght=min_lenght) if min_lenght else lowered

    non_stops = min_len if keep_stopwords else remove_stopwords(min_len, stops=stopwords)
    non_html = remove_html(non_stops) if remove_html else non_stops
    only_letters = remove_non_letters(non_html,
                                      keep_points=keep_points) if only_letters else non_html
    one_space = only_one_space(only_letters)
    return one_space


def keep_tokens_above_freq(word_texts, min_freq=1, min_length=3):
    frequency = defaultdict(int)
    # Count token frequency
    for text in word_texts:
        for token in text:
            frequency[token] += 1
    filtered_texts = [[token for token in text if frequency[token] >= min_freq
                       and len(token) >= min_length]
                      for text in word_texts]
    return filtered_texts


def pool_map(func, vector, processes=None):
    processes = processes or multiprocessing.cpu_count()
    pool = Pool(processes=processes)
    data = pool.map(func, vector)
    pool.close()
    pool.join()
    return data


def _pool_cleanstr_iter(data):
    _, row = data
    return clean_string(str(row.text))


class TextProcessor:

    def __init__(self, raw_data, num_workers=100):
        self.raw_data = raw_data
        self._num_workers = num_workers
        self.dictionary = None
        self.bow = None
        self.tokens = None
        self.clean_data = None

    def clean_texts(self):
        self.clean_data = pd.DataFrame(index=self.raw_data.index, columns=["text"])
        self.clean_data.loc[:, "text"] = pool_map(_pool_cleanstr_iter,
                                                  self.raw_data.iterrows(),
                                                  processes=self._num_workers)

    def tokenize_text(self, min_freq=2, min_length=3):
        self.tokens = pd.DataFrame(index=self.raw_data.index, columns=["text"])
        tokens = pool_map(word_tokenize,
                          self.clean_data["text"].values,
                          processes=self._num_workers)
        # can be parallelized creating the dict first and using it for freq filtering
        tokens = keep_tokens_above_freq(tokens, min_freq=min_freq, min_length=min_length)
        self.dictionary = corpora.Dictionary(tokens)
        self.tokens.loc[:, "text"] = tokens

    def calculate_bow(self):
        self.bow = [self.dictionary.doc2bow(text) for text in self.tokens["text"].values]





