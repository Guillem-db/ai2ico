import os
import multiprocessing
import numpy as np
from multiprocessing.pool import ThreadPool as Pool
import textract
import pandas as pd
from ai2ico.text_processing import clean_string#, documents_to_words, keep_tokens_above_freq, create_dictionary







def pdf_to_text(path: str, clean=False) -> tuple:
    """
    Loads the pdf located in the specified path and return a list of words contained in it.
    :param path: Location of the pdf.
    :return:
    """
    # TODO: clean this fukin mess
    filename = path.split("/")[-1]
    file_id = filename.split(".")[0]
    try:
        raw_text = textract.process(path, method='pdfminer')

        print("Parsed {}".format(filename))
        return file_id, clean_string(raw_text) if clean else raw_text
    except Exception as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise exc
        print("Error parsing {}".format(path.split("/")[-1]))
        return file_id, None



def process_text_file(path: str) -> tuple:
    try:
        with open(path, "r") as f:
            file = f.read()
            filename = path.split("/")[-1]
            file_id = filename.split(".")[0]
            return file_id, clean_string(file)
    except Exception as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise exc
        print("Error parsing {}".format(path.split("/")[-1]))
        raise exc
        return file_id, None


class TextLoader:

    def __init__(self, folder_path: str):
        self.folder_path = folder_path
        self.ticker_index = None
        self.text_dict = {}

    def _create_data_urls(self):

        # TODO: generalise, this is an awful hack
        data_urls = ["upcoming", "past", "current"]
        return [os.path.join(self.folder_path, url) for url in data_urls]

    def _pool_exec(self, func, vector, processes=None):
        processes = processes or multiprocessing.cpu_count()
        pool = Pool(processes=processes)
        data = pool.map(func, vector)
        pool.close()
        pool.join()
        return data


    def load_texts(self, folder_path: str=None, processes: int=None) -> pd.DataFrame:
        #folder_path = folder_path or self.folder_path
        self.text_dict = {}
        for folder_path in self._create_data_urls():
            files = os.listdir(folder_path)
            paths = [os.path.join(folder_path, file) for file in files]
            loaded_data = self._pool_exec(pdf_to_text, paths, processes=processes)

            self.text_dict.update({ticker: text for ticker, text in loaded_data})

        self.ticker_index = np.array(list(self.text_dict.keys()))
        for key in list(self.text_dict.keys()):
            if not self.text_dict[key]:
                self.text_dict.pop(key)

        index, data = [], []
        for key, val in self.text_dict.items():
            index.append(key)
            data.append(val)
        return pd.DataFrame(index=index, data=data, columns=["text"])


    def create_nlp_objects(self, minfreq=2, processes=None):
        ticker_index, dictionary, filtered_texts = create_dictionary(self.text_dict,
                                                                     minfreq=minfreq)
        corpus = self._pool_exec(dictionary.doc2bow, filtered_texts, processes=processes)

        return corpus, ticker_index, dictionary, filtered_texts
        #corpus = [dictionary.doc2bow(text) for text in filtered_texts]