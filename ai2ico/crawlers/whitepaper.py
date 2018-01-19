import os
import numpy as np
import urllib
import textract
import multiprocessing
from urllib.request import urlopen
from functools import partial
from .drive_crawler import GoogleDriveDownloader


def get_drive_id(url):
    """Assumes the id is the longest chunk of the url."""
    # TODO: If it fails check it contains different characters
    chunks = url.split("/")
    max_len = np.argmax([len(chunk) for chunk in chunks])
    return chunks[max_len]



#def pdf_is_valid(path):
#    try:
#
#        return True if len(raw_text) > 1 else False
#    except:
#        return False
#def list_broken_pdf(folder_path):
#    broken_pdf = []
#    for pdf_name in os.listdir(folder_path):
#        filepath = os.path.join(folder_path, pdf_name)
#        if not pdf_is_valid(filepath):
#            broken_pdf.append(pdf_name)
#    return broken_pdf

def download_from_drive(url, dest_path, overwirte=False, unzip=False):
    file_id = get_drive_id(url)
    GoogleDriveDownloader.download_file_from_google_drive(file_id=file_id,
                                                          dest_path=dest_path,
                                                          unzip=unzip, overwrite=overwirte)


def download_wp(download_url, dest_path):

    if "drive.google" not in download_url and "docs.google" not in download_url:
        dest_path += ".pdf"
        if not os.path.exists(dest_path):
            try:
                req = urllib.request.Request(url=download_url,
                                             headers={'User-Agent': 'Mozilla/5.0'})
                response = urlopen(req)
                file = open(dest_path, 'wb')
                file.write(response.read())
                file.close()
                print("Completed: {}".format(download_url))
            except Exception as exc:
                print("FAILED: {} {}".format(exc, download_url))
        else:
            pass
            #print("Already downloaded {}, skipping...".format(dest_path.split("/")[-1]))
    else:
        if not os.path.exists(dest_path):
            download_from_drive(download_url, dest_path)
            print("Drive doc, downloading: {}".format(download_url))


def get_url_and_path(ico_row, folder_path):
    name = ico_row["name"]
    ticker = ico_row["ticker"]
    status = ico_row["status"]
    wp_url = ico_row["wp_url"]
    dest_path = os.path.join(folder_path, status, ticker + "_" + name)
    return wp_url, dest_path


def crawl_pdf(data, folder_path):
    _, ico = data
    wp_url, dest_path = get_url_and_path(ico, folder_path)
    #print("Saving: {}".format(dest_path.split("/")[-1]))
    download_wp(wp_url, dest_path)


class WhitePaperDownloader:

    def __init__(self, url_df, dest_folder, processes=8):
        self.dest_folder = dest_folder
        self.url_df = url_df
        self.processes = processes

    def save_whitepapers(self):
        _crawl_pdf = partial(crawl_pdf, folder_path=self.dest_folder)
        pool = multiprocessing.Pool()
        pool.map(_crawl_pdf, self.url_df.iterrows())
        pool.close()
        pool.join()






