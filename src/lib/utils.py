# utils.py

import datetime

def fname_from_url(url):

    start_idx = url.rfind('/') + 1
    file_name = url[start_idx:]
    return file_name


def date_from_url(zip_url):

    zipdate = zip_url[zip_url.index(
        "gdeltv2") + 8: zip_url.index(".gkg.csv.zip")].strip()
    zipdate = datetime.datetime(
            int(zipdate[:4]), int(zipdate[4:6]), int(zipdate[6:8]), 
            int(zipdate[8:10]), int(zipdate[10:12]), int(zipdate[12:]))
    return zipdate
