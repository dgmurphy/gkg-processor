from lib.logging import logging
import datetime
import pandas as pd 
from requests import get 
import sys
from lib.mongo import get_database

GKG_MASTER_LISTS_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
ZIPS_QUEUE = "zips_queue"
ZIPS_COMPLETED = "zips_completed"

# DEBUG Only
DEBUG_URLS = ["12178916 6775f42fcc8f080e28d8a58eca86393b http://data.gdeltproject.org/gdeltv2/20150219144500.gkg.csv.zip",
             "9351772 1ec221cd30310f8044d319dc17ab161d http://data.gdeltproject.org/gdeltv2/20150219150000.gkg.csv.zip"]


def fname_from_url(url):

    start_idx = url.rfind('/') + 1
    file_name = url[start_idx: ]
    return file_name


def gkg_getter(db):
    
    startTime = pd.Timestamp('now')
    logging.info("GKG Getter run started at " + str(startTime))

    logging.info("Downloading " + GKG_MASTER_LISTS_URL)
    try:

        # response = get(GKG_MASTER_LISTS_URL)
        # if response.ok is False:
        #     logging.critical(f"Could not download from {GKG_MASTER_LISTS_URL}. Exiting")
        #     sys.exit()

        # zip_urls = response.text.splitlines()
        # logging.info(f"Found {len(zip_urls)} lines in GDELT V2 Master List")
        
        # DEBUG
        zip_urls = DEBUG_URLS
        #

        zip_urls = list(filter(lambda u: "gkg.csv.zip" in u, zip_urls))
        logging.info(f"Found {str(len(zip_urls))} gkg zip urls")

        logging.info(f"Posting to DB...")
        
        count = 0
        for zip_url in zip_urls:

            zurl = zip_url[zip_url.index("http"): ].strip()
            # YYYY MM DD HH MM SS
            zipdate = zip_url[zip_url.index("gdeltv2") + 8 : zip_url.index(".gkg.csv.zip")].strip()
            year = zipdate[:4]
            month = zipdate[4:6]
            day = zipdate[6:8]
            hr = zipdate[8:10]
            min = zipdate[10:12]
            sec = zipdate[12:]
            zipdate = datetime.datetime(
                int(year), int(month), int(day), int(hr), int(min), int(sec))
            
            # If this item is not in the DB queue table then add it
            gkg_db_entry = {"url": zurl, "date": zipdate}

            try:
                gkg_db_entry_id = db[ZIPS_QUEUE].insert_one(gkg_db_entry).inserted_id
            
            except Exception as e:
                logging.critical(str(e))
            
            count += 1
            if count % 10000 == 0:
                print(f"posted {str(count)} items")
            

    except Exception as e:
            logging.critical(str(e))
            logging.critical(f"Could not download from {GKG_MASTER_LISTS_URL}. Exiting")
            sys.exit()


def main():

    try:
        logging.info("Connection to MongoDB...")
        db = get_database()

        # Test: add item to gkg_zips_processed collection


        gkg_getter(db)

    except Exception as e:
         logging.critical(str(e))
    

if __name__ == '__main__':
    main()
    print("DONE\n")
 