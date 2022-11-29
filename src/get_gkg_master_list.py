from lib.logging import logging
import datetime
import pandas as pd 
from requests import get 
import sys
import pymongo
import time
from lib.utils import *
from lib.mongo import get_database
from lib.constants import (GKG_MASTER_LISTS_URL, ZIPS_MASTER, 
    ZIPS_COMPLETED, GKG_START_DATE)




# DEBUG Only
DEBUG_URLS = ["a2178916 6775f42fcc8f080e28d8a58eca86393b http://data.gdeltproject.org/gdeltv2/20150219144500.gkg.csv.zip",
             "9351772 1ec221cd30310f8044d319dc17ab161d http://data.gdeltproject.org/gdeltv2/20220219150000.gkg.csv.zip"]



def get_master_GKGs(db):
    
    startTime = pd.Timestamp('now')
    logging.info("GKG Getter run started at " + str(startTime))
    numdocs = db[ZIPS_MASTER].count_documents({})
    logging.info(f"Zip URLs collection has {str(numdocs)} docs.")

    logging.info("Downloading " + GKG_MASTER_LISTS_URL)
    try:

        response = get(GKG_MASTER_LISTS_URL)
        if response.ok is False:
            logging.critical(f"Could not download from {GKG_MASTER_LISTS_URL}. Exiting")
            sys.exit()

        zip_urls = response.text.splitlines()
        logging.info(f"Found {len(zip_urls)} lines in GDELT V2 Master List")
        
        # DEBUG
        #zip_urls = DEBUG_URLS
        #

        zip_urls = list(filter(lambda u: "gkg.csv.zip" in u, zip_urls))
        logging.info(f"Found {str(len(zip_urls))} gkg zip urls")

        logging.info(f"Posting gkg zip urls to DB...")

        # create momgo unique index
        db[ZIPS_COMPLETED].create_index([('url', pymongo.DESCENDING)],
            unique=True) 

        count = 0
        for zip_url in zip_urls:

            zipdate =  date_from_url(zip_url)
            zurl = zip_url[zip_url.index("http"): ].strip()

            # Use recent dates 
            if zipdate >= GKG_START_DATE:
                
                gkg_db_entry = {"url": zurl, "date": zipdate}

                try:

                    # Add this zip only if it has not already been processed
                    if db[ZIPS_COMPLETED].count_documents(gkg_db_entry, limit = 1) == 0:
                        db[ZIPS_MASTER].update_one({'url': zurl}, {"$set": gkg_db_entry}, upsert=True)
                
                except Exception as e:
                    logging.critical(str(e))
            
                count += 1
                if count % 500 == 0:
                    logging.info(f"Processed {str(count)} items.")
                    logging.info(f"Last one was {str(zipdate)}")
            

    except Exception as e:
            logging.critical(str(e))
            logging.critical(f"Could not download from {GKG_MASTER_LISTS_URL}. Exiting")
            sys.exit()

    newnumdocs = db[ZIPS_MASTER].count_documents({})
    docs_added = newnumdocs - numdocs
    logging.info(f"Added {str(docs_added)} docs.")
    logging.info(f"Zip URLs collection now has {str(newnumdocs)} docs.")


def main():

    try:
        logging.info("Connection to MongoDB...")
        db = get_database()
        coll_names = db.list_collection_names()
        logging.info(f"Existing collections: {str(coll_names)}")

        # For reference
        latest = db[ZIPS_MASTER].find().sort("date", -1).limit(1)
        if len(list(latest.clone())):
            logging.info(f"Latest ZIP found was: {str(latest[0])}")
        else:
            logging.info(f"The zips master queue is currently empty.")

        while True:
            get_master_GKGs(db)
            logging.info("All GKG urls retrieved. Process paused...")
            time.sleep(60 * 120) # pause 2 hours for more urls to be added


    except Exception as e:
         logging.critical(str(e))
    

if __name__ == '__main__':
    main()
    print("DONE\n")
 