from lib.logging import logging
import datetime
from requests import get 
import pymongo
import time
from lib.mongo import get_database
from lib.constants import ZIPS_MASTER, ZIPS_COMPLETED

def numdocs(db):
    return db[ZIPS_MASTER].count_documents({})


def process_gkgs(db):
    '''
    Get the most recent GKG URL
    Download, unzip gkg file
    Read GKK records into Mongo
    Delete files
    Add URL to ZIPS_COMPLETED
    '''
    count = 0
    logging.info(f"Found {str(numdocs(db))} gkg zip records.")
    logging.info(f"Processing zips...")

    while True:
        if numdocs(db) == 0:
            logging.info("Waiting for new zip urls...")
            time.sleep(60 * 60) # wait for more urls to be added
        else:    
            latest_gkg = db[ZIPS_MASTER].find().sort("date", -1).limit(1)[0]
            zurl = latest_gkg.get("url")

            # put this record on the completed collection
            db[ZIPS_COMPLETED].insert_one(latest_gkg)

            # Remove from master list
            db[ZIPS_MASTER].delete_one({"_id": latest_gkg["_id"]})

            count += 1
            if count % 1000 == 0:
                logging.info(f"Processed {str(count)} zips.")
                logging.info(f"{str(numdocs(db))} in queue.")


def main():

    try:
        logging.info("Connection to MongoDB...")
        db = get_database()
        process_gkgs(db)

    except Exception as e:
         logging.critical(str(e))
    

if __name__ == '__main__':
    main()
    print("DONE\n")
 