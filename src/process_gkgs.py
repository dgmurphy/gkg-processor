from lib.logging import logging
from requests import get 
import time
from lib.mongo import get_database
from lib.constants import *
from lib.utils import *
import pandas as pd 
import os
import pymongo

def persons_orgs_filter(df, terms):

    # DEBUG
    #data = [['global strike', 'org1'], ['bar', 'nc3']]
    #df = pd.DataFrame(data, columns=['V1PERSONS', 'V1ORGANIZATIONS'])
    
    fdf = df[(df['V1PERSONS'].str.contains('|'.join(terms), case=False)) |
             (df['V1ORGANIZATIONS'].str.contains('|'.join(terms), case=False))]
 
    return fdf


def numdocs(db):
    return db[ZIPS_MASTER].count_documents({})


def download(url):

    file_path = DATA_DIR + fname_from_url(url)

    # open in binary mode
    with open(file_path, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)    


def process_gkgs(db, persons_orgs):
    '''
    Get the most recent GKG URL
    Download, unzip gkg file
    Read GKK records into Mongo
    Delete files
    Add URL to ZIPS_COMPLETED
    '''
    zips_count = 0
    rows_found = 0
    logging.info(f"Found {str(numdocs(db))} gkg zip records.")
    logging.info(f"Processing zips...")

    while True:
        
        if numdocs(db) == 0:
            last_processed = db[ZIPS_COMPLETED].find().sort("date", -1).limit(1)[0]
            logging.info(f"All zips processed. Last one was {str(last_processed['date'])} ")
            logging.info("Waiting for new zip urls...")
            time.sleep(60 * 60) # wait for more urls to be added

        else:    
            latest_gkg = db[ZIPS_MASTER].find().sort("date", -1).limit(1)[0]
            zurl = latest_gkg.get("url")
            file_path = DATA_DIR + fname_from_url(zurl)

            try:
                # Download the zip file
                #logging.info(f"downloading {file_path}...")
                download(zurl)
                zipdate = date_from_url(zurl)

                #requires unicode escape for some files
                #logging.info(f"reading {file_path}...")
                df = pd.read_csv(file_path, compression='zip', header=None, 
                sep='\t', names=GKG_COLUMN_NAMES, encoding= 'unicode_escape',
                on_bad_lines='skip')
                #logging.info("gkg df shape " + str(df.shape))

                # adding column with iso date
                df['ZIPDATE'] = zipdate

                #logging.info("Applying filters...")
                fdf = persons_orgs_filter(df, persons_orgs)
                rows_found += fdf.shape[0]

                # Upsert the GKG records into the GKG Records Collection
                if fdf.shape[0] > 0:
                    fdf_dict = fdf.to_dict("records")
                    for item in fdf_dict:
                        gkgid = item["GKGRECORDID"]
                        db[GKG_RECORDS].update_one({'GKGRECORDID': gkgid}, {"$set": item}, upsert=True)

                # Delete the zip file
                if os.path.isfile(file_path):
                    os.remove(file_path)

                # put this record on the completed collection
                db[ZIPS_COMPLETED].insert_one(latest_gkg)

                # Remove from master list
                db[ZIPS_MASTER].delete_one({"_id": latest_gkg["_id"]})

                zips_count += 1
                if zips_count % 5 == 0:
                    logging.info(f"Collected {str(rows_found)} rows from {str(zips_count)} zips.")
                    
                if zips_count % 100 == 0:
                    logging.info(f"last file was {file_path} ")
                    logging.info(f"{str(numdocs(db))} in queue.")

            except Exception as e:
                logging.error(f"Error processing {zurl}")
                logging.error(str(e))
                # Don't retry this file
                db[ZIPS_COMPLETED].insert_one(latest_gkg)
                db[ZIPS_MASTER].delete_one({"_id": latest_gkg["_id"]})


def main():

    try:
        logging.info("Connecting to MongoDB...")
        db = get_database()
        
        # create momgo unique index
        # note direction is required but unused for single-key indexes
        db[GKG_RECORDS].create_index([('GKGRECORDID', pymongo.DESCENDING)],
            unique=True) 

        process_gkgs(db, AIR_FORCE_PERSONS_ORGS)

    except Exception as e:
         logging.critical(str(e))
         logging.info("Retrying in 5 minutes...")
         time.sleep(300)
         

if __name__ == '__main__':

    while True:
        main()
    
 