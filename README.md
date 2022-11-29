# GKG Processor
Scripts for downloading and processing GDELT-GKG data.

# Usage
Start docker:

```sudo systemctl start docker```

Start MongoDB:

```
cd mongo
docker compose up
```

Check MongoUI:

http://localhost:8081/

Run the processor:

```
source venv/bin/activate
python gkg-downloader.py
```

## GKG Master List Processor

The script `get_gkg_master_list.py` downloads the GKG master list from the GDELT site, filters out older items and any items that have already been processed, and puts the rest into the zips queue collection in the DB.

If there are no unprocessed GKGs left, it sleeps for a few hours.

New GKG zips are added to the GDELT site every 15 minutes.


## GKG Record Processor

The script `process_gkgs.py` grabs the zip URLs from the zips queue collection and processes them in reverse chron order.

Processing consists of downloading the GKG zip, unzipping,
adding all the GKG records to the GKG records collection, deleting the zip file, and adding the zip url to the processed-zips collection.

If there are no zip urls in the queue it sleeps for a bit then checks again.


