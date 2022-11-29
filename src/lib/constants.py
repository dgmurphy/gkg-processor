# constants.py

import datetime

GKG_START_DATE = datetime.datetime(2022, 1, 1)

AIR_FORCE_PERSONS_ORGS = [
        "united states air force",
        "u s air force",
        "global strike",
        "nc3",
        "barksdale air",
        "nuclear command and control"]

GKG_MASTER_LISTS_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

ZIPS_MASTER = "zips_master_queue"

ZIPS_COMPLETED = "zips_completed"

GKG_RECORDS = "gkg_records"

DATA_DIR = "/home/dmurphy/dev/gkg-processor/data/"

# Differs slightly from GKG codebook to avoid dots in key names
GKG_COLUMN_NAMES = ["GKGRECORDID", 
                    "V2DATE", 
                    "V2SOURCECOLLECTIONIDENTIFIER",
                    "V2SOURCECOMMONNAME",
                    "V2DOCUMENTIDENTIFIER",
                    "V1COUNTS",
                    "V2COUNTS",
                    "V1THEMES",
                    "V2ENHANCEDTHEMES",
                    "V1LOCATIONS",
                    "V2ENHANCEDLOCATIONS",
                    "V1PERSONS",
                    "V2ENHANCEDPERSONS",
                    "V1ORGANIZATIONS",
                    "V2ENHANCEDORGANIZATIONS",
                    "V1TONE",
                    "V2ENHANCEDDATES",
                    "V2GCAM",
                    "V2SHARINGIMAGE",
                    "V2RELATEDIMAGES",
                    "V2SOCIALIMAGEEMBEDS",
                    "V21SOCIALVIDEOEMBEDS",
                    "V2QUOTATIONS",
                    "V2ALLNAMES",
                    "V2AMOUNTS",
                    "V2TRANSLATIONINFO",
                    "V2EXTRASXML",
                    ]
