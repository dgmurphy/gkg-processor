# constants.py

import datetime

GKG_START_DATE = datetime.datetime(2022, 1, 1)

AIR_FORCE_PERSONS_ORGS = [
        "united states air",
        "us air force",
        "u s air force",
        "global strike",
        "nc3",
        "nuclear command and control",
        "department of the air",
        "nuclear weapons center",
        "headquarters air force materiel",
        "kirtland air",
        "eglin air",
        "joint base san antonio",
        "ramstein air",
        "robins air",
        "tinker air",
        "wright-patterson air",
        "hill air",
        "warren air",
        "malmstrom air",
        "minot air",
        "vandenberg air",
        "hanscom air",
        "barksdale air",
        "los angeles air"
        ]

GKG_MASTER_LISTS_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

ZIPS_MASTER = "zips_master_queue"

ZIPS_COMPLETED = "zips_completed"

GKG_RECORDS = "gkg_records"

DATA_DIR = "/home/parallels/dev/gkg-processor/data/"

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


SYNSETS = {
        'u s air force': [
                'us air force',
                'united states air force',
                'united states air',

        ],

        'u s air force academy': [
                'united states air force academy',
                'us air force academy'
        ],

        'u s army': [
                'us army',
                'united states army',
        ],

        'u s navy': [
                'us navy',
                'united states navy',
        ],
       
        'u s naval academy': [
                'us naval academy',
                'united states naval academy',
                'naval academy in annapolis'
        ],
       
        'u s marines': [
                'us marines',
                'united states marines',
        ],
       
        'u s coast guard': [
                'us coast guard',
                'united states coast guard',
        ],

        'u s space force': [
                'us space force',
                'united states space force',
                'u s space',
                'us space'
        ],
       
}

ORG_MENTIONS_FILE = "org_mentions.csv"
ALL_ORGS_FILE = "all_orgs.csv"
ALL_ORGS_EDGE_FILE = "all_orgs_edges.csv"
ALL_ORGS_EDGE_IDS_FILE = "all_orgs_edges_ids.csv"
TOP_ORGS_EDGE_IDS_FILE = "top_orgs_edges_ids.csv"
TOP_ORGS_FILE = "top_orgs.csv"
LINK_VALUE_CUTOFF = 25

ORGS_GRAPH_TITLE = "Orgs Graph"
ORGS_GRAPH_FILE = "Orgs-graph.html"