from lib.logging import logging

from lib.mongo import get_database
from lib.constants import *

def main():

    orgsdict = {}
    personsdict = {}
    themesdict = {}

    logging.info("Connecting to MongoDB...")
    db = get_database()
    
    logging.info(f"Found {db[GKG_RECORDS].count_documents({})} GKG Records")

    for gkg in db[GKG_RECORDS].find():

        if isinstance(gkg['V1ORGANIZATIONS'], str):
            orgs = gkg['V1ORGANIZATIONS'].split(";")
            for org in orgs:
                org = org.strip().lower()
                for key in SYNSETS:
                    if org in SYNSETS[key]:
                        org = key
                if org in orgsdict:
                    orgsdict[org] += 1
                else:
                    orgsdict[org] = 1


        if isinstance(gkg['V2ENHANCEDORGANIZATIONS'], str):
            v2orgs = gkg['V2ENHANCEDORGANIZATIONS'].split(";")
            for v2org in v2orgs:
                try:
                    v2org = v2org[:v2org.index(",")]
                    v2org = v2org.strip().lower()
                    for key in SYNSETS:
                        if v2org in SYNSETS[key]:
                            v2org = key
                    # Don't count duplicates of V1 Orgs
                    if v2org not in orgs:
                        if v2org in orgsdict:
                            orgsdict[v2org] += 1
                        else:
                            orgsdict[v2org] = 1

                except: 
                    pass


        if isinstance(gkg['V1PERSONS'], str):
            persons = gkg['V1PERSONS'].split(";")
            for person in persons:
                person = person.strip().lower()
                if person in personsdict:
                    personsdict[person] += 1
                else:
                    personsdict[person] = 1

        if isinstance(gkg['V2ENHANCEDPERSONS'], str):
            v2persons = gkg['V2ENHANCEDPERSONS'].split(";")
            for v2person in v2persons:
                try:
                    v2person = v2person[:v2person.index(",")]
                    v2person = v2person.strip().lower()
                    # Don't count duplicates of V1 persons
                    if v2person not in persons:
                        if v2person in personsdict:
                            personsdict[v2person] += 1
                        else:
                            personsdict[v2person] = 1
                except:
                    pass

        if isinstance(gkg['V1THEMES'], str):
            themes = gkg['V1THEMES'].split(";")
            for theme in themes:
                theme = theme.strip()
                if theme in themesdict:
                    themesdict[theme] += 1
                else:
                    themesdict[theme] = 1

        if isinstance(gkg['V2ENHANCEDTHEMES'], str):
            v2themes = gkg['V2ENHANCEDTHEMES'].split(";")
            for v2theme in v2themes:
                try:
                    v2theme = v2theme[:v2theme.index(",")]
                    v2theme = v2theme.strip()
                    # Don't count duplicates of V1 themes
                    if v2theme not in themes:
                        if v2theme in themesdict:
                            themesdict[v2theme] += 1
                        else:
                            themesdict[v2theme] = 1
                except:
                    pass

    logging.info(f"Writing files...")

    with open('orgs.csv', 'w') as file:
        for item in sorted(orgsdict, key=orgsdict.get, reverse=True):
            file.write(f"{orgsdict[item]} {item}\n")
        
    with open('persons.csv', 'w') as file:
        for item in sorted(personsdict, key=personsdict.get, reverse=True):
            file.write(f"{personsdict[item]} {item}\n")

    with open('themes.csv', 'w') as file:
        for item in sorted(themesdict, key=themesdict.get, reverse=True):
            if len(item) > 0:
                file.write(f"{themesdict[item]} {item}\n")
 

if __name__ == '__main__':
    main()
    