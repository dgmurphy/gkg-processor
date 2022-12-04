from lib.logging import logging

from lib.mongo import get_database
from lib.constants import *

def main():

    orgset = set()
    personset = set()
    themeset = set()

    logging.info("Connecting to MongoDB...")
    db = get_database()
    
    for gkg in db[GKG_RECORDS].find():

        if isinstance(gkg['V1ORGANIZATIONS'], str):
            orgs = gkg['V1ORGANIZATIONS'].split(";")
            for org in orgs:
                org = org.strip()
                orgset.add(org)

        if isinstance(gkg['V2ENHANCEDORGANIZATIONS'], str):
            orgs = gkg['V2ENHANCEDORGANIZATIONS'].split(";")
            for org in orgs:
                try:
                    org = org[:org.index(",")]
                    org = org.strip()
                    orgset.add(org)
                except: 
                    pass

        if isinstance(gkg['V1PERSONS'], str):
            persons = gkg['V1PERSONS'].split(";")
            for person in persons:
                person = person.strip()
                personset.add(person)

        if isinstance(gkg['V2ENHANCEDPERSONS'], str):
            persons = gkg['V2ENHANCEDPERSONS'].split(";")
            for person in persons:
                try:
                    person = person[:person.index(",")]
                    person = person.strip()
                    personset.add(person)
                except:
                    pass

        if isinstance(gkg['V1THEMES'], str):
            themes = gkg['V1THEMES'].split(";")
            for theme in themes:
                theme = theme.strip()
                themeset.add(theme)

        if isinstance(gkg['V2ENHANCEDTHEMES'], str):
            themes = gkg['V2ENHANCEDTHEMES'].split(";")
            for theme in themes:
                try:
                    theme = theme[:theme.index(",")]
                    theme = theme.strip()
                    themeset.add(theme)
                except:
                    pass


    with open('orgs.csv', 'w') as file:
        file.writelines('\n'.join(list(orgset)))

    with open('persons.csv', 'w') as file:
        file.writelines('\n'.join(list(personset)))

    with open('themes.csv', 'w') as file:
        file.writelines('\n'.join(list(themeset)))


if __name__ == '__main__':
    main()
    