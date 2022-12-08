import pandas as pd
from lib.logging import logging
from lib.mongo import get_database
from lib.constants import *
from itertools import combinations


def build_mentions_file():

    logging.info("Connecting to MongoDB...")
    db = get_database()
    
    logging.info(f"Found {db[GKG_RECORDS].count_documents({})} GKG Records")
    logging.info(f"Building graph...")

    #### Build orgs mentions file 
    with open(ORG_MENTIONS_FILE, 'w') as file:

        for gkg in db[GKG_RECORDS].find():

            # Build org set
            orgset = set()
            if isinstance(gkg['V1ORGANIZATIONS'], str):
                orgs = gkg['V1ORGANIZATIONS'].split(";")
                for org in orgs:
                    org = org.strip().lower()
                    for key in SYNSETS:
                        if org in SYNSETS[key]:
                            org = key
                    orgset.add(org)


            if isinstance(gkg['V2ENHANCEDORGANIZATIONS'], str):
                orgs = gkg['V2ENHANCEDORGANIZATIONS'].split(";")
                for org in orgs:
                    try:
                        org = org[:org.index(",")]
                        org = org.strip().lower()
                        for key in SYNSETS:
                            if org in SYNSETS[key]:
                                org = key
                    except:
                        pass

                    orgset.add(org)


            orgsrow = ""
            for org in orgset:
                orgsrow += org + "; "

            # remove last delim, add carriage return
            file.write(orgsrow.strip()[:-1] + "\n")


def get_orgs_set(orgs_line):

    orgs_line = orgs_line.replace("\n", " ")
    orgs_line = orgs_line.replace(",", " ")
    
    org_set = set()
    l = orgs_line.strip()
    org_list = l.split(";")
    
    for org in org_list:
        org = org.strip()
        if len(org) > 1:
            org_set.add(org)

    return org_set


def make_orgs_pairs(orgs_line):

    org_set = set()

    orgs_line = orgs_line.replace("\n", " ")
    orgs_line = orgs_line.replace(",", " ")

    l = orgs_line.strip()
    org_list = l.split(";")
    
    for org in org_list:

        org = org.strip()
        if len(org) > 1:
            org_set.add(org)

    pairs = []
    oset_tuples = list(combinations(org_set, 2))
    for tup in oset_tuples:
        pair = sorted(tup)  # ensure same order for same pair
        pairs.append(pair)

    return pairs    


def names_to_ids(orgs_dict, row, idx):

    orgs = str(row.name).split(',')

    ids = []
    for org in orgs:
        org = org.strip()
        oid = orgs_dict[org][1]
        ids.append(oid)
    
    return str(ids[idx])


def main():

    #build_mentions_file()

    #### Build node list
    with open(DATA_DIR + ORG_MENTIONS_FILE, 'r') as file:
        logging.info(f"Reading {ORG_MENTIONS_FILE} ...")

        # read the file into temp df
        df = pd.read_csv(file, header=0, sep='\t', names=['ORGS'], 
            index_col=False)

    logging.info("consolidated df shape: " + str(df.shape))

    # create org nodes
    org_rows = df['ORGS'].tolist()
    orgs_dict = {}

    # assign org IDs
    org_id = 0
    org_set = set()
    for line in org_rows:
        org_set = get_orgs_set(line)

        for org in org_set:
            if org in orgs_dict.keys():
                values = orgs_dict[org]
                node_size = values[0]
                orgs_dict[org] = [node_size + 1, values[1]]

            else:
                orgs_dict[org] = [1, org_id]  # node size, node id
                org_id += 1
    
    # convert, sort and write person nodes file
    org_list = []
    for key, value in orgs_dict.items():   
        ol = [value[1], key, value[0]]  # id, label, nodesize
        org_list.append(ol)

    node_list_df = pd.DataFrame(org_list, columns=['id', 'label', 'value'])
    node_list_df = node_list_df.sort_values(by=['value'], ascending=False)
    print(str(node_list_df.head(20)))

    logging.info("writing " + ALL_ORGS_FILE)
    node_list_df.to_csv(DATA_DIR + ALL_ORGS_FILE, header=True, index=False, sep=",")    

    #### Build edge list

    # make orgs column into set
    df['ORGS'] = df.apply(lambda row: make_orgs_pairs(row.ORGS), axis=1)

    logging.info('creating pairs')
    pairs = []
    org_list = df['ORGS'].tolist()
    for row in org_list:
        for pair in row:
            pair_string = str(pair[0]) + ", " + str(pair[1])
            pairs.append(pair_string)

    # make one column with all the pairs and value counts
    logging.info('building edge df')
    pdf = pd.DataFrame()
    pdf["values"] = pairs
    vc = pdf["values"].value_counts()
    edge_df = pd.DataFrame(vc)    

    # Human readable histogram of pairs
    logging.info("writing " + ALL_ORGS_EDGE_FILE)
    edge_df = edge_df.sort_values(by=['values'], ascending=False)
    edge_df.to_csv(DATA_DIR + ALL_ORGS_EDGE_FILE, header=False, index=True, sep=",")

    # Edge list using node ids
    # Add node ids as columns
    edge_df['id1'] = edge_df.apply(lambda row: names_to_ids(orgs_dict, row, 0), axis=1)
    edge_df['id2'] = edge_df.apply(lambda row: names_to_ids(orgs_dict, row, 1), axis=1)
 
    logging.info("writing " + ALL_ORGS_EDGE_IDS_FILE)
    # write ids edge list
    edge_df = edge_df.sort_values(by=['values'], ascending=False)
    edge_df.to_csv(DATA_DIR + ALL_ORGS_EDGE_IDS_FILE, columns=['id1', 'id2', 'values'], header=True, index=False, 
        sep=",")

    logging.info("Filtering edge list for link-strength > " + 
        str(LINK_VALUE_CUTOFF))

    top_edges_df = edge_df[edge_df['values'] >= LINK_VALUE_CUTOFF]
    
    # write the shortend edgelist
    logging.info("writing " + TOP_ORGS_EDGE_IDS_FILE)
    edges_short = top_edges_df.sort_values(by=['values'], ascending=False)
    edges_short.to_csv(DATA_DIR + TOP_ORGS_EDGE_IDS_FILE, header=True, index=False, sep=",",
        columns=['id1', 'id2', 'values'])          

    # keep all the nodes that appear anywhere in the top-n edge list
    keep_orgs = set()
    o1_set = set(edges_short['id1'].apply(str).tolist())
    o2_set = set(edges_short['id2'].apply(str).tolist())
    oset = o1_set.union(o2_set)

    # convert, sort and write shortened person nodes file
    olist = []
    for key, value in orgs_dict.items():    # key is label, value is [nodesize, id]
        if str(value[1]) in oset:
            ol = [value[1], key, value[0]]  # id, label, nodesize
            olist.append(ol)

    node_list_df = pd.DataFrame(olist, columns=['id', 'label', 'value'])
    node_list_df = node_list_df.sort_values(by=['value'], ascending=False)

    logging.info("writing " + TOP_ORGS_FILE)
    node_list_df.to_csv(DATA_DIR + TOP_ORGS_FILE, header=True, index=False, sep=",")    


if __name__ == '__main__':
    main()
    logging.info("DONE\n")