import csv
import sys
import mysql.connector
import requests

HOSTNAME = "localhost"
USERNAME = ""
PASSWORD = ""
AUTH_PLUGIN = "mysql_native_password"
DB = 'TeamRandom'

SYS_CAT_TABLES = set(['Conditionals','Fragments','FragmentsAttributesList','RelationAttributes','Sites'])
APP_DB_TABLES = set(['Addresses','Categories','Customers','Inventories','Products','Vendors'])
NON_FRAG_REALTIONS = set(['Categories','Products','Inventories','Vendors','Customers','Addresses'])

SITES = {
    '1' : '10.3.5.215:8081',
    '2' : '10.3.5.214:8081',
    '3' : '10.3.5.213:8081'
}

ATTR_NAME_ID_MAP = {
    'Categories' : {
        'categoryID' : 36,
        'categoryName' : 13
    },    
    'Products' : {
        'productID' : 4,
        'productName' : 6,
        'productDescription': 3,
        'standardCost': 2,
        'listPrice': 1,
        'categoryID': 5,
    },    
    'Inventories' : {
        'productID' : 37,
        'vendorID' : 38,
        'quantity': 14,
    },
    'Vendors':{
        'vendorID' : 7,
        'vendorName' : 8,
        'addressID' : 10,
        'rating' : 9,
        'phone' : 11,
        'email' : 12,
    },
    'Addresses':{
        'addressID' : 28,
        'city' : 31,
        'state' : 32,
        'countryName' : 33,
        'regionName' : 34,
        'postalCode' : 35,
    },
    'Customers':{
        'customerID' : 22,
        'customerName' : 15,
        'addressID' : 10,
        'phone' : 11,
        'email' : 12,
    }
}

FRAGMENT_ATTRIBUTES = {
}

FRAGMENTS_SITE = {
}

FRAGMENTS = {
    # 'FRAG_NAME' : ['type', 'reln_name', ['attributes'], 'conditional']
}

DH_FRAGMENTS = {
    # 'FRAG_NAME' : [ 'reln_name', 'right_frag', 'right_frag_attr']
}

FRAG_ID = 0
COND_ID = 0

def createTableQueryGenUtil(reln_name):
    QUERY = " describe {}".format(reln_name)
    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)
    res = cursor.fetchall()
    cursor.close()
    db.close()
    return res

def getRelnData(reln_name):
    QUERY = " select * from {}".format(reln_name)
    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)
    res = cursor.fetchall()
    cursor.close()
    db.close()
    return res

def sendCreateTableQuery(reln_name, site_id):
    URL = 'http://{}/query'.format(SITES[site_id])
    attrs = createTableQueryGenUtil(reln_name)

    QUERY = " CREATE TABLE IF NOT EXISTS {} ( {} );".format(reln_name, ','.join([ '{} {}'.format(x[0], x[1].decode("utf-8").upper()  ) for x in attrs]))
    req_obj = {'query': QUERY}
    query_response = requests.post(URL, json = req_obj)

    QUERY = " INSERT INTO {} VALUES {};".format(reln_name, ','.join( [ str(row) for row in getRelnData(reln_name) ]) )
    req_obj = {'query': QUERY}
    query_response = requests.post(URL, json = req_obj)

def executeQuery(frag_name, select_clause, reln_name, where_clause="1=1"):
    global FRAG_ID, COND_ID
    QUERY = " CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {} WHERE {};".format(frag_name, select_clause, reln_name, where_clause)
    print(QUERY)
    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)
    res = cursor.fetchall()

    FRAG_ID+=1

    # HORIZONTAL FRAGMENTATION
    if where_clause != "1=1":
        QUERY = " INSERT INTO Fragments (fragment_id, relation_name, fragment_type, site_id, table_name, selectivity) VALUES ({}, '{}', '{}', {}, '{}', {});".format(FRAG_ID, reln_name, 'HF', FRAGMENTS_SITE[frag_name.strip(' ')], frag_name.strip(' '), 0.5)
        print(QUERY)
        cursor.execute(QUERY)
        COND_ID+=1
        QUERY = " INSERT INTO Conditionals (conditional_id, fragment_id, predicate) VALUES ({}, {}, '{}');".format(COND_ID, FRAG_ID, where_clause)
        print(QUERY)
        cursor.execute(QUERY)
        for attr, attr_id in ATTR_NAME_ID_MAP[reln_name].items():
            QUERY = " INSERT INTO FragmentsAttributesList (attribute_id, fragment_id) VALUES ({}, {});".format(attr_id, FRAG_ID)
            print(QUERY)
            cursor.execute(QUERY)
    # VERTICAL FRAGMENTATION
    else:
        QUERY = " INSERT INTO Fragments (fragment_id, relation_name, fragment_type, site_id, table_name, selectivity) VALUES ({}, '{}', '{}', {}, '{}', {});".format(FRAG_ID, reln_name, 'VF', FRAGMENTS_SITE[frag_name.strip(' ')], frag_name.strip(' '), 0.5)
        print(QUERY)
        cursor.execute(QUERY)
        for attr in select_clause.split(','):
            QUERY = " INSERT INTO FragmentsAttributesList (attribute_id, fragment_id) VALUES ({}, {});".format(ATTR_NAME_ID_MAP[reln_name][attr.strip(' ')], FRAG_ID)
            print(QUERY)
            cursor.execute(QUERY) 

    cursor.close()
    db.commit()
    db.close()

def executeJoinQuery(frag_name, attrs, reln_name1, reln_name2, left_join_attr, right_join_attr):
    global FRAG_ID, COND_ID
    QUERY = " CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {},{} WHERE {}.{}={}.{};".format(frag_name, attrs, reln_name1, reln_name2, reln_name1, left_join_attr, reln_name2, right_join_attr)
    print(QUERY)
    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)

    FRAG_ID+=1
    COND_ID+=1
    QUERY = " INSERT INTO Fragments (fragment_id, relation_name, fragment_type, site_id, table_name, selectivity) VALUES ({}, '{}', '{}', {}, '{}', {});".format(FRAG_ID, reln_name1, 'DH', FRAGMENTS_SITE[frag_name.strip(' ')], frag_name.strip(' '), 0.5)
    print(QUERY)
    cursor.execute(QUERY)
    QUERY = " INSERT INTO Conditionals (conditional_id, fragment_id, predicate) VALUES ({}, {}, '{}');".format(COND_ID, FRAG_ID, FRAGMENTS[reln_name2][2])
    print(QUERY)
    cursor.execute(QUERY)
    for attr, attr_id in ATTR_NAME_ID_MAP[reln_name1].items():
        QUERY = " INSERT INTO FragmentsAttributesList (attribute_id, fragment_id) VALUES ({}, {});".format(attr_id, FRAG_ID)
        print(QUERY)
        cursor.execute(QUERY)

    cursor.close()
    db.commit()
    db.close()

def clearAppDB():
    global APP_DB_TABLES, SYS_CAT_TABLES
    PR_TABLES = APP_DB_TABLES.union(SYS_CAT_TABLES)

    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()

    QUERY = "show tables"
    cursor.execute(QUERY)
    tables = set([ table[0] for table in cursor.fetchall() ])

    for table in tables-PR_TABLES:
        QUERY = "DROP TABLE {}".format(table)
        cursor.execute(QUERY)

    for table in ('Conditionals', 'Fragments', 'FragmentsAttributesList'):
        QUERY = "DELETE FROM {}".format(table)
        cursor.execute(QUERY)

    cursor.close()
    db.commit()
    db.close()

def sendClearDBRequest():
    for site_id in ('2', '3'):
        QUERY = "DROP DATABASE TeamRandom; CREATE DATABASE TeamRandom;"
        req_obj = {'query': QUERY}
        query_response = requests.post('http://{}/query'.format(SITES[site_id]), json = req_obj)

def insertNoFragSysCatData(reln_name, site):
    global FRAG_ID
    FRAG_ID+=1
    db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    QUERY = " INSERT INTO Fragments VALUES ({}, '{}', '{}', {}, '{}', {});".format(FRAG_ID, reln_name, 'NF', site, reln_name.strip(' '), 0.5)
    print(QUERY)
    cursor.execute(QUERY)
    for attr, attr_id in ATTR_NAME_ID_MAP[reln_name].items():
        QUERY = " INSERT INTO FragmentsAttributesList VALUES ({}, {});".format(attr_id, FRAG_ID)
        print(QUERY)
        cursor.execute(QUERY) 
    cursor.close()
    db.commit()
    db.close()

# Conditionals, Fragments, FragmentsAttributesList
if __name__ == '__main__':
    clearAppDB()
    sendClearDBRequest()
    with open(sys.argv[1]) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                reln_name = row[0]
                frag_name = row[1]
                frag_type = row[2].strip("\'\" ")

                if frag_type == 'V':
                    NON_FRAG_REALTIONS.discard(reln_name)
                    attrs = ''.join(list(row[3:])).strip("\'\[\]\" ")
                    attrs = ','.join([ x.strip("\"\' ") for x in attrs.split(' ') if x!='' ])
                    FRAGMENTS[frag_name] = ['V', reln_name, attrs ]
                elif frag_type == 'H':
                    NON_FRAG_REALTIONS.discard(reln_name)
                    frag_conditional = ''.join(list(row[3:])).strip("\'\[\]\" ")
                    FRAGMENTS[frag_name] = ['H', reln_name, frag_conditional ]
                elif frag_type == 'DH':
                    NON_FRAG_REALTIONS.discard(reln_name)
                    frag_cond = ''.join(list(row[3:])).strip("\'\[\]\" ").split(' ')
                    frag_attr = frag_cond[0]
                    right_frag_attr = frag_cond[1]
                    right_frag = frag_cond[2]
                    DH_FRAGMENTS[frag_name] = [reln_name, frag_attr, right_frag_attr, right_frag]
                elif row[0] != '' and frag_type == '' and row[0]!='Fragment':
                    FRAGMENTS_SITE[row[0].strip("\ '")] = row[1].strip("\' ")
            line_count += 1

    for frag, details in FRAGMENTS.items():
        if details[0] == 'V':
            executeQuery(frag, details[2], details[1])
        else:
            executeQuery(frag, '*', details[1], details[2])


    for frag, cond in DH_FRAGMENTS.items():
        reln_name1 = cond[0]
        reln_name2 = cond[3]
        left_join_attr = cond[1]
        right_join_attr = cond[2]
        attrs = ','.join([ '{}.{}'.format(reln_name1, attr) for attr in list(ATTR_NAME_ID_MAP[cond[0]].keys())])
        executeJoinQuery(frag, attrs, reln_name1, reln_name2, left_join_attr, right_join_attr)
    
    for reln_name in NON_FRAG_REALTIONS:
        insertNoFragSysCatData(reln_name, FRAGMENTS_SITE[reln_name])

    for frag, site in FRAGMENTS_SITE.items():
        if site!='1':
            print(site)
            sendCreateTableQuery(frag, site)
            # DROP TABLE
            db = mysql.connector.connect(host = HOSTNAME, user = USERNAME, password = PASSWORD, database=DB)
            cursor = db.cursor()
            QUERY = "DROP TABLE {}".format(frag)
            cursor.execute(QUERY)
            cursor.close()
            db.close()
