import config
import config
import requests

def executeQuery(query, site_id = '1'):
    URL = config.URLS[str(site_id)]
    req_obj = {'query': query}
    query_response = requests.post(URL, json = req_obj)
    query_response_object = query_response.json()
    return query_response_object['response']

def moveTable(reln_name, site_id, result_site):
    URL = config.MOVE_TABLE_URLS[str(site_id)]
    req_obj = {'table': reln_name, 'result_site':result_site}
    query_response = requests.post(URL, json = req_obj)
    query_response_object = query_response.json()

def moveSemiJoinTable(R, S, site1, site2, join_attr):
    """
        R semi join S, move joinn col of s to r, do join
    """
    URL = config.SJ_URLS[str(site1)]
    req_obj = {'R': R, 'S':S, 'site':site2, 'join_attr':join_attr}
    query_response = requests.post(URL, json = req_obj)
    query_response_object = query_response.json()

    print("MOVED {}_PRIME to site {}".format(R, site2))

def fetchFragments(relation):
    QUERY = """ {} "{}" """.format(config.FETCH_RELATION_FRAGS, relation)
    response = executeQuery(QUERY)
    fragments = set()
    for row in response:
        for frag_id in row:
            fragments.add(str(frag_id))
    return list(fragments)

def getAttributesDatatype():
    ATTRIBUTES_DATATYPE = {}
    query_response = executeQuery(config.FETCH_ALL_ATTRS_QUERY)
    for row in query_response:
        ATTRIBUTES_DATATYPE[row[3].strip(' ')] = row[-1].strip(' ')
        ATTRIBUTES_DATATYPE['{}.{}'.format(row[1].strip(' '),row[3].strip(' '))] = row[-1].strip(' ')
    return ATTRIBUTES_DATATYPE


def fetchFragmentAttributeNames(fragment_id):
    """
    return : fragment attribute names
    """
    if fragment_id in config.attribute_cache.keys():
        return config.attribute_cache[fragment_id]
    QUERY = """ {} {} """.format(config.FETCH_FRAG_ATTRS_NAME_QUERY, fragment_id)
    response = executeQuery(QUERY)
    attributes = set()
    for row in response:
        for attr in row:
            attributes.add(attr)
    config.attribute_cache[fragment_id] = attributes
    return attributes

def getFragmentType(fragment_id):
    QUERY = """ {} {} """.format(config.FETCH_FRAG_TYPE_QUERY, fragment_id)
    response = executeQuery(QUERY)
    frag_type = response[0][0]
    return frag_type

def getFragSite(fragment_id):
    if fragment_id in config.frag_site_cache.keys():
        return config.frag_site_cache[fragment_id]
    QUERY = """ {} {} """.format(config.FETCH_FRAG_SITE_QUERY, fragment_id)
    response = executeQuery(QUERY)
    frag_site = response[0][0]
    config.frag_site_cache[fragment_id] = frag_site
    return frag_site

def getFragPrimKey(fragment_id):
    if fragment_id in config.frag_pk_cache.keys():
        return config.frag_pk_cache[fragment_id]
    QUERY = """ {} {} """.format(config.GET_FRAG_PK, fragment_id)
    response = executeQuery(QUERY)
    frag_pk = set()
    for row in response:
        for attr in row:
            frag_pk.add(attr)
    config.frag_pk_cache[fragment_id] = frag_pk
    return frag_pk

def getFragForKey(fragment_id):
    if fragment_id in config.frag_fk_cache.keys():
        return config.frag_fk_cache[fragment_id]
    QUERY = """ {} {} """.format(config.GET_FRAG_FK, fragment_id)
    response = executeQuery(QUERY)
    frag_fk = set()
    for row in response:
        for attr in row:
            frag_fk.add(attr)
    config.frag_fk_cache[fragment_id] = frag_fk
    return frag_fk

def getFragmentTableName(fragment_id):
    QUERY = """ {} {} """.format(config.GET_FRAGMENT_TABLENAME, fragment_id)
    response = executeQuery(QUERY)
    table_name = response[0][0]
    return table_name

def getFragmentCardinality(fragment_id):
    # ENTER SITE DETAILS HERE
    if fragment_id in config.frag_card_cache.keys():
        return config.frag_card_cache[fragment_id]
    fragment_table_name = getFragmentTableName(fragment_id)
    frag_site = getFragSite(fragment_id)
    QUERY = """ {} {} """.format(config.GET_FRAGMENT_CARDINALITY, fragment_table_name)
    response = executeQuery(QUERY, frag_site)
    frag_cardinality = response[0][0]
    config.frag_card_cache[fragment_id] = int(frag_cardinality)
    return int(frag_cardinality)

def getSelectivityFactor(fragment_id):
    if fragment_id in config.frag_sel_cache.keys():
        return config.frag_sel_cache[fragment_id]
    QUERY = """ {} {} """.format(config.GET_FRAGMENT_SELECTIVITY, fragment_id)
    response = executeQuery(QUERY)
    selec_factor = response[0][0]
    config.frag_sel_cache[fragment_id] = selec_factor
    return selec_factor

def getFragInfo():
    QUERY = """ select fragment_id, site_id, table_name from Fragments"""
    response = executeQuery(QUERY)
    SITE_MAP, ID_NAME_MAP = {}, {}
    for row in response:
        SITE_MAP[row[2]] = row[1]
        ID_NAME_MAP[row[0]] = row[2]
    return SITE_MAP, ID_NAME_MAP

def initialParsing(query):
    query = query.strip(';')
    query = query.replace('SELECT', 'select')
    query = query.replace('FROM', 'from')
    query = query.replace('WHERE', 'where')
    query = query.replace('min', 'MIN')
    query = query.replace('max', 'MAX')
    query = query.replace('avg', 'AVG')
    query = query.replace('sum', 'SUM')
    query = query.replace('UPDATE', 'update')
    for op in ('=','!=','>','>=','<','<='):
        query = query.replace(' {} '.format(op),op)
    return query    
