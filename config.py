# Database Connection AUTH
HOSTNAME = "localhost"
USERNAME = ""
PASSWORD = ""
AUTH_PLUGIN = "mysql_native_password"
DB = ""

OPERATORS = ["le", "lt", "ge", "gt", "eq"]
QUERY_KEYWORDS = set(["value", "select", "from", "where", "le", "lt", "ge", "gt", "eq", "count", "max", "sum", "avg", "min"])

# Cache
attribute_cache = {}
frag_site_cache = {}
frag_card_cache = {}
frag_pk_cache = {}
frag_fk_cache = {}
frag_sel_cache ={}

# IP Addresses
URLS = {}
URLS['1'] = 'http://10.3.5.215:8081/query'
URLS['2'] = 'http://10.3.5.214:8081/query'
URLS['3'] = 'http://10.3.5.213:8081/query'

MOVE_TABLE_URLS = {}
MOVE_TABLE_URLS['1'] = 'http://10.3.5.215:8081/move_table'
MOVE_TABLE_URLS['2'] = 'http://10.3.5.214:8081/move_table'
MOVE_TABLE_URLS['3'] = 'http://10.3.5.213:8081/move_table'

SJ_URLS = {}
SJ_URLS['1'] = 'http://10.3.5.215:8081/move_sj_table'
SJ_URLS['2'] = 'http://10.3.5.214:8081/move_sj_table'
SJ_URLS['3'] = 'http://10.3.5.213:8081/move_sj_table'


# Cost Metrics
SITE_COST = {}
SITE_COST['1'] = 0
SITE_COST['2'] = 0
SITE_COST['3'] = 0

NETWORK_TRANSFER_COST = {}
NETWORK_TRANSFER_COST['1_2'] = 1
NETWORK_TRANSFER_COST['2_1'] = 1
NETWORK_TRANSFER_COST['1_3'] = 1
NETWORK_TRANSFER_COST['3_1'] = 1
NETWORK_TRANSFER_COST['2_3'] = 1
NETWORK_TRANSFER_COST['3_2'] = 1

# Queries
FETCH_FRAG_ATTRS_NAME_QUERY = "select DISTINCT attribute from FragmentsAttributesList, RelationAttributes where FragmentsAttributesList.attribute_id = RelationAttributes.attribute_id and fragment_id="
GET_FRAG_PK = "select attribute from FragmentsAttributesList, RelationAttributes where FragmentsAttributesList.attribute_id = RelationAttributes.attribute_id and RelationAttributes.attribute_key='PK' and fragment_id="
GET_FRAG_FK = "select attribute from FragmentsAttributesList, RelationAttributes where FragmentsAttributesList.attribute_id = RelationAttributes.attribute_id and RelationAttributes.attribute_key='FK' and fragment_id="
FETCH_FRAG_ATTRS_ID_QUERY = "select attribute_id from FragmentsAttributesList where fragment_id="
FETCH_RELATION_FRAGS = "select fragment_id from Fragments where relation_name="
FETCH_ALL_ATTRS_QUERY = "select Fragments.fragment_id, relation_name, RelationAttributes.attribute_id, attribute, attribute_type from Fragments, FragmentsAttributesList, RelationAttributes where Fragments.fragment_id = FragmentsAttributesList.fragment_id and FragmentsAttributesList.attribute_id = RelationAttributes.attribute_id;"
COND_QUERY = """select predicate from Conditionals where fragment_id = {};"""
FETCH_FRAG_TYPE_QUERY = """ select fragment_type from Fragments where fragment_id = """
FETCH_FRAG_SITE_QUERY = """ select site_id from Fragments where fragment_id = """
GET_FRAGMENT_SELECTIVITY = """ select selectivity from Fragments where fragment_id = """
GET_FRAGMENT_CARDINALITY = """ select count(*) from """
GET_FRAGMENT_TABLENAME = """ select table_name from Fragments where fragment_id = """