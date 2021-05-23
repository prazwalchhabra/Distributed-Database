import itertools
import json
import config
import requests
import re
import solve_predicates
import sys 

from moz_sql_parser import parse
from copy import deepcopy

from utils import *

sys.setrecursionlimit(10**6)

class Query(object):

    def __init__(self, query):

        super().__init__()

        query = initialParsing(query)

        self.initial_query = query
        self.FRAGMENT_PREDICATE_CACHE = {}
        self.KEYWORDS = config.QUERY_KEYWORDS
        self.frag_parents = {}

        self.join_col_pairs = set()
        # Hardcoded (PK, PK) for all vertical fragments 
        self.join_col_pairs.add(('vendorID', 'vendorID'))
        
        self.temp_unioned = set()
        self.updateFragParent()
        self.QUERY_ATTRIBUTES = set()
        self.joins = None

        self.query = self.parseQuery(query)
        self.getAttributes()
        delim = ''
        self.generateJoins()


        print("\nINITIAL JOIN TREE")
        print(self.joins)

        self.query_where_clause = '1=1'
        self.query_grp_by_clause = self.getQueryGroupByClause()
        self.query_having_clause = '1=1'


        if 'where' in self.query.keys():
            self.ATTRIBUTES_DATATYPE = getAttributesDatatype()
            self.query_where_clause = self.getQueryWhereClause(self.initial_query)
            self.query_where_clause = ' '.join(self.query_where_clause.split())
            self.join_col_pairs = self.parseJoinColPairs(self.query_where_clause)
            self.joins = self.horizontalPruning(deepcopy(self.joins), self.query_where_clause)
            print("\nPRUNED JOIN TREE (PREDICATE PRUNING)")
            print(self.joins)

        if  self.query_grp_by_clause != '':
            if self.query_grp_by_clause.lower().find("having")!=-1:
                self.query_grp_by_clause = self.query_grp_by_clause.replace('HAVING', 'having')
                self.query_grp_by_clause, self.query_having_clause = self.query_grp_by_clause.split('having')
            self.query_having_clause = self.replaceAllTableName(self.query_having_clause, ' ')
            self.query_grp_by_clause = self.replaceAllTableName(self.query_grp_by_clause, ' ')

        query_attrs = query.split('from')[0].strip(' ').split('select')[1].split(',')
        attr_num = 1
        self.aggregate_projections = []
        self.final_aggregate_projections = []
        for attr in query_attrs:
            for aggregate_keyword in ("MAX", "MIN", "SUM"):
                if attr.find(aggregate_keyword)!=-1:
                    agg_col = attr.replace(aggregate_keyword,'').replace('(','').replace(')','').strip(' ')
                    if agg_col.find('.')!=-1:
                        attr = attr.replace(agg_col, agg_col.split('.')[1])
                        agg_col = agg_col.split('.')[1]
                    self.final_aggregate_projections.append('{}({})'.format(aggregate_keyword, agg_col) )
                    self.aggregate_projections.append('{} as {}'.format(attr, agg_col) )
                    attr_num+=1
            if attr.find("AVG")!=-1:
                agg_col = attr.replace("AVG(",'').replace(')','').strip(' ')
                if agg_col.find('.')!=-1:
                    attr = attr.replace(agg_col, agg_col.split('.')[1])
                    agg_col = agg_col.split('.')[1]
                self.aggregate_projections.append('SUM({}) as {}'.format(agg_col,agg_col) )
                self.aggregate_projections.append('count(*) as {}'.format("attr_{}".format(attr_num)) )
                self.final_aggregate_projections.append('SUM({})/SUM({})'.format(agg_col, "attr_{}".format(attr_num)) )
                self.query_having_clause.replace(attr.strip(' '), 'SUM({})/SUM({})'.format(agg_col, "attr_{}".format(attr_num)))
                attr_num+=1
        
        print(self.join_col_pairs)

        print("\nQUERY PLAN WITH OPTIMAL JOIN ORDER (HEURISTIC BASED)")
        self.joins = self.rearrangeJoins(self.joins)
        print(self.joins)

        infix_query = self.getInfixExecutionExpression(self.joins)
        
        self.frag_site_map, self.frag_id_name_map = getFragInfo()
        self.frag_attr_names = {}
        for frag_id, frag_name in self.frag_id_name_map.items():
            self.frag_attr_names[frag_name] = list(fetchFragmentAttributeNames(frag_id))

        # Added whitespaces for parsing
        infix_query = " {} ".format(infix_query)
        for frag_id, frag_name in self.frag_id_name_map.items():
            infix_query = infix_query.replace(' {} '.format(frag_id), ' {} '.format(frag_name))
        self.INFIX_QUERY_EXPRESSION = infix_query
        print(self.INFIX_QUERY_EXPRESSION)

    def getQueryGroupByClause(self):
        query = self.initial_query.replace("GROUP BY", "group by")
        if query.find("group by") != -1:
            return "GROUP BY {}".format(query.split('group by')[1])
        return ''

    def updateFragParent(self):
        QUERY = """ select table_name, relation_name from Fragments; """
        response = executeQuery(QUERY)
        for row in response:
            frag, parent = row
            self.frag_parents[frag] = parent
        return

    def getJoinColumns(self, reln1, reln2):
        attrs1 = set(self.frag_attr_names[reln1])
        attrs2 = set(self.frag_attr_names[reln2])

        common_attrs = attrs1.intersection(attrs2)

        join_columns = set()

        for col1 in common_attrs:
            for col2 in common_attrs:
                if (col1,col2) in self.join_col_pairs:
                    join_columns.add( '{}.{}={}.{}'.format(reln1,col1,reln2,col2) )

        return ' and '.join(list(join_columns))

    def parseJoinColPairs(self, query_where_clause):
        self.join_col_pairs = set()
        join_col_pairs = set()
        for op in ('=','!=','>','>=','<','<='):
            query_where_clause = query_where_clause.replace(' {} '.format(op), op)
        where_clause_tokens = query_where_clause.split(' ')
        for ele in where_clause_tokens:
            if re.search('\w\=\w',ele) and ele.count('.')==2:
                lhs, rhs = ele.split('=')
                if lhs.split('.')[0] == rhs.split('.')[0]:
                    continue
                join_col_pairs.add((lhs.split('.')[1],rhs.split('.')[1]))
                join_col_pairs.add((rhs.split('.')[1],lhs.split('.')[1]))
        if len(join_col_pairs) == 0:
            join_col_pairs.add(('vendorID','vendorID'))
        return join_col_pairs

    def getTempTableName(self):
        return 'temp_{}'.format(self.temp_table_num)

    def getFragIDfromName(self, reln_name):
        for frag_id, frag_name in self.frag_id_name_map.items():
            if frag_name == reln_name:
                return frag_id

    def getFragmentCardinality(self, frag_name):
        frag_site = self.frag_site_map[frag_name]
        QUERY = """ {} {} """.format(config.GET_FRAGMENT_CARDINALITY, frag_name)
        response = executeQuery(QUERY, frag_site)
        frag_cardinality = response[0][0]
        return int(frag_cardinality)

    def tranferCost(self, reln, storage_site, target_site):
        return (config.SITE_COST[str(target_site)] + config.NETWORK_TRANSFER_COST['{}_{}'.format(storage_site,target_site)])*self.getFragmentCardinality(reln)

    def getRelnUnion(self, reln1, reln2):
        print("DOING {} UNION {}".format(reln1, reln2))
        attrs1 = self.frag_attr_names[reln1]
        attrs2 = self.frag_attr_names[reln2]
        attrs1 = sorted(attrs1)
        attrs2 = sorted(attrs2)
        temp_table_name = self.getTempTableName()
        # 'select attrs from '
        site_1 = self.frag_site_map[reln1]
        site_2 = self.frag_site_map[reln2]

        union_where_clause = self.replaceAllTableName(self.query_where_clause, ' ')
        _where1 = "{} {}".format(union_where_clause, self.replaceAllTableName(self.query_grp_by_clause,' '))
        _where2 = "{} {}".format(union_where_clause, self.replaceAllTableName(self.query_grp_by_clause,' '))

        if reln1 in self.temp_unioned:
            _where1 = "1=1 {}".format(self.replaceAllTableName(self.query_grp_by_clause,' '))
        if reln2 in self.temp_unioned:
            _where2 = "1=1 {}".format(self.replaceAllTableName(self.query_grp_by_clause, ' '))

        # print(self.temp_unioned, reln1, reln2, _where1, _where2)

        if site_1  == site_2:
            QUERY = """CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {} WHERE {} UNION ALL SELECT {} FROM {} WHERE {};""".format(temp_table_name, ','.join(self.getNonAggregateProjections()+self.aggregate_projections), reln1, _where1, ','.join(self.getNonAggregateProjections()+self.aggregate_projections), reln2, _where2)
            print(QUERY)
            print(executeQuery(QUERY, site_1))
            self.frag_site_map[temp_table_name] = site_1
            print("BOTH ON SAME SITE, {} stored at site {}".format(temp_table_name, site_1))
            self.frag_attr_names[temp_table_name] = self.frag_attr_names[reln1]
        else:
            if self.tranferCost(reln1,site_1,site_2)>self.tranferCost(reln2,site_2,site_1):
                reln1, reln2 = reln2, reln1
                attrs1, attrs2 = attrs2, attrs1
                site_1, site_2 = site_2, site_1
                _where1, _where2 = _where2, _where1
            
            # MOVE RELN_1 TO SITE_2
            moveTable(reln1, site_1, site_2)
            print("MOVING {} to site {}".format(reln1, site_2))
            QUERY = """CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {} WHERE {} UNION ALL SELECT {} FROM {} WHERE {};""".format(temp_table_name, ','.join(self.getNonAggregateProjections()+self.aggregate_projections), reln1, _where1, ','.join(self.getNonAggregateProjections()+self.aggregate_projections), reln2, _where2)
            print(QUERY)
            # QUERY = """CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {} UNION SELECT {} FROM {};""".format(temp_table_name, attrs1, reln1, attrs2, reln2)
            print(executeQuery(QUERY, site_2))
            self.frag_site_map[temp_table_name] = site_2
            print("{} stored at site {}".format(temp_table_name, site_2))
            self.frag_attr_names[temp_table_name] = self.frag_attr_names[reln1]
            # DROP RELN_1 FROM SITE_2
            QUERY = """DROP TABLE {};""".format(reln1)
            executeQuery(QUERY, site_2)

        self.temp_unioned.add(temp_table_name)
        
        if reln1.find("temp")!=-1:
            QUERY = """DROP TABLE {};""".format(reln1)
            executeQuery(QUERY, site_1)
        if reln2.find("temp")!=-1:
            QUERY = """DROP TABLE {};""".format(reln2)
            executeQuery(QUERY, site_2)

    def getRelnJoin(self, reln1, reln2, join_columns="1=1"):
        print("DOING {} JOIN {}".format(reln1, reln2))

        attrs1 = set(self.frag_attr_names[reln1])
        attrs2 = set(self.frag_attr_names[reln2])
        temp_table_name = self.getTempTableName()
        # 'select attrs from '
        site_1 = self.frag_site_map[reln1]
        site_2 = self.frag_site_map[reln2]

        project_attrs = []
        for attr in attrs1:
            if attr in attrs2:
                project_attrs.append('{}.{} as {}'.format(reln1, attr, attr))
            else:
                project_attrs.append(attr)
        for attr in attrs2:
            if attr in attrs1:
                continue
            project_attrs.append(attr)

        print(project_attrs)


        if site_1  == site_2:
            QUERY = " CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {},{} WHERE {};".format(temp_table_name, ','.join(project_attrs), reln1, reln2, join_columns)
            print(QUERY)
            print(executeQuery(QUERY, site_2))
            self.frag_site_map[temp_table_name] = site_2
            print("BOTH ON SAME SITE, {} stored at site {}".format(temp_table_name, site_2))
            self.frag_attr_names[temp_table_name] = list( attrs1.union(attrs2) )
        else:
            if self.tranferCost(reln1,site_1,site_2)>self.tranferCost(reln2,site_2,site_1):
                reln1, reln2 = reln2, reln1
                attrs1, attrs2 = attrs2, attrs1
                site_1, site_2 = site_2, site_1
            
            # MOVE RELN_1 TO SITE_2
            moveTable(reln1, site_1, site_2)
            join_attr = self.getJoinAttr(attrs1, attrs2)
            moveSemiJoinTable(reln1, reln2, site_1, site_2, join_attr)
            print("MOVING {} to site {}".format(reln1, site_2))
            QUERY = " CREATE TABLE IF NOT EXISTS {} SELECT {} FROM {},{} WHERE {};".format(temp_table_name, ','.join(project_attrs), reln1, reln2, join_columns)
            print(QUERY)
            print(executeQuery(QUERY, site_2))
            self.frag_site_map[temp_table_name] = site_2
            print("{} stored at site {}".format(temp_table_name, site_2))
            self.frag_attr_names[temp_table_name] = list( attrs1.union(attrs2) )
            # DROP RELN_1 FROM SITE_2
            QUERY = """DROP TABLE {};""".format(reln1)
            executeQuery(QUERY, site_2)
        
        if reln1.find("temp")!=-1:
            QUERY = """DROP TABLE {};""".format(reln1)
            executeQuery(QUERY, site_1)
        if reln2.find("temp")!=-1:
            QUERY = """DROP TABLE {};""".format(reln2)
            executeQuery(QUERY, site_2)

    def runQuery(self):
        self.temp_table_num = 1
        tokens = self.INFIX_QUERY_EXPRESSION.split(' ')
        relns = []
        ops = []

        i = 0
        while i < len(tokens):
            if tokens[i] == ' ' or tokens[i] == '':
                i += 1
                continue
            elif tokens[i] == '(':
                ops.append(tokens[i])
            elif tokens[i] == ')':         
                while len(ops) != 0 and ops[-1] != '(':
                    reln2 = relns.pop()
                    reln1 = relns.pop()
                    op = ops.pop()

                    # GENERATE CANDIDATE FOR JOIN PREDS BY REPLACING PARENT WITH CHILD FRAG
                    if op == 'UNION':
                        self.getRelnUnion(reln1, reln2)
                    else:
                        join_columns = self.getJoinColumns(reln1,reln2)
                        print(join_columns)
                        self.getRelnJoin(reln1, reln2, join_columns)

                    # APPEND PREDS BY REPLACING reln1, reln2 with temp relation
                    relns.append(self.getTempTableName())
                    self.frag_parents[self.getTempTableName()] = self.getTempTableName()
                    self.temp_table_num+=1
                ops.pop()
            elif tokens[i] in ('JOIN', 'UNION'):
                while len(ops) != 0 and solve_predicates.precedence(ops[-1]) >= solve_predicates.precedence(tokens[i]):
                    reln2 = relns.pop()
                    reln1 = relns.pop()
                    op = ops.pop()

                    if op == 'UNION':
                        self.getRelnUnion(reln1, reln2)
                    else:
                        join_columns = self.getJoinColumns(reln1,reln2)
                        print(join_columns)
                        self.getRelnJoin(reln1, reln2, join_columns)

                    relns.append(self.getTempTableName())
                    self.frag_parents[self.getTempTableName()] = self.getTempTableName()
                    self.temp_table_num+=1
                ops.append(tokens[i])
            else:
                relns.append( tokens[i] )
            i += 1

        while len(ops) != 0:         
            reln2 = relns.pop()
            reln1 = relns.pop()
            op = ops.pop()
            if op == 'UNION':
                self.getRelnUnion(reln1, reln2)
            else:
                join_columns = self.getJoinColumns(reln1,reln2)
                print(join_columns)
                self.getRelnJoin(reln1, reln2, join_columns)
            relns.append(self.getTempTableName())
            self.frag_parents[self.getTempTableName()] = self.getTempTableName()
            self.temp_table_num+=1

        print(relns[-1])
        project_attribute_list = self.replaceAllTableName(','.join(self.getFinalNonAggregateProjections()+self.final_aggregate_projections), ',')

        final_query_where_clause = "1=1 {}".format(self.query_grp_by_clause)
        if relns[-1] not in self.temp_unioned:
            final_query_where_clause = self.replaceAllTableName(self.query_where_clause, ' ')
        QUERY = "select {} from {} where {}".format(project_attribute_list, relns[-1], final_query_where_clause)

        if self.initial_query.lower().find("having")!=-1:
            QUERY = "{} HAVING {}".format(QUERY, self.query_having_clause)

        res = executeQuery(QUERY , self.frag_site_map[relns[-1]])

        if relns[-1].find("temp")!=-1:
            QUERY = """DROP TABLE {};""".format(relns[-1])
            executeQuery(QUERY, self.frag_site_map[relns[-1]])
        return res

    def getNonAggregateProjections(self):
        projections = self.initial_query.split(' from ')[0].split('select')[1].split(',')
        non_agg_proj = []
        for col in projections:
            flag = True
            for aggregate_keyword in ("MAX", "MIN", "SUM", "AVG"):
                if col.find(aggregate_keyword)!=-1:
                    flag = False
                    break
            if flag:
                if col.find(".")!=-1:
                    non_agg_proj.append(col.split('.')[1].strip(' '))
                else:
                    non_agg_proj.append(col.strip(' '))
        grp_by_cls = self.query_grp_by_clause.replace("group by",'')
        grp_by_cls = self.query_grp_by_clause.replace("GROUP BY",'')
        for attr in grp_by_cls.split(','):
            if attr == '':
                continue
            non_agg_proj.append(attr.strip(' '))
        non_agg_proj = sorted(list(set(non_agg_proj)))
        return non_agg_proj

    def getFinalNonAggregateProjections(self):
        projections = self.initial_query.split(' from ')[0].split('select')[1].split(',')
        non_agg_proj = []
        for col in projections:
            flag = True
            for aggregate_keyword in ("MAX", "MIN", "SUM", "AVG"):
                if col.find(aggregate_keyword)!=-1:
                    flag = False
                    break
            if flag:
                if col.find(".")!=-1:
                    non_agg_proj.append(col.split('.')[1].strip(' '))
                else:
                    non_agg_proj.append(col.strip(' '))
        non_agg_proj = sorted(list(set(non_agg_proj)))
        return non_agg_proj
            
    def perform_aggregate(self, query):
        res = executeQuery(query)
        return res

    def replaceAllTableName(self, s, delim):
        s = ' '.join(s.split())
        s = s.split(delim)
        _s = []
        for x in s:
            if x.find('.') != -1:
                agg_attr = False
                for aggregate_keyword in ("MAX", "MIN", "SUM", "AVG"):
                    if x.find(aggregate_keyword)!=-1:
                        a,b = x.split('(')
                        b = b.split(')')[0]
                        x = "{}({})".format(a,b.split('.')[1])
                        agg_attr = True
                        break
                if agg_attr == False:
                    x = x.split('.')[1]
            _s.append(x)
        s = '{}'.format(delim).join(_s)
        return s

    def getInfixExecutionExpression(self, join):
        if type(join) == str:
            return join
        elif type(join) == list:
            if type(join[0]) != list and join[0] in ("UNION", "JOIN", ''):
                try:
                    INFIX_EXPR = ' ( ' + ' {} '.format(join[0]).join( self.getInfixExecutionExpression(join[1]) ) + ' ) '
                    return INFIX_EXPR
                except:
                    print(join)
            elif type(join[0]) == list:
                inter_join_exp = []
                for int_join in join:
                    inter_join_exp.append(self.getInfixExecutionExpression(int_join))
                return inter_join_exp
            else:
                return join

    def joinCost(self, join_order):
        """
        Join Cost = Data Transfer Cost
        Assumption Selectivity Factors = 0.5
        Cardinality of Join = Size of relation with join attribute as foreign key
        Result Site Of Join Order: Site of 1st relation in Join Order; since
        we check all possible permutations, each case is handled
        """
        total_cost = 0
        attrs = fetchFragmentAttributeNames(join_order[0])
        result_site = getFragSite(join_order[0])
        joins_size = getFragmentCardinality(join_order[0])

        for i, frag in enumerate(join_order[1:]):

            frag_attrs = fetchFragmentAttributeNames(frag)
            join_attr = self.getJoinAttr(frag_attrs, attrs)
            
            if join_attr is None:
                return float('inf')

            if getFragSite(frag) == result_site:
                if join_attr in getFragForKey(frag):
                    joins_size = getFragmentCardinality(frag)
            else:
                cost = self.getAttributeSize(join_attr) * joins_size
                benefit = (1-getSelectivityFactor(frag))*self.getAttributeSize(join_attr)*getFragmentCardinality(frag)
                total_cost += cost - benefit
                attrs = attrs.union(frag_attrs)
        return total_cost

    def getAttributeSize(self, attribute):
        if self.ATTRIBUTES_DATATYPE[attribute] == 'INT':
            return 8
        return 255

    def getOptimalJoinOrder(self, join_query):
        # single table
        if join_query.find('JOIN')==-1:
            return join_query

        frags = [ frag.strip() for frag in join_query.split('JOIN') ]
        opt_join_perm = frags
        cost = float('inf')
        for join_order in itertools.permutations(frags):
            join_order_cost = self.joinCost(join_order)
            if join_order_cost < cost:
                opt_join_perm = join_order
                cost = join_order_cost
        opt_join_order = ' JOIN '.join(opt_join_perm)
        return opt_join_order
    
    def rearrangeJoins(self, join):
        if type(join) == str:
            return join
        elif type(join) == list:
            if self.queryTreeParseCheck(join):
                intermed_joins = []
                for int_join in join:
                    intermed_joins.append(self.getOptimalJoinOrder(int_join))
                return intermed_joins
            else:
                join = [ self.rearrangeJoins(ele) for ele in join ]
                return join

    def getQueryWhereClause(self, query):
        query = query.replace('GROUP BY', 'group by')
        query_where_clause = query.split('where')[1].split('group by')[0]    
        return solve_predicates.preParsePredicate(query_where_clause)

    def generateJoins(self):
        if type(self.query["from"]) is not list:
            self.query["from"] = [self.query["from"]]

        delim = ''
        
        for table in self.query["from"]:
            fragments = fetchFragments(table)

            frag_type = getFragmentType(fragments[0])
            if  frag_type == 'VF':
                delim = 'JOIN'
                fragments = self.verticalFragmentPruning(fragments)
            elif frag_type != 'NF':
                delim = 'UNION'
            
            if self.joins is None:
                self.joins = [delim, fragments]
            else:
                joins = []
                for frag in fragments:
                    joins.append( self.appendJoinToPrevJoin(deepcopy(self.joins), frag) )

                self.joins = [delim, joins]
        return

    def queryTreeParseCheck(self, join):
        if type(join[0]) != list and join[0] not in ("UNION", "NATURAL_JOIN", "JOIN", ''):
            return True
        return False

    def appendJoinToPrevJoin(self, join, frag):
        if type(join) == str:
            return join
        elif type(join) == list:
            if self.queryTreeParseCheck(join):
                intermed_joins = []
                for int_join in join:
                    intermed_joins.append(int_join+' JOIN '+frag)
                return intermed_joins
            else:
                join = [ self.appendJoinToPrevJoin(ele, frag) for ele in join ]
                return join

    def parseQuery(self, inputQuery):
        tokenizedQuery = json.loads( json.dumps( parse(inputQuery) ) )
        print("TOKENIZED_QUERY -----> ",tokenizedQuery)
        return tokenizedQuery

    def isAttribute(self, obj):
        if type(obj) is str and obj not in self.KEYWORDS:
            return True
        return False

    def getAttributes(self):
        obj = self.query
        for key, value in obj.items():
            if key.lower() != "from":
                self.getAttributesUtil(value)

    def getAttributesUtil(self, obj):
        if type(obj) is dict:
            for key, value in obj.items():
                if type(value) is dict:
                    self.getAttributesUtil(value) 
                elif type(value) is list:
                    for obj in value:
                        self.getAttributesUtil(obj)
                else:
                    self.getAttributesUtil(key)
                    self.getAttributesUtil(value)
        else:
            if self.isAttribute(obj):
                if obj.find('.')!=-1:
                    # relation.attr
                    self.QUERY_ATTRIBUTES.add(obj.split('.')[1])
                else:
                    self.QUERY_ATTRIBUTES.add(obj)

    def getFragmentPredicate(self, fragment_id):
        if fragment_id in self.FRAGMENT_PREDICATE_CACHE.keys():
            return self.FRAGMENT_PREDICATE_CACHE[fragment_id]
        predicate = executeQuery(config.COND_QUERY.format(fragment_id))
        if len(predicate) == 0:
            return None
        predicate = solve_predicates.preParsePredicate(predicate[0][0])
        self.FRAGMENT_PREDICATE_CACHE[fragment_id] = predicate
        return predicate


    def verticalFragmentPruning(self, fragments):
        # All Attributes Required, no pruning possible
        if '*' in self.QUERY_ATTRIBUTES:
            return fragments

        pruned_fragments = set()

        for frag in fragments:
            frag_attrs = fetchFragmentAttributeNames(frag)
            if self.QUERY_ATTRIBUTES.isdisjoint(frag_attrs) is False:
                pruned_fragments.add(frag)
        return list(pruned_fragments)

    def getJoinAttr(self, ATTRS_1, ATTRS_2):
        join_attr = ATTRS_1.intersection(ATTRS_2)
        if join_attr.__len__()==0:
            return None
        return list(join_attr)[0]
    
    def preParseFragConditional(self, conditional):
        conditional = conditional.replace(' or ', ' OR ').split(' OR ')
        return conditional

    def horizontalPruning(self, join, query_where_clause):
        if type(join) == str:
            return join
        elif type(join) == list:
            if self.queryTreeParseCheck(join):
                intermediate_joins = []
                for int_join in join:
                    frags = int_join.split('JOIN')
                    conditionals = []
                    for frag in frags:
                        if frag!='':
                            frag_pred = self.getFragmentPredicate(frag)
                            if frag_pred is not None:
                                frag_pred = self.preParseFragConditional(frag_pred)
                                if len(conditionals) == 0:
                                    conditionals = frag_pred
                                else:
                                    int_conditionals = []
                                    for pred in frag_pred:
                                        for cond in conditionals:
                                            int_conditionals.append('{} and {}'.format(pred, cond))
                                    conditionals = int_conditionals
                    
                    if len(conditionals) == 0:
                        intermediate_joins.append(int_join)
                        continue

                    for cond in conditionals:
                        VAR_CONDITIONALS = solve_predicates.getVarConditionals(cond)
                        if solve_predicates.evaluate(query_where_clause, VAR_CONDITIONALS, self.ATTRIBUTES_DATATYPE) == True:
                            intermediate_joins.append(int_join)
                            break
                return intermediate_joins
            else:
                join = [ self.horizontalPruning(ele, query_where_clause) for ele in join ]
                return join

def createTableQueryGenUtil(reln_name):
    QUERY = " describe {}".format(reln_name)
    return executeQuery(QUERY, '1')

def executeUpdateQuery(query):
    RELN_ATTRS = {
        'Categories' : ['categoryID','categoryName'],
        'Products' : ['productID','productName' ,'productDescription','standardCost','listPrice','categoryID'],
        'Inventories' : ['productID','vendorID','quantity'],
        'Vendors':['vendorID','vendorName','addressID' ,'rating','phone','email'],
        'Addresses':['addressID','city','state','countryName','regionName','postalCode'],
        'Customers':['customerID','customerName','addressID','phone','email']
    }

    def createTempTable(table_name, attributes, where_clause):
        QUERY = """SELECT {} FROM {} WHERE {};""".format(','.join(attributes), table_name, where_clause)
        print(QUERY)
        data = Query(QUERY).runQuery()
        if len(data)==0:
            return
        data = ','.join( [ str(tuple(row)) for row in data ])
    
        attrs = createTableQueryGenUtil(table_name)
        print(attrs)

        QUERY = " CREATE TABLE IF NOT EXISTS tmpupd_table ( {} );".format(','.join([ '{} {}'.format(x[0], x[1].upper()  ) for x in attrs]))
        print(QUERY)
        executeQuery(QUERY,'1')

        QUERY = " INSERT INTO tmpupd_table ({}) VALUES {};".format(','.join(attributes), data)
        print(QUERY)
        executeQuery(QUERY,'1')
    
    def updateTempTable(table_name):
        QUERY = query.replace(table_name, 'tmpupd_table')
        print(QUERY)
        executeQuery(QUERY,'1')

    def updateFragmentData(table_name, where_clause):
        fragments = fetchFragments(table_name)
        frag_type = getFragmentType(fragments[0])
        frag_pk = list(getFragPrimKey(fragments[0]))[0]
        print("KEY--->",frag_pk)
        if frag_type == 'VF':
            for frag_id in fragments:
                frag_name = getFragmentTableName(frag_id)
                frag_site = getFragSite(frag_id)
                attributes = fetchFragmentAttributeNames(frag_id)

                QUERY = "SELECT {} FROM tmpupd_table;".format(frag_pk)
                print(QUERY)
                data = executeQuery(QUERY, '1')
                keys = [ str(row[0]) for row in data ]

                QUERY = "DELETE FROM {} where {} IN ({});".format(frag_name, frag_pk, ','.join(keys) )
                executeQuery(QUERY,frag_site)

                QUERY = """SELECT {} FROM tmpupd_table;""".format(','.join(attributes))
                print(QUERY)
                data = executeQuery(QUERY,'1')
                if len(data)==0:
                    continue
                data = ','.join( [ str(tuple(row)) for row in data ])
                print(data)

                QUERY = " INSERT INTO {} ({}) VALUES {};".format(frag_name, ','.join(attributes), data)
                executeQuery(QUERY,frag_site)
                print(QUERY)
            return
        for frag_id in fragments:
            frag_name = getFragmentTableName(frag_id)
            frag_site = getFragSite(frag_id)
            attributes = fetchFragmentAttributeNames(frag_id)
            frag_predicate = executeQuery(config.COND_QUERY.format(frag_id))
            if len(frag_predicate) == 0:
                frag_predicate = [["1=1"]]
            frag_predicate = solve_predicates.preParsePredicate(frag_predicate[0][0])

            QUERY = "DELETE FROM {} where {};".format(frag_name, where_clause)
            executeQuery(QUERY,frag_site)

            QUERY = """SELECT {} FROM tmpupd_table WHERE {};""".format(','.join(attributes), frag_predicate)
            print(QUERY)
            data = executeQuery(QUERY,'1')
            if len(data)==0:
                continue
            data = ','.join( [ str(tuple(row)) for row in data ])

            QUERY = " INSERT INTO {} ({}) VALUES {};".format(frag_name, ','.join(attributes), data)
            executeQuery(QUERY,frag_site)
            print(QUERY)

    def dropTempTable():
        QUERY = "DROP TABLE tmpupd_table;"
        print(QUERY)
        executeQuery(QUERY,'1')

    query = initialParsing(query)
    query = ' '.join(query.split())
    table_name = query.split(' ')[1].strip(' ')
    attributes = sorted(RELN_ATTRS[table_name])
    where_clause = "1=1"
    if query.find('where')!=-1:
        where_clause = query.split('where')[1].strip(' ')

    sites = set([ getFragSite(frag_id) for frag_id in fetchFragments(table_name) ])
    print("PARTICIPANT SITES ",sites)
    for site in sites:
        try:
            URLS = {}
            URLS['1'] = 'http://10.3.5.215:8081/upd_qry'
            URLS['2'] = 'http://10.3.5.214:8081/upd_qry'
            URLS['3'] = 'http://10.3.5.213:8081/upd_qry'
            URL = URLS[str(site)]
            req_obj = {'query': query}
            query_response = requests.post(URL, json = req_obj, timeout=3)
            query_response_object = query_response.json()
            print("SITE {} READY FOR QUERY EXECUTION".format(site))
        except:
            print("ABORTED by SITE ", site)
            return
    
    createTempTable(table_name, attributes, where_clause)
    updateTempTable(table_name)

    for site in sites:
        try:
            URLS = {}
            URLS['1'] = 'http://10.3.5.215:8081/upd_qry'
            URLS['2'] = 'http://10.3.5.214:8081/upd_qry'
            URLS['3'] = 'http://10.3.5.213:8081/upd_qry'
            URL = URLS[str(site)]
            req_obj = {'query': query}
            query_response = requests.post(URL, json = req_obj, timeout=3)
            query_response_object = query_response.json()
            print("SITE {} READY TO COMMIT".format(site))
        except:
            dropTempTable()
            print("ABORTED BY SITE ",site)
            return

    QUERY = "select * from tmpupd_table"
    print(QUERY)
    print(executeQuery(QUERY,'1'))

    updateFragmentData(table_name, where_clause)
    dropTempTable()


query = input()

if query.find('update')!=-1 or query.find("UPDATE")!=-1:
    executeUpdateQuery(query)
else:
    query = Query(query)
    print(query.runQuery())