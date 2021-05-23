def getVarVal(var, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE):
    if var in VAR_CONDITIONALS.keys():
        return VAR_CONDITIONALS[var]
    if ATTRIBUTES_DATATYPE[var] == 'INT':
        return (float('-inf'), float('inf'))
    else:
        return ( '!=', '' )

def precedence(op):
    if op in ('>', '>=', '<', '<=', '=', '!='):
        return 9
    elif op == 'and':
        return 8
    elif op == 'or':
        return 7
    elif op == 'JOIN':
        return 6
    elif op == 'UNION':
        return 5
    return 0

def getIntOpRange(op, val):
    val = int(val)
    if op == '=':
        return (val, val)
    elif op == '!=':
        return (float('-inf'), val-1)
    elif op == '>':
        return (val+1, float('inf'))
    elif op == '>=':
        return (val, float('inf'))
    elif op == '<':
        return (float('-inf'), val-1)
    elif op == '<=':
        return (float('-inf'), val)

def solveVarchar(a, b, op):
    a, b = list(a), list(b)
    a[1], b[1] = a[1].replace("\'", ''), b[1].replace("\"", '')
    a, b = tuple(a), tuple(b)
    if op == '=':
        if a[0] == b[0]:
            if a[1] == b[1]:
                return True
            return False
        else:
            if a[1] != b[1]:
                return True
            return False
    elif op == '!=':
        if a[0] == b[0]:
            if a[1] == b[1]:
                return False
            return True
        else:
            return True

def applyOp(a, b, op, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE):

    # BOOLEAN operator
    if op == 'or':
        return a | b
    elif op == 'and':
        return a & b

    # a or b is a variable
    if a in ATTRIBUTES_DATATYPE.keys() and b in ATTRIBUTES_DATATYPE.keys():
        if ATTRIBUTES_DATATYPE[a] == 'INT' and ATTRIBUTES_DATATYPE[b]=='INT':
            return solveIntOpCond(VAR_CONDITIONALS[a], VAR_CONDITIONALS[b], op)
        else:
            return solveVarchar(a,b,op)
    elif a in ATTRIBUTES_DATATYPE.keys():
        if ATTRIBUTES_DATATYPE[a] == 'INT':
            return solveIntOpCond(VAR_CONDITIONALS[a], b, op)
        else:
            return solveVarchar(VAR_CONDITIONALS[a], b, op)
    elif b in ATTRIBUTES_DATATYPE.keys():
        if ATTRIBUTES_DATATYPE[b] == 'INT':
            return solveIntOpCond(a, VAR_CONDITIONALS[b], op)
        else:
            return solveVarchar(a, VAR_CONDITIONALS[b], op)
    
    # a, b are not variables
    if type(a) == bool or type(b)==bool:
        return solveIntOpCond(a,b,op)
    elif a[0] not in ('=', '!=', '>', '>=', '<', '<='):
        return solveIntOpCond(a,b,op)
    else:
        return solveVarchar(a, b, op)

def solveIntOpCond(a, b, op):
    if op == '=':
        if a[1]<b[0] or b[1]<a[0]:
            return False
        return True
    elif op == '!=':
        if a==b:
            return False
        return True
    elif op == '>':
        if b[0]>=a[1]:
            return False
        return True
    elif op == '>=':
        if b[0]>a[1]:
            return False
        return True
    elif op == '<':
        if a[0]>=b[1]:
            return False
        return True
    elif op == '<=':
        if a[0]>b[1]:
            return False
        return True
    elif op == 'or':
        return a | b
    elif op == 'and':
        return a & b
 
def evaluate(query_where_clause, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE):
    tokens = query_where_clause.split(' ')
    values = []
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
                val2 = values.pop()
                val1 = values.pop()
                op = ops.pop()
                values.append(applyOp(val1, val2, op, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE))
            ops.pop()
        elif tokens[i] in ('=', '!=', '>', '>=', '<', '<=', 'and', 'or'):
            while (len(ops) != 0 and precedence(ops[-1]) >= precedence(tokens[i])):
                val2 = values.pop()
                val1 = values.pop()
                op = ops.pop()
                values.append(applyOp(val1, val2, op, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE))
            ops.append(tokens[i])         
        elif tokens[i] in ATTRIBUTES_DATATYPE.keys():
            values.append(getVarVal(tokens[i], VAR_CONDITIONALS, ATTRIBUTES_DATATYPE))
        else:
            try:
                val = int(tokens[i])
                values.append( (val,val) )
            except:
                val = tokens[i]
                values.append(('=', val))
        i += 1

    while len(ops) != 0:         
        val2 = values.pop()
        val1 = values.pop()
        op = ops.pop()
        values.append(applyOp(val1, val2, op, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE))
    return values[-1]
 
def preParsePredicate(predicate):
    op_replacements = {}
    op_replacements['='] = ' = '
    op_replacements['! ='] = ' != '
    op_replacements['>'] = ' > '
    op_replacements['>  ='] = ' >= '
    op_replacements['<'] = ' < '
    op_replacements['<  ='] = ' <= '
    op_replacements['('] = ' ( '
    op_replacements[')'] = ' ) '
    for op in ('=', '! =', '>', '>  =', '<', '<  =', '(', ')'):
        predicate = predicate.replace(op, op_replacements[op])
    return predicate

def getVarConditionals(predicate):
    VAR_CONDITIONALS = {}
    predicate = preParsePredicate(predicate)
    predicate = predicate.strip(' ').split('and')
    for cond in predicate:
        var, op, val = [ x for x in cond.strip(' ').split(' ') if x!='' ]
        try:
            VAR_CONDITIONALS[var] = getIntOpRange(op, val)
        except:
            VAR_CONDITIONALS[var] = (op, val)
    return VAR_CONDITIONALS

# if __name__ == "__main__":
#     ATTRIBUTES_DATATYPE = getAttributesDatatype()
#     predicate = "Products.categoryID!=1 and Products.listPrice>5000 and Products.productDescription != 'a' "
#     VAR_CONDITIONALS = getVarConditionals(predicate)
#     predicate = preParsePredicate("""(Products.categoryID=1 or Products.listPrice>5000) and Products.categoryID=1 or Products.productDescription!="b" """)

#     print(evaluate(predicate, VAR_CONDITIONALS, ATTRIBUTES_DATATYPE))