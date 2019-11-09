import re
import sqlite3

def get_select_conditions(inputline):
    if re.search("select", inputline, re.IGNORECASE):
        selected_conditions = re.search("\[.+\]", inputline).group().replace("[", "").replace("]", "")
        selected_conditions = selected_conditions.split(",")
        return " and".join(selected_conditions)
    else:
        return None

def get_relations(inputline):
    return re.search("\(.+\)", inputline).group().replace("(", "").replace(")", "").split(",")

def get_projections(inputline):
    if re.search("project", inputline, re.IGNORECASE):
        project_cols = re.search("\<.+\>", inputline).group().replace("<", "").replace(">", "")
        if ("provenance" not in project_cols.split(",")):
            project_cols+=", provenance"
        return project_cols
    else:
        return '*'

def get_new(inputline):
    if re.search(":", inputline, re.IGNORECASE):
        new_name = re.search("^.+:", inputline).group().replace(":", "")
        return new_name
    else:
        return '_provenance_temp'
    

def print_results(cur):
    results = cur.fetchall()
    print("\n\n**************** RESULTS ****************\n\n")
    for result in results:
        print(result)
        print

def get_columns(cur, table):
    query='PRAGMA table_info({})'.format(table)
    cur.execute(query)
    results = cur.fetchall()
    columns = []
    for result in results:
        if (result[1]!="provenance"):
            columns.append(result[1])
    return columns

def execute_query(cur, query):
    print (query)
    cur.execute(query)

def process_join (cur, relation):
    relations=re.split(" join ", relation, flags=re.IGNORECASE)
    temp_table="_provenance_step2"
    result_table="_provenance_step3"
    current=relations.pop()
    while (relations):
        next=relations.pop()
        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(temp_table))
        execute_query(cur, 'ALTER TABLE {} RENAME COLUMN provenance TO provenance2'.format(next))
        execute_query(cur, 'CREATE TABLE {} AS SELECT * FROM {} NATURAL JOIN {}'.format(temp_table, current, next))
        execute_query(cur, 'ALTER TABLE {} RENAME COLUMN provenance2 TO provenance'.format(next))
        execute_query(cur, 'UPDATE {} SET provenance="("||provenance||")" WHERE provenance LIKE "%+%"'.format(temp_table))
        execute_query(cur, 'UPDATE {} SET provenance2="("||provenance2||")" WHERE provenance2 LIKE "%+%"'.format(temp_table))
        execute_query(cur, 'UPDATE {} SET provenance2=provenance||"*"||provenance2'.format(temp_table))
        columns=",".join(get_columns(cur, temp_table))
        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(result_table))
        execute_query(cur, 'CREATE TABLE {} AS SELECT {} FROM {}'.format(result_table, columns, temp_table))
        execute_query(cur, 'ALTER TABLE {} RENAME COLUMN provenance2 TO provenance'.format(result_table))
        current=result_table
    return result_table
        
def process_query_provenance(inputline, cur):
    print ("Processing provenance")
    select_conditions = get_select_conditions(inputline)    
    projections = get_projections(inputline)
    new_name = get_new(inputline)
    relations = get_relations(inputline)

    query=""
    union=False
    for relation in relations:
        if union:
            query+= " UNION "
        if (re.search(" join ", relation, re.IGNORECASE)):
            relation=process_join(cur, relation)
        query += 'Select {} from {}'.format(projections, relation)
        if select_conditions:
            query += ' where {}'.format(select_conditions)
        union=True

    temp_name="_provenance_step1"
    execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(temp_name))
    execute_query(cur, 'CREATE TABLE {} AS {}'.format(temp_name, query))
    columns=",".join(get_columns(cur, temp_name))

    execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(new_name))
    execute_query(cur, 'CREATE TABLE {} AS SELECT {},GROUP_CONCAT(provenance, "+") FROM {} GROUP BY {}'.format(new_name, columns, temp_name, columns))
    execute_query(cur, 'SELECT * FROM {}'.format(new_name))
    print_results(cur)
