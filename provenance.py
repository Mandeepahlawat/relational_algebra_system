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

def process_query_provenance(inputline, cur):
    print ("Processing provenance")
    select_conditions = get_select_conditions(inputline)    
    projections = get_projections(inputline)
    relations = get_relations(inputline)
    new_name = get_new(inputline)

    query=""
    union=False
    for relation in relations:
        if union:
            query+= " UNION "
        query += 'Select {} from {}'.format(projections, relation)
        if select_conditions:
            query += ' where {}'.format(select_conditions)
        union=True

    temp_name="_provenance_step1"
    temp_query='DROP TABLE IF EXISTS {}'.format(temp_name)
    cur.execute(temp_query)
    temp_query='CREATE TABLE {} AS {}'.format(temp_name, query)
    print (temp_query)
    cur.execute(temp_query)
    columns=get_columns(cur, temp_name)
    columns_text=",".join(columns)
    print (columns_text)

    temp_query='DROP TABLE IF EXISTS {}'.format(new_name)
    cur.execute(temp_query)
    temp_query='CREATE TABLE {} AS SELECT {},GROUP_CONCAT(provenance, "+") FROM {} GROUP BY {}'.format(new_name, columns_text, temp_name, columns_text)
    print (temp_query)
    cur.execute(temp_query)
    temp_query='SELECT * FROM {}'.format(new_name)
    cur.execute(temp_query)
    print_results(cur)
