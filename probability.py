import re
import sqlite3
from functools import reduce

ANNOTATED_COLUMN_NAME = 'provenance'
TEMP_TABLE_NAME_1 = 'temp_prob_table_1'
TEMP_TABLE_NAME_2 = 'temp_prob_table_2'
TEMP_RESULT_TABLE_NAME_1 = 'temp_prob_result_table_1'
TEMP_RESULT_TABLE_NAME_2 = 'temp_prob_result_table_2'

def get_select_conditions(inputline):
    if re.search("select", inputline, re.IGNORECASE):
        selected_conditions = re.search("\[.+\]", inputline).group().replace("[", "").replace("]", "")
        selected_conditions = selected_conditions.split(",")
        return " and".join(selected_conditions)
    else:
        return None

def is_join_present(inputline):
    join_present = re.search("\(.+join.+\)", inputline)
    if join_present:
        return True

def get_relations(inputline):
    return re.search("\(.+\)", inputline).group().replace("(", "").replace(")", "")

def get_projections(inputline):
    if re.search("project", inputline, re.IGNORECASE):
        project_cols = re.search("\<.+\>", inputline).group().replace("<", "").replace(">", "")
        project_cols += ', {}'.format(ANNOTATED_COLUMN_NAME)
        return project_cols
    else:
        return '*'

def add_probabilities(results):
    final_results = {}
    for result in results:
        key = result[0:-1]
        val = result[-1]
        final_results[key] = final_results.get(key, []) + [val]

    for key, val in final_results.items():
        temp = [(1 - float(item)) for item in final_results.get(key)]
        temp = reduce((lambda x, y : x * y), temp)
        final_results[key] = 1 - temp

    final_results_list = []
    for key, val in final_results.items():
        final_results_list.append(tuple([i for i in key] + [val]))

    return final_results_list

def multiply_probabilities(results):
    final_results = {}
    for result in results:
        key = result[0:-1]
        val = result[-1]
        final_results[key] = final_results.get(key, []) + [val]

    for key, val in final_results.items():
        temp = reduce((lambda x, y : x * y), temp)
        final_results[key] = 1 - temp

    return list(final_results.items())

def get_all_values_from_tuple_index(prob_indices, join_result):
    result = 1
    for i in prob_indices:
        result *= float(join_result[i])
    return result

def delete_values_from_tuple_index_and_add_new(prob_indices, join_result, new_value):
    result = join_result
    for i, val in enumerate(prob_indices):
        index = val - i
        result = result[:index] + result[index+1:]
    return result + (str(new_value),)

def get_join_and_columns_from_relation_dict(relation_dict):
    join_columns = []
    common_columns = []
    for key, values in relation_dict.items():
        for val in values:
            if val != ANNOTATED_COLUMN_NAME:
                if val in join_columns:
                    join_columns.append("'{}.{}'".format(key,val))
                    common_columns.append(val)
                else:
                    join_columns.append(val)
    return (common_columns, join_columns)

def create_temp_join_table(cur, inputline, relation_dict):
    cur.execute("select * from {}".format(get_relations(inputline)))

    join_results = cur.fetchall()
    join_columns = [col[0] for col in cur.description]
    prob_indices = [i for i,x in enumerate(join_columns) if x == ANNOTATED_COLUMN_NAME]

    # calculate new value for probability
    probability_list = [get_all_values_from_tuple_index(prob_indices, join_result) for join_result in join_results]
    # delete existing probability values and append the new value in the end
    new_join_results = [delete_values_from_tuple_index_and_add_new(prob_indices, join_result, probability_list[i]) for i, join_result in enumerate(join_results)]

    #update join_columns to append table_name in front of columns
    common_columns, join_columns = get_join_and_columns_from_relation_dict(relation_dict)

    join_columns.append(ANNOTATED_COLUMN_NAME)

    temp_table_name = None
    temp_other_table_name = None
    if is_temp_table_empty(TEMP_TABLE_NAME_1, cur):
        temp_table_name = TEMP_TABLE_NAME_1
        temp_other_table_name = TEMP_TABLE_NAME_2
    else:
        temp_table_name = TEMP_TABLE_NAME_2
        temp_other_table_name = TEMP_TABLE_NAME_1
           
    cur.execute ("CREATE TABLE {} ({})".format(temp_table_name, " text, ".join(join_columns) + ' text'))

    for result in new_join_results:
        print(result)
        cur.execute("INSERT INTO {} values {}".format(temp_table_name, result))

    #perform natural join
    cur.execute("PRAGMA table_info({})".format(temp_table_name))
    temp_col_list = [col[1] for col in cur.fetchall()]
    
    select_condition_list = []
    for i, col in enumerate(temp_col_list):
        com_cols = [col for col in temp_col_list[i+1:] if col.endswith('.{}'.format(temp_col_list[i]))]
        for com_col in com_cols:
            select_condition = ""
            select_condition += "{}=[{}]".format(temp_col_list[i], com_col)
            select_condition_list.append(select_condition)

    select_condition = " and ".join(select_condition_list)
    #create new temp table after natural join and drop the other one
    cur.execute("create table {} as select * from {} where {}".format(temp_other_table_name, temp_table_name, select_condition))
    drop_temp_table_by_name(temp_table_name, cur)

def get_all_table_columns(inputline, cur):
    relation_dict = {}
    relation_list = get_relations(inputline).split('join')
    for relation in relation_list:
        cur.execute("PRAGMA table_info({})".format(relation))
        column_names = [ col[1] for col in cur.fetchall()]
        relation_dict[relation.strip()] = column_names
    return relation_dict

def print_results(cur, operation=None):
    results = cur.fetchall()
    if operation is not None:
        results = operation(results)
    print("\n\n**************** RESULTS ****************\n\n")

    if is_temp_table_empty(TEMP_RESULT_TABLE_NAME_1, cur):
        final_result_table_name = TEMP_RESULT_TABLE_NAME_2
    else:
        final_result_table_name = TEMP_RESULT_TABLE_NAME_1

    cur.execute("PRAGMA table_info({})".format(final_result_table_name))
    column_names = [col[1] for col in cur.fetchall()]

    print(tuple(column_names))
    for result in results:
        print(result)
        print


def create_temp_result_table(cur, results, result_table_name, projections):
    if projections != '*':
        # join probabilities before inserting them in temp result table
        results = add_probabilities(results)
        column_names = [col.strip() for col in projections.split(',')]
    else:
        if is_temp_table_empty(TEMP_TABLE_NAME_1, cur):
            cur.execute("PRAGMA table_info({})".format(TEMP_TABLE_NAME_2))
            column_names = [col[1] for col in cur.fetchall()]
        else:
            cur.execute("PRAGMA table_info({})".format(TEMP_TABLE_NAME_1))
            column_names = [col[1] for col in cur.fetchall()]
    
    cur.execute ("CREATE TABLE {} ({})".format(result_table_name, " text, ".join(column_names) + ' text'))
    for result in results:
        print(result)
        cur.execute("INSERT INTO {} values {}".format(result_table_name, result))

def is_temp_table_empty(table_name, cur):
    cur.execute ("SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(table_name))
    table_count=cur.fetchone()[0]
    return table_count == 0

def drop_temp_table_by_name(table_name, cur):
    if not is_temp_table_empty(table_name, cur):
        cur.execute("DROP TABLE {}".format(table_name))

def is_union_query(inputline):
    union = re.search("union", inputline, re.IGNORECASE)
    if union:
        return True   

def is_nested_join_query(inputline):
    nested_join = re.search("nestedjoin", inputline, re.IGNORECASE)
    if nested_join:
        return True

def union_queries(cur):
    cur.execute("select * from {} union all select * from {}".format(TEMP_RESULT_TABLE_NAME_1, TEMP_RESULT_TABLE_NAME_2))
    return cur.fetchall()

def get_nested_join_table_names(cur):
    relation_dict = {}
    relation_list = [TEMP_RESULT_TABLE_NAME_1, TEMP_RESULT_TABLE_NAME_2]
    for relation in relation_list:
        cur.execute("PRAGMA table_info({})".format(relation))
        column_names = [ col[1] for col in cur.fetchall()]
        relation_dict[relation.strip()] = column_names
    return relation_dict

def nested_join_queries(cur):
    relation_dict = get_nested_join_table_names(cur)
    query = "({} join {})".format(TEMP_RESULT_TABLE_NAME_1, TEMP_RESULT_TABLE_NAME_2)
    create_temp_join_table(cur, query, relation_dict)

    cur.execute("PRAGMA table_info({})".format(TEMP_RESULT_TABLE_NAME_1))
    result_table_cols = [col[1] for col in cur.fetchall()]

    projection_list = []
    for col in result_table_cols:
        if '.' in col:
            dot_index = col.index('.')
            new_col = col[dot_index+1:].replace("'","")
            projection_list.append(new_col)
        else:
            projection_list.append(col)

    projection_list = ",".join(projection_list)
    
    temp_table_name = None
    if is_temp_table_empty(TEMP_TABLE_NAME_1, cur):
        temp_table_name = TEMP_TABLE_NAME_2
    else:
        temp_table_name = TEMP_TABLE_NAME_1


    query = 'Select {} from {}'.format(projection_list, temp_table_name)

    print("nestedjoin", query)
    cur.execute(query)
    return cur.fetchall()


def process_probability_query(inputline, cur):
    # inputline = "project <code1, code2> select[code1='YUL'] (a)"
    # inputline = "project <code1, code2> select[code1='YUL'] (a join b)"
    # inputline = "project <code1, code2> (a join b)"
    # inputline = "project <code1> select[code1='YUL'] (a join b)"
    # inputline = "project <code1> select[code1='YUL'] (a join b join c)"
    # inputline = "project <code2> (a) union project <code3> (b)"
    # inputline = "project <code1> (a) union project <code1> (b) union project <code1> (c)"
    # inputline = "project <code1> (a) union project <code1> (b) union project <code1> (c) union project <code3> (b)"
    # inputline = "project <code1> (a) nestedjoin project <code1> (b)"
    # inputline = "project <city> select[position='Analyst'](r) nestedjoin project <city> select[position='Field agent' or position='Double agent'](r)"
    inputline = "project <city> select[position='Analyst'](r) union project <city> select[position='Field agent' or position='Double agent'](r)"

    # drop both temp result tables
    drop_temp_table_by_name(TEMP_RESULT_TABLE_NAME_1, cur)
    drop_temp_table_by_name(TEMP_RESULT_TABLE_NAME_2, cur)
    
    query_list = []
    is_union = False
    is_nested_join = False

    if is_union_query(inputline):
        query_list = re.split("union", inputline, flags=re.IGNORECASE)
        is_union = True
    elif is_nested_join_query(inputline):
        query_list = re.split("nestedjoin", inputline, flags=re.IGNORECASE)
        is_nested_join = True
    else:
        query_list.append(inputline)

    for index, query in enumerate(query_list):
        # drop both temp join tables everytime we process a query
        drop_temp_table_by_name(TEMP_TABLE_NAME_1, cur)
        drop_temp_table_by_name(TEMP_TABLE_NAME_2, cur)
        
        results = []
        if is_join_present(query):
            relation_dict = get_all_table_columns(query, cur)
            create_temp_join_table(cur, query, relation_dict)
            select_conditions = get_select_conditions(query)
            projections = get_projections(query)

            temp_table_name = None
            if is_temp_table_empty(TEMP_TABLE_NAME_1, cur):
                temp_table_name = TEMP_TABLE_NAME_2
            else:
                emp_table_name = TEMP_TABLE_NAME_1

            if select_conditions:
                query = 'Select {} from {} where {}'.format(projections, temp_table_name, select_conditions)
            else:
                query = 'Select {} from {}'.format(projections, temp_table_name)

            print(query)
            cur.execute(query)
        else:
            select_conditions = get_select_conditions(query)
            projections = get_projections(query)
            relations = get_relations(query)

            if select_conditions:
                query = 'Select {} from {} where {}'.format(projections, relations, select_conditions)
            else:
                query = 'Select {} from {}'.format(projections, relations, select_conditions)
            
            print(query)
            cur.execute(query)

        results = cur.fetchall()

        if is_temp_table_empty(TEMP_RESULT_TABLE_NAME_1, cur):
            result_table_name = TEMP_RESULT_TABLE_NAME_1
        else:
            result_table_name = TEMP_RESULT_TABLE_NAME_2

        create_temp_result_table(cur, results, result_table_name, projections)
        
        if index != 0:
            print("nested query")
            combined_results = []
            if is_union:
                combined_results = union_queries(cur)
            if is_nested_join:
                drop_temp_table_by_name(TEMP_TABLE_NAME_1, cur)
                drop_temp_table_by_name(TEMP_TABLE_NAME_2, cur)
                combined_results = nested_join_queries(cur)

            # delete one of the temp results table and create a new one consisting union results
            result_table_name = TEMP_RESULT_TABLE_NAME_1
            drop_temp_table_by_name(result_table_name, cur)
            cur.execute("PRAGMA table_info({})".format(TEMP_RESULT_TABLE_NAME_2))
            result_table_projections = ",".join([col[1] for col in cur.fetchall()])
            create_temp_result_table(cur, combined_results, result_table_name, result_table_projections)
            drop_temp_table_by_name(TEMP_RESULT_TABLE_NAME_2, cur)

        
        if index+1 == len(query_list):
            # last query, hence show results
            cur.execute('select * from {}'.format(result_table_name))
            print_results(cur)
