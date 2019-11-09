import re
import sqlite3
from functools import reduce

ANNOTATED_COLUMN_NAME = 'prob'
TEMP_TABLE_NAME = 'temp_prob_table'
# JOIN_SPLIT_TEXT = 'join'

def get_select_conditions(inputline):
    if re.search("select", inputline, re.IGNORECASE):
        selected_conditions = re.search("\[.+\]", inputline).group().replace("[", "").replace("]", "")
        selected_conditions = selected_conditions.split(",")
        return " and".join(selected_conditions)
    else:
        return None

# def update_select_conditions(inputline, relation_dict):
#     if re.search("select", inputline, re.IGNORECASE):
#         selected_conditions = re.search("\[.+\]", inputline).group().replace("[", "").replace("]", "")
#         selected_conditions = selected_conditions.split(",")
#         return " and".join(selected_conditions)
#     else:
#         return None

def is_join_present(inputline):
    join_present = re.search("\(.+join.+\)", inputline)
    if join_present:
        return True
        # join_present.group().replace("(", "").replace(")", "")

def get_relations(inputline):
    return re.search("\(.+\)", inputline).group().replace("(", "").replace(")", "")
    # relations = re.search("\(.+\)", inputline).group().replace("(", "").replace(")", "")
    # if is_join_present(inputline):
    #     relations_list = relations.split(JOIN_SPLIT_TEXT)
    #     return " join ".join(["{} as t{}".format(relation, i) for i, relation in enumerate(relations_list)])
    # else:
    #     return relations


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

    return list(final_results.items())

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

def get_join_columns_from_relation_dict(relation_dict):
    join_columns = []
    for key, values in relation_dict.items():
        for val in values:
            if val != ANNOTATED_COLUMN_NAME:
                if val in join_columns:
                    join_columns.append("'{}.{}'".format(key,val))
                else:
                    join_columns.append(val)
    return join_columns

def create_temp_join_table(cur, inputline, relation_dict):
    name = TEMP_TABLE_NAME
    cur.execute ("SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='{}'".format(name))
    table_count=cur.fetchone()[0]
    #table already exists so delete existing table
    if table_count != 0:
        cur.execute ("DROP TABLE {}".format(name))

    cur.execute("select * from {}".format(get_relations(inputline)))

    join_results = cur.fetchall()
    join_columns = [col[0] for col in cur.description]
    prob_indices = [i for i,x in enumerate(join_columns) if x == ANNOTATED_COLUMN_NAME]

    probability_list = [get_all_values_from_tuple_index(prob_indices, join_result) for join_result in join_results]
    new_join_results = [delete_values_from_tuple_index_and_add_new(prob_indices, join_result, probability_list[i]) for i, join_result in enumerate(join_results)]
    
    # update join_columns, remove prob values
    # join_columns = [x for x in join_columns if x != ANNOTATED_COLUMN_NAME]

    #update join_columns to append table_name in front of columns
    join_columns = get_join_columns_from_relation_dict(relation_dict)
    join_columns.append(ANNOTATED_COLUMN_NAME)

    cur.execute ("CREATE TABLE {} ({})".format(name, " text, ".join(join_columns) + ' text'))

    for result in new_join_results:
        print(result)
        cur.execute("INSERT INTO {} values {}".format(name, result))


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
    for result in results:
        print(result)
        print

def process_probability_query(inputline, cur):
    # "project <code1, code2> select[code1='YUL'] (a)"
    # "project <code1, code2> select[code1='YUL'] (a join b)"
    # "project <code1, code2> (a join b)"
    # "project <code1> select[code1='YUL'] (a join b)"
    inputline = "project <code1> select[code1='YUL'] (a)"

    if is_join_present(inputline):
        relation_dict = get_all_table_columns(inputline, cur)
        create_temp_join_table(cur, inputline, relation_dict)
        select_conditions = get_select_conditions(inputline)
        projections = get_projections(inputline)
        query = 'Select {} from {} where {}'.format(projections, TEMP_TABLE_NAME, select_conditions)

        print(query)
        cur.execute(query)
    else:
        select_conditions = get_select_conditions(inputline)
        projections = get_projections(inputline)
        relations = get_relations(inputline)

        query = 'Select {} from {} where {}'.format(projections, relations, select_conditions)
        print(query)
        cur.execute(query)

    if projections != '*':  
        print_results(cur, add_probabilities)
    else:
        print_results(cur)

