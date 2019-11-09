import re
import sqlite3
from functools import reduce


def get_select_conditions(inputline):
    if re.search("select", inputline, re.IGNORECASE):
        selected_conditions = re.search("\[.+\]", inputline).group().replace("[", "").replace("]", "")
        selected_conditions = selected_conditions.split(",")
        return " and".join(selected_conditions)
    else:
        return None

def get_relations(inputline):
    return re.search("\(.+\)", inputline).group().replace("(", "").replace(")", "")

def get_projections(inputline):
    if re.search("project", inputline, re.IGNORECASE):
        project_cols = re.search("\<.+\>", inputline).group().replace("<", "").replace(">", "")
        project_cols += ', prob'
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


def print_results(cur, operation):
    results = cur.fetchall()
    results = operation(results)
    print("\n\n**************** RESULTS ****************\n\n")
    for result in results:
        print(result)
        print

def process_probability_query(inputline, cur):
    inputline = "project <code1, code2> (a)"
    select_conditions = get_select_conditions(inputline)
    projections = get_projections(inputline)
    relations = get_relations(inputline)

    query = 'Select {} from {}'.format(projections, relations)
    # new_name = get_new(inputline)

    print(query)
    cur.execute(query)
    print_results(cur, add_probabilities)
