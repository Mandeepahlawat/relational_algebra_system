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
        return project_cols
    else:
        return '*'


def get_new(inputline):
    if re.search(":", inputline, re.IGNORECASE):
        new_name = re.search("^.+:", inputline).group().replace(":", "")
        return new_name
    else:
        return ''


def print_results(cur):
    results = cur.fetchall()
    print("\n\n**************** RESULTS ****************\n\n")
    for result in results:
        print(result)
        print

def isBagColumnPresentIn(name):
    with open(name + ".txt") as f:
        content = f.readlines()
        column_names = content[0].split("\t")
        column_names = [col.strip() for col in column_names]
        column_names = [col for col in column_names if col]
        for col in column_names:
            if col == "bag":
                return True
    return False


def process_query_bag(inputline, cur):
    print("Processing bag")
    select_conditions = get_select_conditions(inputline)
    projections = get_projections(inputline)
    relations = get_relations(inputline)
    new_name = get_new(inputline)

    query = ""
    union = False
    for relation in relations:
        if isBagColumnPresentIn(relation):
            print("Yes")
        if union:
            query += " UNION "
        query += 'Select {} from {}'.format(projections, relation)
        if select_conditions:
            query += ' where {}'.format(select_conditions)
        union = True

    if (new_name):
        temp = 'DROP TABLE IF EXISTS {}'.format(new_name)
        cur.execute(temp)
        query = 'CREATE TABLE {} AS {}'.format(new_name, query)
        cur.execute(query)
        query = 'SELECT * FROM {}'.format(new_name)

    print(query)
    cur.execute(query)
    print_results(cur)
