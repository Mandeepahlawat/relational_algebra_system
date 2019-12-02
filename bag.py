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
        if ("bag" not in project_cols.split(",")):
            project_cols += ", bag"
        return project_cols
    else:
        return '*'


def get_new(inputline):
    if re.search(":", inputline, re.IGNORECASE):
        new_name = re.search("^.+:", inputline).group().replace(":", "")
        return new_name
    else:
        return '_bag_temp'


def print_results(cur):
    results = cur.fetchall()
    print("\n\n**************** RESULTS ****************\n\n")
    for result in results:
        print(result)
        print


def get_columns(cur, table):
    query = 'PRAGMA table_info({})'.format(table)
    cur.execute(query)
    results = cur.fetchall()
    columns = []
    for result in results:
        if (result[1] != "bag"):
            columns.append(result[1])
    return columns


def get_columns_omitbags(cur, table):
    query = 'PRAGMA table_info({})'.format(table)
    cur.execute(query)
    results = cur.fetchall()
    columns = []
    for result in results:
        if result[1] != "bag" and result[1] != "bag2":
            columns.append(result[1])
    return columns


def execute_query(cur, query):
#    print(query)
    cur.execute(query)


def process_join(cur, relation):
    relations = re.split(" join ", relation, flags=re.IGNORECASE)
    temp_table = "_bag_step2"
    copy_table = "_copy_table"
    result_table = "_bag_step3"
    current = relations.pop()

    origColumn = get_columns(cur, current)

    columnsOrig = ",".join(origColumn) + ",bag"

    columnsAndInfoBag2 = " TEXT,".join(origColumn) + " TEXT, bag2 TEXT"

    while (relations):
        next = relations.pop()
        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(copy_table))
        execute_query(cur, 'CREATE TABLE {} ({})'.format(copy_table, columnsAndInfoBag2))
        execute_query(cur, 'INSERT INTO {} SELECT {} FROM {}'.format(copy_table, columnsOrig, current))

        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(temp_table))
        execute_query(cur, 'CREATE TABLE {} AS SELECT * FROM {} NATURAL JOIN {}'.format(temp_table, copy_table, next))

        execute_query(cur, 'SELECT * FROM {}'.format(temp_table))
        print_results(cur)

        temp_columns = get_columns_omitbags(cur, temp_table)
        temp_columns_type = " TEXT,".join(temp_columns) + " TEXT, bag TEXT"
        columnsOrigMCopy = ",".join(temp_columns) + ",bag*bag2"

        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(result_table))
        execute_query(cur, 'CREATE TABLE {} ({})'.format(result_table, temp_columns_type))
        execute_query(cur, 'INSERT INTO {} SELECT {} FROM {}'.format(result_table, columnsOrigMCopy, temp_table))

        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(temp_table))
        execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(copy_table))
        current = result_table
    return result_table


def process_query_bag(inputline, cur):
    print("Processing bag")

    # "project <code1, code2> select[code1='YUL'] (a)"
    # "project <code1, code2> select[code1='YUL'] (a join b)"
    # "project <code1, code2> (a join b)"
    # "project <code1> select[code1='YUL'] (a join b)"
    # inputline = "project <code1, code2, code3> (test_bag join test_bag1)"

    select_conditions = get_select_conditions(inputline)
    projections = get_projections(inputline)
    new_name = get_new(inputline)
    relations = get_relations(inputline)

    query = ""
    union = False
    for relation in relations:
        if union:
            query += " UNION ALL "

        if re.search(" join ", relation, re.IGNORECASE):
            relation = process_join(cur, relation)

        query += 'Select {} from {}'.format(projections, relation)
        if select_conditions:
            query += ' where {}'.format(select_conditions)
        union = True

    temp_name = "_bag_step1"
    execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(temp_name))
    execute_query(cur, 'CREATE TABLE {} AS {}'.format(temp_name, query))

    # execute_query(cur, '{}'.format(query))
    # print_results(cur);

    columns = ",".join(get_columns(cur, temp_name))

    execute_query(cur, 'DROP TABLE IF EXISTS {}'.format(new_name))
    # execute_query(cur, 'CREATE TABLE {} AS SELECT * FROM {} '.format(new_name, temp_name))
    execute_query(cur, 'CREATE TABLE {} AS SELECT {},sum(bag) as bag FROM {} GROUP BY {}'.format(new_name, columns,
                                                                                                 temp_name, columns))
    execute_query(cur, 'SELECT * FROM {}'.format(new_name))
    print_results(cur)
