import glob
import re
import sqlite3
from provenance import process_query_provenance
from bag import process_query_bag
from probability import process_probability_query
from standard import process_query_standard
from certainty import process_query_certainty


def loadTable(cur, name):
    cur.execute("SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='" + name + "'")
    table_count = cur.fetchone()[0]
    # table already exists so delete existing table
    if table_count != 0:
        cur.execute("DROP TABLE {}".format(name))

    with open(name + ".txt") as f:
        content = f.readlines()
        column_names = content[0].split(",")
        column_names = [col.strip() for col in column_names]
        column_names = [col for col in column_names if col]
        cur.execute("CREATE TABLE {} ({})".format(name, " text, ".join(column_names) + ' text'))

        # insert lines by skipping first row
        for idx, line in enumerate(content):
            if idx != 0:
                line_content = line.split(",")
                line_content = [line.strip() for line in line_content]
                # removes empty string values from the list
                line_content = [line for line in line_content if line]
                # extra step, if we don't do it sqlitite thinks that individual values are variables instead of string
                cleaned = []
                for line in line_content:
                    if re.search('\'.+\'', line):
                        cleaned.append(line)
                    else:
                        cleaned.append('\'{}\''.format(line))

                line_content = cleaned

                cur.execute("INSERT INTO {} values ({})".format(name, ", ".join(line_content)))


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


def process_query(inputline, cur):
    select_conditions = get_select_conditions(inputline)
    projections = get_projections(inputline)
    relations = get_relations(inputline)
    new_name = get_new(inputline)

    query = ""
    union = False
    for relation in relations:
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


def union_or_natjoin_present(inputline):
    return re.search("union", inputline, re.IGNORECASE) or re.search("natjoin", inputline, re.IGNORECASE)

def process_query_combined(inputline, cur, annotation):
    if (annotation == "4"):
        process_query_provenance(inputline, cur)
    elif annotation == "2":
        process_probability_query(inputline, cur)
    elif (annotation == "1"):
        process_query_bag(inputline, cur)
    elif (annotation == "5"):
        process_query_standard(inputline, cur)
    elif (annotation == "3"):
        process_query_certainty(inputline, cur)
    else:
        process_query(inputline, cur)

def main():
    table_names = [name[:-4] for name in glob.glob("*.txt")]

    # create sql database and load all the data
    con = sqlite3.connect("relational_algebra")
    cur = con.cursor()

    for table_name in table_names:
        loadTable(cur, table_name)

    annotation = 0
    temp = 0

    # format of input line "project <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)"
    inputline = input(
        "Input your query in this format:\nproject <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)\nOr q to quit\n")

    while (inputline and (inputline != "q")):
        # inputline = "project <code1,code2> select[code1='YUL', code2='CDG'] (a)"
        # inputline = "temp: project <code1, code2> select [code1="YUL"] (a,c)"
        # inputline = "project <code1, code2> (temp)"
        # if (re.search("^\d$", inputline)):
        #     annotation=inputline
        #     print("Annotation is "+annotation)
        # else:
        annotation = input("Enter annotation number from 1 to 5\n")
        while int(annotation) > 5:
            annotation = input("Enter annotation number from 1 to 5\n")


        if (union_or_natjoin_present(inputline) and annotation != "2"):

            new_name = get_new(inputline)

            if new_name != '':
                inputline = re.split(':', inputline)[1]
                new_name = new_name + ': '

            sub_query = re.split('natjoin|union', inputline)
            processed_sub_query = []
            for sub in sub_query:
                temp += 1
                query = '_main_temp_{}: {}'.format(temp, sub)
                processed_sub_query.append('_main_temp_{}'.format(temp))
                process_query_combined(query, cur, annotation)
            operations = re.findall('union|natjoin', inputline, re.IGNORECASE)
            conv = []
            for op in operations:
                if op == "union":
                    conv.append(" , ")
                if op == "natjoin":
                    conv.append(" join ")

            query = processed_sub_query.pop(0)

            for op in conv:
                query += op + processed_sub_query.pop(0)
            inputline = new_name + "(" + query + ")"
        process_query_combined(inputline, cur, annotation)


        inputline = input(
            "Input your query in this format:\nproject <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)\nOr q to quit\n")

    print("Thank you and have a nice day!")
    con.commit()
    con.close()


main()
