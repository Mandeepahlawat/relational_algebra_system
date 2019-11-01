import glob
import re
from enum import Enum
import sqlite3

def loadTable(cur, name):
    cur.execute ("SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='"+name+"'")
    table_count=cur.fetchone()[0]
    #table already exists so delete existing table
    if table_count != 0:
        cur.execute ("DROP TABLE {}".format(name))
    
    with open(name+".txt") as f:
        content = f.readlines()
        column_names = content[0].split("\t")
        column_names = [col.strip() for col in column_names]
        column_names = [col for col in column_names if col]
        cur.execute ("CREATE TABLE {} ({})".format(name, " text, ".join(column_names) + ' text'))

        # insert lines by skipping first row
        for idx, line in enumerate(content):
            if idx != 0:
                line_content = line.split("\t")
                line_content = [line.strip() for line in line_content]
                # removes empty string values from the list
                line_content = [line for line in line_content if line]
                # extra step, if we don't do it sqlitite thinks that individual values are variables instead of string
                line_content = ["'{}'".format(line) for line in line_content]

                cur.execute("INSERT INTO {} values ({})".format(name, ", ".join(line_content)))

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
        return project_cols
    else:
        return '*'

def print_results(cur):
    results = cur.fetchall()
    print("\n\n**************** RESULTS ****************\n\n")
    for result in results:
        print(result)
        print

def main():
    table_names=[name[:-4] for name in glob.glob("*.txt")]
    
    # create sql database and load all the data
    con = sqlite3.connect("relational_algebra")
    cur = con.cursor()

    for table_name in table_names:
        loadTable(cur, table_name)

    # format of input line "project <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)"
    inputline = input("Input your query in this format:\nproject <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)\nOr q to quit\n\n")

    while (inputline and (inputline!="q")):
        # inputline = "project <code1,code2> select[code1='YUL', code2='CDG'] (a)"     
        select_conditions = get_select_conditions(inputline)    
        projections = get_projections(inputline)
        relations = get_relations(inputline)
        
        if select_conditions:
            query = 'Select {} from {} where {}'.format(projections, relations, select_conditions)
        else:
            query = 'Select {} from {}'.format(projections, relations)

        cur.execute(query)
        print_results(cur)
        inputline = input("Input your query in this format:\nproject <projection_column1, projection_column2> select[condition1, condition2] (table_name1 join table_name2)\nOr q to quit\n\n")

    print ("Thank you and have a nice day!")
    con.commit()
    con.close()

main()
