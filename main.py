import re
from enum import Enum
import sqlite3

class Operation (Enum):
    SELECT = "Select"
    UNION = "Union"
    PROJECTION = "Project"

def parseLine(line):
    if re.search("^S\(([A-Za-z0-9]+)\)$", line):
        m = re.search("^S\(([A-Za-z]+)\)$", line)
        return Operation.SELECT.value + " " + m.group(1)
    if re.search("^U\(([A-Za-z0-9]+), ([A-Za-z0-9]+)\)$", line):
        m = re.search("^U\(([A-Za-z0-9]+), ([A-Za-z0-9]+)\)$", line)
        return Operation.UNION.value + " " + m.group(1) + " " + m.group(2)
    if re.search("^P\((.*)\)$", line):
        m = re.search("^P\((.*)\)$", line)
        cols=m.group(1).split(",")
        table=cols.pop(0).strip()
        attrs=""
        for a in cols:
            attrs+=a.strip()+","
        attrs=attrs[:-1]
        return Operation.PROJECTION.value, table, attrs
    return "Error"

def loadTable(con, name):
    cur = con.cursor()
    cur.execute ("SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name='"+name+"'")
    r=cur.fetchone()[0]
    if (r==0):
        with open(name+".txt") as f:
            content=f.readlines()
            cols=len(content[0].split("\t"))
            create="CREATE TABLE "+name+" ("
            names="("
            for i in range(cols):
                create+="c"+str(i)+" TEXT,"
                names+="?,"
            create=create[:-1]+")"
            names=names[:-1]+")"
            cur.execute (create)
            query="INSERT INTO "+name+" VALUES "+names
            for line in content:
                values=tuple(line[:-1].split("\t"))
                cur.execute(query, values)

def projection(con, table, columns):
    cur = con.cursor()
    cur.execute ("SELECT "+columns+" FROM "+table)
    res=cur.fetchall()
    for line in res:
        print(line)
    

def main():
    con = sqlite3.connect(":memory:")
    inputline = "P(a, c2, c1, c0)"
    (operation, table, attrs)=parseLine(inputline)
    loadTable(con, table)
    if (operation==Operation.PROJECTION.value):
        projection(con, table, attrs)

main()
