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
        return Operation.PROJECTION.value + " " + m.group(1)
    return "Error"

def testQuery():
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute ("CREATE TABLE flights (from_airport text, to_airport text, days text)")
    cur.execute ("INSERT INTO flights VALUES ('YUL', 'DME', '1234567')")
    cur.execute ("SELECT * FROM flights WHERE from_airport='YUL'")
    print(cur.fetchone())
    cur.close ()

def main():
    inputline = "P(atr1, atr2)"
    print(parseLine(inputline))
    testQuery()

main()
