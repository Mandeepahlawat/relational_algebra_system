import re
from enum import Enum

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


def main():
    inputline = "P(atr1, atr2)"
    print(parseLine(inputline))




main()
