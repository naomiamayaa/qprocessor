Relational Algebra Evaluator

This project implements a simple Relational Algebra (RA) evaluator in Python. It supports core relational operations including:
- Selection 
- Projection 
- Join
- Set operations: union, intersection, difference

Usage:
Define relations in a text file (e.g., Employees.txt) using the format:
Employees (EID, Name, Age) = {
  E1, John, 32
  E2, Alice, 28
}

Write queries using RA syntax:
Query: select Age > 30 (Employees)
Query: project Name (Employees)
Query: join Employees, Departments on Employees.DeptID = Departments.DID
Query: union Employees, Managers

Run the Python script:
python QProcessor.py

Implementation Notes:
Relations are represented as Python dictionaries and lists of dictionaries.
Joins start with a cartesian product, then filter according to the join condition.
Attribute collisions are handled by renaming (_2) to ensure unambiguous references.
Set operations align schemas automatically and remove duplicates when necessary.

Example Output:
For Query: select Age > 30 (Employees):
+-------+-------+-----+
| EID   | Name  | Age |
+=======+=======+=====+
| E1    | John  | 32  |
+-------+-------+-----+

Files:
QProcessor.py – main Python script
Employees.txt – sample relation definitions and queries
