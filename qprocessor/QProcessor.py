# Reads the relation definition in text form and converts it into a suitable internal data structure 
# (e.g., lists, dictionaries, or classes).
# Parses the relational algebra query and maps it into a sequence of ordered operations 
# (selection, projection, join, union, etc.).

import re
from tabulate import tabulate

relations = {}

def select(rows, condition):
    """
    Evaluate select condition on rows.
    Example condition: Age > 30 or Employees.name = Departments.name or Name = 'John'
    """
    if not rows:
        return []
        
    attribute, op, value = re.split(r"\s*(>=|<=|=|>|<)\s*", condition)
    attribute, value = attribute.strip(), value.strip()

    # Check if attribute exists in the first row to give better error messages
    sample_row = rows[0]
    attr_found = False

    result = []
    for row in rows:
        # Handle qualified attribute names (e.g., Employees.name)
        if '.' in attribute:
            # Extract the attribute name after the dot
            attr_name = attribute.split('.')[1]
            # Check if this attribute exists in the row, or try the _2 version
            if attr_name in row:
                left_val = row[attr_name]
                attr_found = True
            elif f"{attr_name}_2" in row:
                left_val = row[f"{attr_name}_2"]
                attr_found = True
            else:
                continue  # Skip if attribute not found
        else:
            if attribute in row:
                left_val = row[attribute]
                attr_found = True
            else:
                continue  # Skip if attribute not found

        # Handle qualified value names (e.g., Departments.name) or quoted strings
        if '.' in value:
            # This is comparing two attributes
            value_attr = value.split('.')[1]
            if value_attr in row:
                right_val = row[value_attr]
            elif f"{value_attr}_2" in row:
                right_val = row[f"{value_attr}_2"]
            else:
                continue  # Skip if attribute not found
        else:
            # This is comparing to a literal value
            right_val = value
            
            # Handle quoted strings by removing quotes
            if (right_val.startswith("'") and right_val.endswith("'")) or \
               (right_val.startswith('"') and right_val.endswith('"')):
                right_val = right_val[1:-1]  # Remove quotes
            else:
                # Try to convert value to int if possible
                try:
                    right_val = int(value)
                except ValueError:
                    pass  # keep as string

        # Convert left value if it's numeric
        try:
            left_val = int(left_val)
        except ValueError:
            pass  # keep as string

        # Perform comparison
        if op == ">" and left_val > right_val: 
            result.append(row)
        elif op == "<" and left_val < right_val: 
            result.append(row)
        elif op == ">=" and left_val >= right_val: 
            result.append(row)
        elif op == "<=" and left_val <= right_val: 
            result.append(row)
        elif op == "=" and left_val == right_val: 
            result.append(row)
    
    # Give a warning if no attribute was found
    if not attr_found and rows:
        attr_name = attribute.split('.')[1] if '.' in attribute else attribute
        available_attrs = list(sample_row.keys())
        print(f"Warning: Attribute '{attr_name}' not found. Available attributes: {available_attrs}")

    return result

def project(rows, attributes, all_attributes):
    """
    Project relation onto specified attributes.
    Returns a list of dictionaries (each projected row).
    """
    if not rows:
        return []
        
    # Check for invalid attributes
    invalid_attrs = [attr for attr in attributes if attr not in all_attributes]
    if invalid_attrs:
        raise ValueError(f"Invalid attributes for projection: {invalid_attrs}. Available: {all_attributes}")
    
    projected_rows = []
    for row in rows:
        projected_row = {attr: row[attr] for attr in attributes if attr in all_attributes}
        projected_rows.append(projected_row)

    return projected_rows

def cartesian_product(rows1, rows2):
    result = []
    for r1 in rows1:
        for r2 in rows2:
            combined = r1.copy()
            # Handle attribute name collisions
            for k, v in r2.items():
                if k in combined:
                    combined[f"{k}_2"] = v   # rename conflicting attributes
                else:
                    combined[k] = v
            result.append(combined)
    return result

def join(relation1, relation2, condition=None):
    """
    Join two relations based on condition.
    Examples:
      join("Employees", "Departments")  
      join("Employees", "Departments", "Employees.DeptID = Departments.DID")
    """
    print(f"Joining {relation1} and {relation2} with condition: {condition}")

    name1, attrs1, rows1 = relations[relation1]
    name2, attrs2, rows2 = relations[relation2]

    # Cartesian product (always start from this)
    cartesian = cartesian_product(rows1, rows2)

    # Find common attributes
    common_attrs = [attr for attr in attrs1 if attr in attrs2]

    # CASE 1: No common attributes, no condition → return Cartesian product
    if not common_attrs and condition is None:
        return cartesian

    # CASE 2: No common attributes, but condition exists → filter Cartesian product
    if not common_attrs and condition is not None:
        return select(cartesian, condition)  # reuse your select function

    # CASE 3: Common attributes, but no condition → natural join
    if common_attrs and condition is None:
        result = []
        for row in cartesian:
            # keep only rows where common attributes match
            if all(row[attr] == row[f"{attr}_2"] for attr in common_attrs):
                # drop the duplicate _2 attributes
                cleaned = {k: v for k, v in row.items() if not k.endswith("_2")}
                result.append(cleaned)
        return result

    # CASE 4: Common attributes AND condition → natural join + filter
    if common_attrs and condition is not None:
        result = []
        for row in cartesian:
            if all(row[attr] == row[f"{attr}_2"] for attr in common_attrs):
                cleaned = {k: v for k, v in row.items() if not k.endswith("_2")}
                result.append(cleaned)
        return select(result, condition)

    return cartesian  # fallback (shouldn't reach here)

    
def union(relation1, relation2):
    """
    Union of two relations. Relations must have compatible schemas.
    Returns all unique rows from both relations.
    """
    print(f"Computing union of {relation1} and {relation2}")
    
    name1, attrs1, rows1 = relations[relation1]
    name2, attrs2, rows2 = relations[relation2]
    
    # Check schema compatibility
    if set(attrs1) != set(attrs2):
        raise ValueError(f"Relations {relation1} and {relation2} have incompatible schemas")
    
    # Combine rows and remove duplicates
    result = []
    seen = set()
    
    for row in rows1 + rows2:
        # Create a tuple of values in consistent order for duplicate detection
        row_tuple = tuple(row[attr] for attr in sorted(attrs1))
        if row_tuple not in seen:
            seen.add(row_tuple)
            result.append(row)
    
    return result

def intersection(relation1, relation2):
    """
    Intersection of two relations. Relations must have compatible schemas.
    Returns rows that appear in both relations.
    """
    print(f"Computing intersection of {relation1} and {relation2}")
    
    name1, attrs1, rows1 = relations[relation1]
    name2, attrs2, rows2 = relations[relation2]
    
    # Check schema compatibility
    if set(attrs1) != set(attrs2):
        raise ValueError(f"Relations {relation1} and {relation2} have incompatible schemas")
    
    # Find rows that appear in both relations
    result = []
    rows2_tuples = set()
    
    # Convert rows2 to set of tuples for efficient lookup
    for row in rows2:
        row_tuple = tuple(row[attr] for attr in sorted(attrs1))
        rows2_tuples.add(row_tuple)
    
    # Find matching rows from rows1
    for row in rows1:
        row_tuple = tuple(row[attr] for attr in sorted(attrs1))
        if row_tuple in rows2_tuples:
            result.append(row)
    
    return result

def difference(relation1, relation2):
    """
    Difference of two relations (relation1 - relation2).
    Relations must have compatible schemas.
    Returns rows that are in relation1 but not in relation2.
    """
    print(f"Computing difference of {relation1} - {relation2}")
    
    name1, attrs1, rows1 = relations[relation1]
    name2, attrs2, rows2 = relations[relation2]
    
    # Check schema compatibility
    if set(attrs1) != set(attrs2):
        raise ValueError(f"Relations {relation1} and {relation2} have incompatible schemas")
    
    # Find rows that are in relation1 but not in relation2
    result = []
    rows2_tuples = set()
    
    # Convert rows2 to set of tuples for efficient lookup
    for row in rows2:
        row_tuple = tuple(row[attr] for attr in sorted(attrs1))
        rows2_tuples.add(row_tuple)
    
    # Find rows from rows1 that are not in rows2
    for row in rows1:
        row_tuple = tuple(row[attr] for attr in sorted(attrs1))
        if row_tuple not in rows2_tuples:
            result.append(row)
    
    return result

def parse_relation(text):
    """
    Parse one or more relation definitions from text.
    Returns a dict mapping relation names -> (name, attributes, rows).
    Example:
      Employees (EID, Name, Age) = { ... }
      Departments (DID, DeptName) = { ... }
    """
    pattern = r"""
    (\w+)\s*                   # Relation name
    \(\s*([^)]+?)\s*\)\s*      # Attributes
    =\s*\{\s*                  # Opening brace
    ([\s\S]+?)                 # Rows
    \}\s*                      # Closing brace
    """
    regex = re.compile(pattern, re.VERBOSE | re.MULTILINE)

    relations_found = {}
    for match in regex.finditer(text.strip()):
        name = match.group(1)
        attributes = [attr.strip() for attr in match.group(2).split(",")]
        rows_text = [row.strip() for row in match.group(3).strip().split("\n") if row.strip()]
        rows = [{attr: val.strip() for attr, val in zip(attributes, row.split(","))} for row in rows_text]
        relations_found[name] = (name, attributes, rows)

    if not relations_found:
        raise ValueError("No valid relation found in text block")

    return relations_found

def parse_query(line):
    """
    Parse query of the form:
    select Age > 30 (Employees)
    """
    operations = []

    line = line.replace("Query:", "").strip()
    pattern = r"(select|project|join|union|intersection|difference)\s+(.+)\s*\((\w+)\)"
    match = re.match(pattern, line, re.IGNORECASE)

    if not match:
        raise ValueError("Invalid query format")

    operator = match.group(1).lower()
    condition = match.group(2).strip()
    relation_name = match.group(3).strip()
    return (operator, condition, relation_name)

def print_table(attributes, rows, name):
    """
    Employees = {EID, Name, Age
    "E1", "John", 32
    }
    """
    # print header
    print(f"{name} = {rows}")

    print(f"\n{name}")

    # Convert rows (list of dicts) to list of lists
    table_data = [[row[attr] for attr in attributes] for row in rows]

    # Print table
    print(tabulate(table_data, headers=attributes, tablefmt="grid"))
    print("\n" + "="*40 + "\n")

class RAExpression:
    def __init__(self, op, arg=None, params=None, name=None):
        self.op = op        # 'select', 'project', 'join', etc.
        self.arg = arg      # child expression OR relation name
        self.params = params # condition or list of attributes
        self.name = name 

def eval_expression(expr):
    if expr.op == "relation":  # base case
        if expr.name not in relations:
            raise ValueError(f"Unknown relation: {expr.name}")
        name, attrs, rows = relations[expr.name]
        return name, attrs, rows

    elif expr.op == "select":
        name, attrs, rows = eval_expression(expr.arg)
        result = select(rows, expr.params)
        return name, attrs, result

    elif expr.op == "project":
        name, attrs, rows = eval_expression(expr.arg)
        result = project(rows, expr.params, attrs)
        return name, expr.params, result

    elif expr.op == "join":
        print(f"Evaluating join with params: {expr.params}")
        left_name = expr.params["left"]
        right_name = expr.params["right"]
        condition = expr.params["condition"]

        result_rows = join(left_name, right_name, condition)

        # Merge attributes, handling duplicates
        _, attrs1, _ = relations[left_name]
        _, attrs2, _ = relations[right_name]
        
        # Create merged attributes list - keep original names and add _2 for duplicates
        merged_attrs = attrs1.copy()
        for attr in attrs2:
            if attr not in attrs1:
                merged_attrs.append(attr)
            else:
                merged_attrs.append(f"{attr}_2")

        return f"join_{left_name}_{right_name}", merged_attrs, result_rows

    elif expr.op == "union":
        left_name = expr.params["left"]
        right_name = expr.params["right"]
        result_rows = union(left_name, right_name)
        
        # Use attributes from first relation (they should be compatible)
        _, attrs, _ = relations[left_name]
        return f"union_{left_name}_{right_name}", attrs, result_rows

    elif expr.op == "intersection":
        left_name = expr.params["left"]
        right_name = expr.params["right"]
        result_rows = intersection(left_name, right_name)
        
        # Use attributes from first relation (they should be compatible)
        _, attrs, _ = relations[left_name]
        return f"intersection_{left_name}_{right_name}", attrs, result_rows

    elif expr.op == "difference":
        left_name = expr.params["left"]
        right_name = expr.params["right"]
        result_rows = difference(left_name, right_name)
        
        # Use attributes from first relation (they should be compatible)
        _, attrs, _ = relations[left_name]
        return f"difference_{left_name}_{right_name}", attrs, result_rows

def parse_expression(s):
    s = s.strip()

    # Remove "Query:" prefix if present
    if s.lower().startswith("query:"):
        s = s[len("query:"):].strip()

    # print(f"DEBUG: Parsing expression: '{s}'")

    # Special handling for join operations
    join_pattern = r"^join\s+(.+)$"
    join_match = re.match(join_pattern, s, re.IGNORECASE)
    
    if join_match:
        join_params = join_match.group(1).strip()
        print(f"Parsing join params: {join_params}")
        
        # Split by " on " to separate relations from condition
        parts = join_params.split(" on ")
        left_right = parts[0].split(",")
        left = left_right[0].strip()
        right = left_right[1].strip()

        condition = None
        if len(parts) > 1:
            condition = parts[1].strip()

        params = {
            "left": left,
            "right": right,
            "condition": condition
        }
        
        print(f"Join params: {params}")
        return RAExpression(op="join", params=params)

    # Special handling for set operations (union, intersection, difference)
    set_pattern = r"^(union|intersection|difference)\s+(.+)$"
    set_match = re.match(set_pattern, s, re.IGNORECASE)
    
    if set_match:
        op = set_match.group(1).lower()
        set_params = set_match.group(2).strip()
        print(f"Parsing {op} params: {set_params}")
        
        # Split by comma to get the two relations
        relations_list = [rel.strip() for rel in set_params.split(",")]
        if len(relations_list) != 2:
            raise ValueError(f"Set operation {op} requires exactly two relations")
        
        params = {
            "left": relations_list[0],
            "right": relations_list[1]
        }
        
        print(f"{op} params: {params}")
        return RAExpression(op=op, params=params)

    # Match select/project operations with parentheses
    # Use a more robust pattern that handles nested parentheses better
    select_project_pattern = r"^(select|project)\s+(.+)\s+\(([^)]+)\)$"
    sp_match = re.match(select_project_pattern, s, re.IGNORECASE)
    
    if sp_match:
        op = sp_match.group(1).lower()
        params = sp_match.group(2).strip()
        inner = sp_match.group(3).strip()
        
        # print(f"DEBUG: Found {op} operation with params='{params}', inner='{inner}'")

        # If project, split attributes by comma
        if op == "project":
            params = [p.strip() for p in params.split(",")]

        # Recursively parse the inner expression
        inner_expr = parse_expression(inner)
        return RAExpression(op=op, arg=inner_expr, params=params)

    # Base case: just a relation name
    name = s.strip()
    # print(f"DEBUG: Treating as relation name: '{name}'")
    return RAExpression(op="relation", name=name)

if __name__ == "__main__":
    # Read file
    with open("Employees.txt") as f:
        content = f.read()

    # Split into relation definitions and queries more carefully
    # First, split by Query: but keep the Query: prefix
    parts = re.split(r'(?=Query:)', content, flags=re.MULTILINE)
    
    for part in parts:
        part = part.strip()
        
        if part.startswith("Query:"):
            # Extract just the query line (first line of the part)
            query_lines = part.split('\n')
            query_line = query_lines[0].strip()  # Just the first line with "Query:"
            
            # Remove inline comments from the query line
            if '#' in query_line:
                query_line = query_line.split('#')[0].strip()
            
            try:
                print(f"\nExecuting: {query_line}")
                expressions = parse_expression(query_line)
                name, attrs, rows = eval_expression(expressions)
                print_table(attrs, rows, name)
            except Exception as e:
                print(f"Error executing query '{query_line}': {e}")
                
        elif part:  # Non-empty part that's not a query
            # Clean the part by removing comments and empty lines
            clean_lines = []
            for line in part.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    clean_lines.append(line)
            
            if clean_lines:
                clean_part = '\n'.join(clean_lines)
                try:
                    rels = parse_relation(clean_part)
                    relations.update(rels)
                    print(f"Loaded relations: {list(rels.keys())}")
                except ValueError as e:
                    print(f"Warning: Could not parse relations in: {clean_part[:50]}...")