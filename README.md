# Distributed-Database
A distributed MySQL database that supports read-only and update SQL queries on database that is fragmented. It supports all types of fragmentation i.e. Horizontal, Vertical, Derived Horizontal and Hybrid Fragmentation.

### File Structure
'query.py' contains the main query class. To execute your query run this file.
'utils.py' contains the utility functions that the query class uses to parse query and access the system catalog.
'solve_predicates.py' is used to find solve whether the result of predicate for two fragments is true(non-empty) or false(empty).
'fragmentation_schema.py' contains code to update fragmentation schema of the database as per the provided schema.
'example_schema.csv' contains example fragmentation schema of the database.

### Assumptions
The project assumes 3 sites across which the fragments are distributed.
