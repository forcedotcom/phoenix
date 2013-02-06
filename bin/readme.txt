psql.sh
=======

Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] <zookeeper>  <path-to-sql-or-csv-file>...

Example 1. Create table, upsert row and run query using single .sql file
./psql localhost ../examples/stock_symbol.sql

Example 2. Create table, load CSV data and run queries using .csv and .sql files:
./psql.sh localhost ../examples/web_stat.sql ../examples/web_stat.csv ../examples/web_stat_queries.sql

Note: Please see comments in web_stat_queries.sql for the sample queries being executed

performance.sh
==============

Usage: performance <zookeeper> <row count>

Example: Generates and upserts 1000000 rows and time basic queries on this data
./performance.sh localhost 1000000
