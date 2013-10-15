SqlLine
=======
https://github.com/julianhyde/sqlline

Execute SQL from command line. Sqlline manual is available at http://www.hydromatic.net/sqlline/manual.html
	
	Usage: 
	$ sqlline.sh <zookeeper> <optional_sql_file> 
	Example: 
	$ sqlline.sh localhost
	$ sqlline.sh localhost ../examples/stock_symbol.sql

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

csv-bulk-loader.sh
==================

Usage: csv-bulk-loader <option value>

<option>  <value>
-i        CSV data file path in hdfs (mandatory)
-s        Phoenix schema name (mandatory if not default)
-t        Phoenix table name (mandatory)
-sql      Phoenix create table sql file path (mandatory)
-zk       Zookeeper IP:<port> (mandatory)
-o        Output directory path in hdfs (optional)
-idx      Phoenix index table name (optional, not yet supported)
-error    Ignore error while reading rows from CSV? (1-YES | 0-NO, default-1) (optional)
-help     Print all options (optional)

Example: Set the HADOOP_HOME variable and run as:
./csv-bulk-loader.sh -i data.csv -s Test -t Phoenix -sql ~/Documents/createTable.sql -zk localhost:2181


