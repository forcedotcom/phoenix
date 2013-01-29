How to run psql script with included examples
=============================================

Create web_stat table, load CSV data and run queries:
./psql.sh -h host,domain,feature,date,core,db,active_visitor -s jdbc:phoenix:localhost ../examples/web_stat.sql ../examples/web_stat.csv ../examples/web_stat_queries.sql

Note: Please see comments in web_stat_queries.sql for the sample queries being executed
