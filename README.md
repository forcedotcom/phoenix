# Phoenix JDBC Driver for HBase #
Phoenix is a SQL layer over HBase delivered as an embedded JDBC driver targeting low latency queries over HBase data. Tables are created and updated through DDL statements and stored and versioned on the server in an HBase table. Columns are defined as either being part of a multi-part row key or as key value cells. The Phoenix query engine transforms your SQL query into one or more HBase scans and orchestrates their execution to produce standard JDBC result sets.

A Phoenix table can either be:

1. built from scratch, in which case the HBase table and column families will be created automatically.
2. mapped to an existing HBase table, by creating either a read-write TABLE or a read-only VIEW, with the caveat that the binary representation of the row key and key values must match that of the Phoenix data types (see Data Types tab for the detail on the binary representation).
  * For a read-write TABLE, column families will be created automatically if they don't already exist. An empty key value will be added to the first column family of each existing row to minimize the size of the projection for queries.
  * For a read-only VIEW, all column families must already exist. The only change made to the HBase table will be the addition of the Phoenix coprocessors used for query processing. The primary use case for a VIEW is to transfer existing data into a Phoenix table, since data modification are not allowed on a VIEW and query performance will likely be less than as with a TABLE.

Most commonly, an application will let HBase manage timestamps. However, under some circumstances, an application needs to control the timestamps itself. In this case, a long-valued "CurrentSCN" property may be specified at connection time to control timestamps for any DDL, DML, or query. This capability may be used to run snapshot queries against prior row values, since Phoenix uses the value of this connection property as the max timestamp of scans.

The standard java.sql JDBC interfaces are supported: Connection, Statement, PreparedStatement, and ResultSet. The driver class is "phoenix.jdbc.PhoenixProdEmbeddedDriver" and the connection url is "phoenix:jdbc:" followed by the zookeeper quorum host name specification.

For example:

        Class.forName("phoenix.jdbc.PhoenixProdEmbeddedDriver");
        Connection conn = DriverManager.getConnection("phoenix:jdbc:localhost");

## Transactions ##
The DML commands of Phoenix (UPSERT and DELETE) batch pending changes to HBase tables on the client side. The changes are sent to the server when the transaction is committed and discarded when the transaction is rolled back. Phoenix does not providing any additional transactional semantics beyond what HBase supports when a batch of mutations is submitted to the server. If auto commit is turned on for a connection, then DML commands are immediately executed on the server. When possible, Phoenix executes the entire DML command on the server (in a coprocessor), so performance will improve.

## Meta Data ##
The catalog of tables, their columns, primary keys, and types may be retrieved via the java.sql metadata interfaces: DatabaseMetaData, ParameterMetaData, and ResultSetMetaData. For retrieving schemas, tables, and columns through the DatabaseMetaData interface, the schema pattern, table pattern, and column pattern are specified as in a LIKE expression (i.e. % and _ are wildcards escaped through the \ character). The table catalog argument to the metadata APIs deviates from a more standard relational database model, and instead is used to specify a column family name (in particular to see all columns in a given column family).

Several basic shell scripts are provided for convenience:

* psql to run one or more .SQL scripts, usually for the purpose of running DDL
* pcsv to populate a Phoenix table from a CSV file

For detailed documentation on the current level of SQL support, see the wiki. 

## System Requirements ##
* HBase v 0.94.2 or higher
* JDK 6 or higher

## Installation ##
* Download the phoenix.jar
* Put phoenix.jar into the lib directory of each region server for the cluster on which you'd like to use Phoenix. It must be on the classpath of HBase.
* Add the following jars to the classpath of any Phoenix client:
  * phoenix.jar
  * antlr-3.2.jar
  * opencsv-2.3.jar
  * commons-configuration-1.6.jar
  * commons-io-2.0.1.jar
  * guava-11.0.2.jar
  * hadoop-core-1.0.4.jar
  * hbase-0.94.0-security.jar
  * jackson-core-asl-1.8.8.jar
  * jackson-mapper-asl-1.8.8.jar
  * protobuf-java-2.4.0a.jar
  * slf4j-api-1.4.3.jar
  * slf4j-log4j12-1.4.3.jar
  * zookeeper-3.4.3.jar

## Getting Started ##
A good way to experiment with Phoenix is to download and install a SQL client such as [Squirrel](http://squirrel-sql.sourceforge.net/)

1. Copy the above Phoenix required client-side jars into the lib directory of Squirrel
2. Start Squirrel and add new driver to Squirrel (Drivers -> New Driver)
3. In Add Driver dialog box, set Name to Phoenix
4. Press List Drivers button and jdbc.PhoenixProdEmbeddedDriver should be automatically populated in Class Name textbox. Press OK to close this dialog.
5. Switch to Alias tab and create new Alias (Aliases -> New Aliases)
6. In the dialog box, Name: <any name>, Driver: Phoenix, User Name: <anything>, Password: <anything>
7. Construct URL as follows: jdbc:phoenix:<zookeeper quorum server>. For example, to connect to a local HBase use: jdbc:phoenix:localhost
8. Press Test (which should succeed if everything is setup correctly) and press OK to close.
9. Now double click on your newly created Phoenix alias and click Connect. Now you are ready to run SQL queries against Phoenix.

You can now create tables, insert data, run queries, and inspect table metadata (i.e. list tables, their columns, primary keys, and types) directly in Squirrel.
