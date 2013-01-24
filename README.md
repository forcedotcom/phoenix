<h1>Phoenix JDBC Driver for HBase<br />
<em><sup><sup>We put the SQL back in the NoSQL</sup></sup></em></h1>
Phoenix is a SQL layer over HBase delivered as an embedded JDBC driver targeting low latency queries over HBase data. Tables are created and updated through DDL statements and stored and versioned on the server in an HBase table. Columns are defined as either being part of a multi-part row key or as key value cells. The Phoenix query engine transforms your [SQL query](http://forcedotcom.github.com/Phoenix/#select) into one or more HBase scans and orchestrates their execution to produce standard JDBC result sets. To see what's supported, go to our [language reference guide](http://forcedotcom.github.com/Phoenix/).

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
The DML commands of Phoenix ([UPSERT VALUES](http://forcedotcom.github.com/Phoenix/#upsert_values), [UPSERT SELECT](http://forcedotcom.github.com/Phoenix/#upsert_select) and [DELETE](http://forcedotcom.github.com/Phoenix/#delete)) batch pending changes to HBase tables on the client side. The changes are sent to the server when the transaction is committed and discarded when the transaction is rolled back. Phoenix does not providing any additional transactional semantics beyond what HBase supports when a batch of mutations is submitted to the server. If auto commit is turned on for a connection, then Phoenix will, whenver possible, execute the entire DML command through a coprocessor on the server-side, so performance will improve.

## Meta Data ##
The catalog of tables, their columns, primary keys, and types may be retrieved via the java.sql metadata interfaces: DatabaseMetaData, ParameterMetaData, and ResultSetMetaData. For retrieving schemas, tables, and columns through the DatabaseMetaData interface, the schema pattern, table pattern, and column pattern are specified as in a LIKE expression (i.e. % and _ are wildcards escaped through the \ character). The table catalog argument to the metadata APIs deviates from a more standard relational database model, and instead is used to specify a column family name (in particular to see all columns in a given column family).

For detailed documentation on the current level of SQL support, see our [language reference guide](http://forcedotcom.github.com/Phoenix/).

## System Requirements ##
* HBase v 0.94.2 or higher
* JDK 6 or higher

## Installation ##
To install a pre-built phoenix, use these directions
* Download the following two jars:
  * [phoenix.jar](http://forcedotcom.github.com/Phoenix/downloads/phoenix.jar)
  * [phoenix-client.jar](http://forcedotcom.github.com/Phoenix/downloads/phoenix-client.jar)
* Add the phoenix.jar to the classpath of any HBase region server. An easy way to do this is to copy it into the HBase lib directory.
* Add the phoenix-client.jar to the classpath of any Phoenix client. This jar includes the minimum set of required HBase jars, along with the following required phoenix jars
  * phoenix.jar
  * antlr-3.2.jar
  * opencsv-2.3.jar

Alternatively, you can build it yourself by following these [build instructions](https://github.com/forcedotcom/Phoenix/wiki/Home#building).

## Getting Started ##
One way to experiment with Phoenix is to download and install a SQL client such as [SQuirrel](http://squirrel-sql.sourceforge.net/). Here are the setup steps necessary:

1. Copy the phoenix-client.jar into the lib directory of SQuirrel
2. Start SQuirrel and add new driver to Squirrel (Drivers -> New Driver)
3. In Add Driver dialog box, set Name to Phoenix
4. Press List Drivers button and jdbc.PhoenixProdEmbeddedDriver should be automatically populated in Class Name textbox. Press OK to close this dialog.
5. Switch to Alias tab and create new Alias (Aliases -> New Aliases)
6. In the dialog box, Name: <any name>, Driver: Phoenix, User Name: <anything>, Password: <anything>
7. Construct URL as follows: jdbc:phoenix:<zookeeper quorum server>. For example, to connect to a local HBase use: jdbc:phoenix:localhost
8. Press Test (which should succeed if everything is setup correctly) and press OK to close.
9. Now double click on your newly created Phoenix alias and click Connect. Now you are ready to run SQL queries against Phoenix.

You can now issue SQL statements in the SQL tab (create tables, insert data, run queries), and inspect table metadata in the Object tab (i.e. list tables, their columns, primary keys, and types) directly in Squirrel.

In addition, several basic shell scripts are provided to allow for direct SQL execution:

* bin/psql.sh to run one or more .SQL scripts with the output sent to stdout
* bin/pcsv.sh to populate a Phoenix table from a CSV file

## Samples ##
The best place to see samples are in our unit tests under test/func/java. These are end-to-end tests demonstrating how to use all aspects of the Phoenix JDBC driver. 

##Mailing List##
Join our [Phoenix HBase](https://groups.google.com/forum/#!forum/phoenix-hbase) Google group and let us know if you have ideas or run into problems.

