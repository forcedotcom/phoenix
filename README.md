<h1>Phoenix: JDBC Driver for HBase<br />
<em><sup><sup>'We put the SQL back in NoSQL'</sup></sup></em></h1>
Phoenix is a SQL layer over HBase, delivered as an embedded JDBC driver. Phoenix targets low-latency queries (milliseconds), as opposed batch operation via map/reduce. To see what's supported, go to our [language reference guide](http://forcedotcom.github.com/Phoenix/), and read more on our [wiki](https://github.com/forcedotcom/Phoenix/wiki).

## How It Works ##

The Phoenix query engine transforms your [SQL query](http://forcedotcom.github.com/Phoenix/#select) into one or more HBase scans, and orchestrates their execution to produce standard JDBC result sets. Direct use of the HBase API, along with coprocessors and custom filters, results in [performance](https://github.com/forcedotcom/Phoenix/wiki/Performance) on the order of milliseconds for small queries, or seconds for millions of rows. 

Tables are created and altered through [DDL statements](http://forcedotcom.github.com/Phoenix/#create), and their schema is stored and versioned on the server in an HBase table. Columns are defined as either being part of a multi-part row key, or as key/value cells. You can also map Phoenix on to existing tables (with some minimal changes: see the [wiki](https://github.com/forcedotcom/Phoenix/wiki) for more details).

Applications interact with Phoenix through a standard JDBC interface; all the usual interfaces are supported, including `Connection`, `Statement`, `PreparedStatement`, and `ResultSet`. The driver class is `phoenix.jdbc.PhoenixProdEmbeddedDriver`, and the connection url is `phoenix:jdbc:` followed by the zookeeper quorum hostname specification. For example:

        Class.forName("phoenix.jdbc.PhoenixProdEmbeddedDriver");
        Connection conn = DriverManager.getConnection("phoenix:jdbc:localhost");

For detailed documentation on the current level of SQL support, see our [language reference guide](http://forcedotcom.github.com/Phoenix/). For details about how Phoenix handles schema, transactions, and more, see the [wiki](https://github.com/forcedotcom/Phoenix/wiki).

## System Requirements ##
* HBase v 0.94.2 or higher
* JDK 6 or higher

## Installation ##
To install a pre-built phoenix, use these directions:

* Download the following two jars: [phoenix-1.0.jar](http://forcedotcom.github.com/Phoenix/lib/phoenix-1.0.jar) and [phoenix-1.0-client.jar](http://forcedotcom.github.com/Phoenix/lib/phoenix-1.0-client.jar)
* Add the phoenix-1.0.jar to the classpath of every HBase region server. An easy way to do this is to copy it into the HBase lib directory.
* Restart all region servers.
* Add the phoenix-1.0-client.jar to the classpath of any Phoenix client. This jar includes the minimum set of required HBase jars, along with the following required phoenix jars
    * phoenix.jar
    * antlr-3.5-complete.jar
    * opencsv-2.3.jar

Alternatively, you can build it yourself using maven by following these [build instructions](https://github.com/forcedotcom/Phoenix/wiki#building).

## Getting Started ##
One way to experiment with Phoenix is to download and install a SQL client such as [SQuirrel](http://squirrel-sql.sourceforge.net/). Here are the setup steps necessary:

1. Copy the phoenix-client.jar into the lib directory of SQuirrel
2. Start SQuirrel and add new driver to Squirrel (Drivers -> New Driver)
3. In Add Driver dialog box, set Name to Phoenix
4. Press List Drivers button and jdbc.PhoenixProdEmbeddedDriver should be automatically populated in Class Name textbox. Press OK to close this dialog.
5. Switch to Alias tab and create new Alias (Aliases -> New Aliases)
6. In the dialog box, Name: <any name>, Driver: Phoenix, User Name: _anything_, Password: _anything_
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
Join one or both of our Google groups:

* [Phoenix HBase User](https://groups.google.com/forum/#!forum/phoenix-hbase-user) for users of Phoenix.
* [Phoenix HBase Dev](https://groups.google.com/forum/#!forum/phoenix-hbase-dev) for developers of Phoenix.
