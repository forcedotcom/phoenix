<h1>Phoenix: A SQL layer over HBase<br />
<em><sup><sup>'We put the SQL back in the NoSQL'</sup></sup></em></h1>
Phoenix is a SQL layer over HBase, delivered as a client-embedded JDBC driver, powering the HBase use cases at Salesforce.com. Phoenix targets low-latency queries (milliseconds), as opposed to batch operation via map/reduce. To see what's supported, go to our [language reference guide](http://forcedotcom.github.com/phoenix/), and read more on our [wiki](https://github.com/forcedotcom/phoenix/wiki).
## Mission
Become the standard means of accessing HBase data through a well-defined, industry standard API.

## How It Works ##

The Phoenix query engine transforms your [SQL query](http://forcedotcom.github.com/phoenix/#select) into one or more HBase scans, and orchestrates their execution to produce standard JDBC result sets. Direct use of the HBase API, along with coprocessors and custom filters, results in [performance](https://github.com/forcedotcom/phoenix/wiki/Performance) on the order of milliseconds for small queries, or seconds for tens of millions of rows. 

Tables are created and altered through [DDL statements](http://forcedotcom.github.com/phoenix/#create), and their schema is stored and versioned on the server in an HBase table. Columns are defined as either being part of a multi-part row key, or as key/value cells. You can also map Phoenix on to existing tables (see the [wiki](https://github.com/forcedotcom/phoenix/wiki) for more details).

Applications interact with Phoenix through a standard JDBC interface; all the usual interfaces are supported, including `Connection`, `Statement`, `PreparedStatement`, and `ResultSet`. The driver class is `com.salesforce.phoenix.jdbc.PhoenixDriver`, and the connection url is `jdbc:phoenix:` followed by the zookeeper quorum hostname specification. For example:

        Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost");

For detailed documentation on the current level of SQL support, see our [language reference guide](http://forcedotcom.github.com/phoenix/). For details about how Phoenix handles schema, transactions, and more, see the [wiki](https://github.com/forcedotcom/phoenix/wiki).

## System Requirements ##
* HBase v 0.94.2 or higher
* JDK 6 or higher

## Build Requirements ##
* All the system requirements
* Maven 3.X (https://maven.apache.org/)


## Installation ##
To install a pre-built phoenix, use these directions:

* Expand the following tar: [phoenix-1.0-install.tar](http://forcedotcom.github.com/phoenix/lib/phoenix-1.0-install.tar)
* Add the phoenix-1.0.jar to the classpath of every HBase region server. An easy way to do this is to copy it into the HBase lib directory.
* Restart all region servers.
* Add the phoenix-1.0-client.jar to the classpath of any Phoenix client.

Alternatively, you can build it yourself using maven by following these [build instructions](https://github.com/forcedotcom/Phoenix/wiki#wiki-building).


## Getting Started ##

<b> Squirrel SQL Client </b>

One way to experiment with Phoenix is to download and install a SQL client such as [SQuirrel](http://squirrel-sql.sourceforge.net/). Since Phoenix is a JDBC driver, integration with tools such as this are seamless. Here are the setup steps necessary:

1. Copy the phoenix-1.0-client.jar into the lib directory of SQuirrel
2. Start SQuirrel and add new driver to SQuirrel (Drivers -> New Driver)
3. In Add Driver dialog box, set Name to Phoenix
4. Press List Drivers button and com.salesforce.phoenix.jdbc.PhoenixDriver should be automatically populated in the Class Name textbox. Press OK to close this dialog.
5. Switch to Alias tab and create the new Alias (Aliases -> New Aliases)
6. In the dialog box, Name: _any name_, Driver: Phoenix, User Name: _anything_, Password: _anything_
7. Construct URL as follows: jdbc:phoenix: _zookeeper quorum server_. For example, to connect to a local HBase use: jdbc:phoenix:localhost
8. Press Test (which should succeed if everything is setup correctly) and press OK to close.
9. Now double click on your newly created Phoenix alias and click Connect. Now you are ready to run SQL queries against Phoenix.

Through SQuirrel, you can issue SQL statements in the SQL tab (create tables, insert data, run queries), and inspect table metadata in the Object tab (i.e. list tables, their columns, primary keys, and types).

![squirrel](http://forcedotcom.github.com/phoenix/images/squirrel.png)

<b> Command Line </b>

In addition, you can use the phoenix-1.0-client.jar to execute SQL and/or load CSV data directly. Here are few examples:

        $ java -jar lib/phoenix-1.0-client.jar localhost examples/stock_symbol.sql
        $ java -jar lib/phoenix-1.0-client.jar localhost examples/stock_symbol.sql examples/stock_symbol.csv
        $ java -jar lib/phoenix-1.0-client.jar -t stock_symbol -h symbol,price,date localhost *.csv

![psql](http://forcedotcom.github.com/phoenix/images/psql.png)

## Maven ##

Currently, Phoenix hosts its own maven repository in github. This is done for convience and will later be moved to a 'real' maven repository. You can add it to your mavenized project by adding the following to your pom:
```
 <repositories>
   ...
   <repository>
      <id>phoenix-github</id>
      <name>Phoenix Github Maven</name>
      <url>http://github.com/forcedotcom/Phoenix/tree/maven-artifacts/</url>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
      <releases>
        <enabled>true</enabled>
      </releases>
    </repository>
    ...
  </repositories>
  
  <dependencies>
    ...
    <dependency>
    	<groupId>com.salesforce</groupId>
 		<artifactId>phoenix</artifactId>
  		<version>1.0-SNAPSHOT</version>
     ...
    </dependency>
```
## Samples ##
The best place to see samples are in our unit tests under test/func/java. These are end-to-end tests demonstrating how to use all aspects of the Phoenix JDBC driver. We also have some examples in the examples directory.

##Mailing List##
Join one or both of our Google groups:

* [Phoenix HBase User](https://groups.google.com/forum/#!forum/phoenix-hbase-user) for users of Phoenix.
* [Phoenix HBase Dev](https://groups.google.com/forum/#!forum/phoenix-hbase-dev) for developers of Phoenix.
