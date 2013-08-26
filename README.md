<h1>Phoenix: A SQL skin over HBase<br /> 
<em><sup><sup>'We put the SQL back in NoSQL'</sup></sup></em></h1>
Phoenix is a SQL skin over HBase, delivered as a client-embedded JDBC driver, powering the HBase use cases at Salesforce.com. Phoenix targets low-latency queries (milliseconds), as opposed to batch operation via map/reduce. To see what's supported, go to our [language reference guide](http://forcedotcom.github.com/phoenix/), read more on our [wiki](https://github.com/forcedotcom/phoenix/wiki), and download it [here](https://github.com/forcedotcom/phoenix/wiki/Download).
## Mission
Become the standard means of accessing HBase data through a well-defined, industry standard API.

## Quick Start
Tired of reading already and just want to get started? Jump over to our quick start guide [here](https://github.com/forcedotcom/phoenix/wiki/Phoenix-in-15-minutes-or-less) or map to your existing HBase tables as described [here](https://github.com/forcedotcom/phoenix/wiki#wiki-mapping) and start querying now.

## How It Works ##

The Phoenix query engine transforms your [SQL query](http://forcedotcom.github.com/phoenix/#select) into one or more HBase scans, and orchestrates their execution to produce standard JDBC result sets. Direct use of the HBase API, along with coprocessors and custom filters, results in [performance](https://github.com/forcedotcom/phoenix/wiki/Performance) on the order of milliseconds for small queries, or seconds for tens of millions of rows. 

Tables are created and altered through [DDL statements](http://forcedotcom.github.com/phoenix/#create), and their schema is stored and versioned on the server in an HBase table. Columns are defined as either being part of a multi-part row key, or as key/value cells. You can also map Phoenix on to existing tables (see the [wiki](https://github.com/forcedotcom/phoenix/wiki) for more details).

Applications interact with Phoenix through a standard JDBC interface; all the usual interfaces are supported, including `Connection`, `Statement`, `PreparedStatement`, and `ResultSet`. The driver class is `com.salesforce.phoenix.jdbc.PhoenixDriver`, JDK 1.5+ automatically registers JDBC driver on classpath, and the [connection url](https://github.com/forcedotcom/phoenix/wiki#wiki-connStr) is `jdbc:phoenix:` followed by the zookeeper quorum hostname specification plus optionally the port number and/or root node. For example:

        Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost");

For detailed documentation on the current level of SQL support, see our [language reference guide](http://forcedotcom.github.com/phoenix/). For details about how Phoenix handles schema, transactions, and more, see the [wiki](https://github.com/forcedotcom/phoenix/wiki).

## System Requirements ##
* HBase v 0.94.4 or above
* JDK 6 or higher

## Build Requirements ##
* All the system requirements
* Maven 3.X (https://maven.apache.org/)


## Installation ##
To install a pre-built phoenix, use these directions:

* Download and expand the latest phoenix-[version]-install.tar from [download page](https://github.com/forcedotcom/phoenix/wiki/Download)
* Remove prior phoenix-[version].jar from every HBase region server.
* Add the phoenix-[version].jar to the classpath of every HBase region server. An easy way to do this is to copy it into the HBase lib directory.
* Restart all region servers.
* Remove prior phoenix-[version]-client.jar from the classpath of any Phoenix client.
* Add the phoenix-[version]-client.jar to the classpath of any Phoenix client.

Alternatively, you can build it yourself using maven by following these [build instructions](https://github.com/forcedotcom/Phoenix/wiki#wiki-building).


## Getting Started ##

<h4>Command Line</h4>

A terminal interface to execute SQL from the command line is now bundled with Phoenix v 1.2. To
start it, execute the following from the bin directory:

	$ sqlline.sh localhost

To execute SQL scripts from the command line, you can include a SQL file argument like this:

	$ sqlline.sh localhost ../examples/stock_symbol.sql

![sqlline](http://forcedotcom.github.com/phoenix/images/sqlline.png)

For more information, see the [manual](http://www.hydromatic.net/sqlline/manual.html).

<h5>Loading Data</h5>

In addition, you can use the bin/psql.sh to execute to load CSV data or execute SQL scripts. For example:

        $ psql.sh localhost ../examples/web_stat.sql ../examples/web_stat.csv ../examples/web_stat_queries.sql

<h4>SQL Client</h4>

If you'd rather use a client GUI to interact with Phoenix, download and install [SQuirrel](http://squirrel-sql.sourceforge.net/). Since Phoenix is a JDBC driver, integration with tools such as this are seamless. Here are the setup steps necessary:

1. Remove prior phoenix-[version]-client.jar from the lib directory of SQuirrel
2. Copy the phoenix-[version]-client.jar into the lib directory of SQuirrel (Note that on a Mac, this is the *internal* lib directory).
3. Start SQuirrel and add new driver to SQuirrel (Drivers -> New Driver)
4. In Add Driver dialog box, set Name to Phoenix
5. Press List Drivers button and com.salesforce.phoenix.jdbc.PhoenixDriver should be automatically populated in the Class Name textbox. Press OK to close this dialog.
6. Switch to Alias tab and create the new Alias (Aliases -> New Aliases)
7. In the dialog box, Name: _any name_, Driver: Phoenix, User Name: _anything_, Password: _anything_
8. Construct URL as follows: jdbc:phoenix: _zookeeper quorum server_. For example, to connect to a local HBase use: jdbc:phoenix:localhost
9. Press Test (which should succeed if everything is setup correctly) and press OK to close.
10. Now double click on your newly created Phoenix alias and click Connect. Now you are ready to run SQL queries against Phoenix.

Through SQuirrel, you can issue SQL statements in the SQL tab (create tables, insert data, run queries), and inspect table metadata in the Object tab (i.e. list tables, their columns, primary keys, and types).

![squirrel](http://forcedotcom.github.com/phoenix/images/squirrel.png)

## Maven ##

Currently, Phoenix hosts its own maven repository in github. This is done for convience and will later be moved to a 'real' maven repository. You can add it to your mavenized project by adding the following to your pom:
```
 <repositories>
   ...
   <repository>
      <id>phoenix-github</id>
      <name>Phoenix Github Maven</name>
      <url>https://raw.github.com/forcedotcom/phoenix/maven-artifacts/releases</url>
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
        <version>2.0.0</version>
    </dependency>
    ...
  </dependencies>
```
## Samples ##
The best place to see samples are in our unit tests under src/test/java. The ones in the endToEnd package are tests demonstrating how to use all aspects of the Phoenix JDBC driver. We also have some examples in the examples directory.

##Mailing List##
Join one or both of our Google groups:

* [Phoenix HBase User](https://groups.google.com/forum/#!forum/phoenix-hbase-user) for users of Phoenix.
* [Phoenix HBase Dev](https://groups.google.com/forum/#!forum/phoenix-hbase-dev) for developers of Phoenix.

and follow the Phoenix blog [here](http://phoenix-hbase.blogspot.com/).

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/8438f3b844208e09a811699265666a8d "githalytics.com")](http://githalytics.com/forcedotcom/phoenix.git)

