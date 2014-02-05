# hbase-stat
=============

A simple statistics package for HBase.

## Goal
========

Provide reasonable approximations (exact, if we can manage it) of statistical information about a table in HBase with minimal muss and fuss.

We want to make it easy to gather statistics about your HBase tables - there should be little to no work on the users part beyond ensuring that the right things are setup.

## Usage
=========

###Cluster setup

The only changes that need to be made to a generic configuration for a cluster is to add the RemoveTableOnDelete coprocessor to the list of Master Observers. This coprocessor cleans up the statistics for a table on delete, if that table has statistics 'enbabled' (see below). You should only need to add the following to your hbase-site.xml:

```
<property>
	<name>hbase.coprocessor.master.classes</name>
	<value>com.salesforce.hbase.stats.cleanup.RemoveTableOnDelete</value>
</property>
```

### Table creation

All the work for gathering and cleaning statistics is handled via coprocessors. Generally, each statistic will have its own static methods for adding the coprocessor to the table (if not provided, the HTableDesriptor#addCoprocessor() method should suffice). For instance, to add the MinMaxKey statistic to a table, all you would do is:

```java
	HTableDescriptor primary = …
	MinMaxKey.addToTable(primary)
```

At the very least, you should ensure that the table is created with the com.salesforce.hbase.stats.cleanup.RemoveRegionOnSplit coprocessor to ensure that when a region is removed (via splits or merges) that the stats for that region are also removed. This can be added manually (no recommended) or via the general setup table utility:

```java
    HTableDescriptor primary = new HTableDescriptor("primary");
    primary.addFamily(new HColumnDescriptor(FAM));

    // ...
    //Add your own stats here
    //...

    // setup the stats table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    //ensure statistics are enabled and the cleanup coprocessors setup
    SetupTableUtil.setupTable(admin, primary, true, false);
```

#### SetupTableUtil

In addition to settting up the cleanup coprocessors, the SetupTableUtil sets the 'stats enabled' flag in the primary table's descriptor. If this flag is enabled, the cleanup coprocessors (RemoveTableOnDelete and RemoveRegionOnSplit) will be enabled for the table

 * NOTE: if the cleanup coprocessors are not added to the table, setting the 'stats enabled' flag manually won't do anything. However, if you manually add the cleanup coprocessors, but don't enable stats on the descriptor, again, no cleanup will take place. It's highly recommended to use the SetupTableUtil to ensure you don't forget either side.

You will also note that the SetupTableUtil has an option to ensure that the Statistics table is setup, *its highly recommneded that you use this option* to avoid accidentially forgetting and not having a statistics table when you go to write out statistics. With the wrong write configurations in hbase-site.xml, this could cause the statitic coprocessors to each block until they realize the table doesn't exist.

To use it, you simply do the same as above, but ensure that the "ensureStatsTable" boolean flag is set:

```java
    SetupTableUtil.setupTable(admin, primary, true /* this flag! */, false);
```

### Reading Statistics

Since statistics are kept in a single HTable, you could go any manually read them. However, each statistic could potentially have its own serialization and layout. Therefore, its recommended to the the StatisticReader to read a StatisticTable. Generally, all you will need to provide the StatisticReader is the type of statistic (Point or Histogram), name of the statistic and the underlying table. For instance, to read a histogram statistic "histo" for all the regions (and all the column families) of a table, you would do:

```java
	StatisticsTable stats = …
	StatiticReader reader = new StatisticReader(stats, new HistogramStatisticDeserializer(), "histo");
	reader.read()
```

However, this is a bit of a pain as each statistic will have its own name and type. Therefore, the standard convention is for each StatisticTracker to provide a getStatisticReader(StatisticTable) method to read that statistic from the table. For instance, to read the EqualWidthHistogramStatistic, all you need to do is:

```java
	StatisticsTable stats = …
	StatiticReader reader = EqualWidthHistogramStatistic.getStatisticsReader(stats);
	reader.read();
```

Some statistics are a little more complicated in the way they store their information, for instance using different column qualifiers at the same time to store different parts of the key. Generally, these should provide their own mechanisms to rebuild a stat from the serialized information. For instance, MinMaxKey provides an interpret method:

```java
    StatisticsTable stats = …
    StatisticReader<StatisticValue> reader = MinMaxKey.getStatistcReader(stats);
    List<MinMaxStat> results = MinMaxKey.interpret(reader.read());
```


### Statistics Table Schema
===========================

The schema was inspired by OpenTSDB (opentsdb.net) where each statistic is first grouped by table, then region. After that, each statistic (MetricValue) is grouped by:
	* type
	* info
	** this is like the sub-type of the metric to help describe the actual type. For instance, on a min/max for the column, this could be 'min'
	* value

Suppose that we have a table called 'primary' with column 'col' and we are using the MinMaxKey statistic. Assuming the table has a single region, entries in the statistics table will look something like:

```
|           Row             | Column Family | Column Qualifier | Value 
|  primary<region name>col  |     STAT      |   max_region_key |  10  
|  primary<region name>col  |     STAT      |   min_region_key |  3
```

This is because the MinMaxKey statistic uses the column name (in this case 'col') as the type, we use the only CF on the stats table (STATS) and have to subtypes - info - elements: max_region_key and min_region_key, each with associated values.

## Requirements
===============

* Java 1.6.0_34 or higher
* HBase-0.94.5 or higher

### If building from source
* Maven 3.X


## Building from source
=======================

From the base (hbase-stat) directory…

To run tests

    $ mvn clean test
    
To build a jar

    $ mvn clean package

and then look in the target/ directory for the build jar

## Roadmap / TODOs
==================
 - Switch statistic cleanup to use a coprocessor based delete
 	- we want to delete an entire prefix, but that first requires doing a scan and then deleting everything back from the scan
 	
 - Enable more fine-grained writing of statistics so different serialization mechanisms can be inserted.
 	- most of the plumbing is already there (StatisticReader/Writer), but need to work it into the cleaner mechanisms
