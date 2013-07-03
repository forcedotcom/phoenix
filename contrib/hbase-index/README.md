# hbase-index
=============

A general table-level indexing framework for HBase. This is a set of pieces that when combined together enable you to do 'HBase consistent' secondary indexing.

## How it works
===============

We attempt to be completely transparent to the client, providing all the indexing functionality on the server side via a combination of coprocessors and WALEdit manipulation.

### Writing

When you make an edit from the client, we catch it on the region via a coprocessor and check to see if it should be indexed. If so, we then write a custom WAL entry that captures both the primary edit as well as all the secondary index edits. From this point on, your edit and its index entries are considered durable - just like usual. 

Once we are durable, the edit goes through the regular processing on the primary table. At the same time, we also make the index update to the index table(s). Either this edit must succeed or the region to which we are attempting to write is unavailable. If we can't write to an index, we kill the server (System.exit) - this ensures that we always write the index entry and don't get too far behind on the primary table vs. the index(1).

### Reading

When reading from an index table, there is no explicit guarantee of consistent across servers (acID-like semantics), so the best thing to do its to read _as of a timestamp_, ensuring that you get close to when the edit in the primary table occurs. In the usual (non-failure) case, there is very little time difference between the primary and index tables; you get a couple milliseconds as we deal with network overhead, but there is very little else slowing things down in the usual operation flow between when the primary and index puts are visible.

(1) We could use a 'invalid index' indicator, but then again has to live somewhere (another HBase table?) which has the same failure considerations, so its not really worth the extra complexity for what is really a relatively small chance of increased availabilty. 

## HBase Consistent
========

HBase only guarantees consistency on a per-row, per-table basis. Therefore, its up to you to maintain consistency if you want to write across two different tables.
hbase-index provides this consistency guarantee by hacking the HBase Write-Ahead Log (WAL) to ensure that secondary index entries always get written if the primary
table write succeeds.

## Caveats
==========

There are no guarantees of:

 - serializability
  - two edits may occur on the primary table and their index entries may be written out of order.
	- We resolve this within the HBase model by ensuring that index entries timestamp always matches the primary table edit timestamp.

## Usage
=========

For the general Put/Delete case (the only operations currently supported), you don't need to change anything in the usual update path. However, there are a couple of things that you will need to change when setting up your cluster and tables. 

### Jars

You will need to put the class jar for your desired version of hbase-index on the hbase classpath. Internally, we employ a RegionObserver coprocessor as well as a custom HLog Reader, both of which need to be available to HBase on startup.

### hbase-site.xml changes

You will need to add the following to your hbase-site.xml:
```
<property>
	<name>hbase.regionserver.hlog.reader.impl</name>
	<value>org.apache.hadoop.hbase.regionserver.wal.IndexedHLogReader</value>
</property>
```

* NOTE: The IndexedHLogReader does *NOT support compressed WAL Edits*, so you will need to ensure that "hbase.regionserver.wal.enablecompression" is set to false.

#### Supporting Indexing with Compressed WAL

HBase >= 0.94.9 added support for a pluggable WALEditCodec (mainly [HBASE-8636](https://issues.apache.org/jira/browse/HBASE-8636)) which we leverage to provide full indexing support with WAL Compression enabled.

The only thing you need to add is the following property to hbase-site:

```
<property>
	<name>hbase.regionserver.wal.codec</name>
	<value>org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec</value>
</property>
```
And also put the hbase-index-0.94.9-compat.jar on the HBase classpath on startup.

This supports both compressed *and* uncompressed WAL. So feel free to toggle:

```
<property>
	<name>hbase.regionserver.wal.enablecompression</name>
	<value>true</value>
</property>
```

### Note
 Moving to 0.94.9 with WAL Compression requires a clean shutdown of the cluster - no WALs can be left over to replay
 when the cluster comes back online. Our custom WALEditCodec - the IndexedWALEditCodec - is *not* backwards compatible
 with the indexing _if compression is enabled_. If compression is not enabled, moving to the codec from the IndexedHLogReader
 will be fine.
 
 _This means that if you are using the IndexedWALEditCodec - HBase 0.94.9+ - you must do a clean restart (no remaining WALs)
 of the cluster when switching between compressed and uncompressed WAL_.


## Custom Indexing
===================

hbase-index has a built-in concept of an IndexBuilder that lets you create custom index entries based on the primary table edits. You only need to implement a com.salesforce.hbase.index.builder.IndexBuilder; actually, you should subclass BaseIndexBuilder for cross-version compatability - not doing so voids your warranty with respect to upgrades.

Then, you just need setup the Indexer to use your custom builder by setting up the table via IndexUtil#enableIndexing(). The enableIndexing() method ensures that your custom IndexBuilder is used by the indexer for the table and that your custom options are available to your IndexBuilder on the server-side.

There is an example index builder, ColumnFamilyIndexer, that just indexes on column family. You can enable it on a table via ColumnFamilyIndexer#enableIndexing(), which internally will call IndexUtil#enableIndexing() and then setup the target index table(s) via ColumnFamilyIndexer#createIndexTable(). See TestEndtoEndIndexing for a thorough example.

## Requirements
===============

* Java 1.6.0_34 or higher
* HBase-0.94.[0..5, .9]
 - 0.94.6 has a bug in the configuration creation that mean default table references from coprocessors don't work [HBASE-8684](https://issues.apache.org/jira/browse/HBASE-8684)
 - 0.94.7 breaks the RegionServerServices WAL accessing interface
 - 0.94.9: has all the necessary bug fixes AND provides the interface to support indexing with a compressed WAL

### If building from source
* All of the above
* Maven 3.X

## Building from source
=======================

### Building the Jars
For HBase < 0.94.9
```
    $ mvn clean install -DskipTests
```
For HBase >= 0.94.9
```
    $ mvn clean install -DskipTests -Dhbase=0.94.9
```
This will build the necessary jars in the index-core/target directory (and if using -Dhbase=0.94.9, the hbase-0.94.9-compact/target directory).

### Running the tests

To just run the index-core tests, you can do:
```
    $ mvn clean test
```
This runs the tests against HBase 0.94.4 and does not support WAL Compression.

To run the tests against 0.94.9, run:
```
    $ mvn clean install -DskipTests
    $ mvn clean test -Dhbase=0.94.9
```

The first step ensures that the index-core jar is present in the local repository as the hbase-0.94.9 compatibility module requires the the index-core test-jar (and maven isn't smart enough to realize that when doing compilation, so we have to go through this extra step). 

## Roadmap/TODOs
=======
 - Support alternative failure mechansims.
 	- The 'abort the server' mechanism is a bit heavy handed and decreases the robustness of the system in the face of transitive errors. A possible mechanism would be an 'index invalidator' that marks an index as invalid after a certain number of failures. 
 - Investigate client-written index updates. 
 	- By have the region manage all the updates, it adds a lot more CPU and bandwidth load on an already delicate resource. This mechanism would still serialize the index updates to the WAL, but let the client ensure that the index updates are written to the index table. Only when the client fails to make the index updates (either via timeout or explicitly saying so) do we go into the failure + replay situation. This gets particularly tricky when managing rolling the at WAL - we cannot roll until the index updates have been marked complete, meaning we may need to block incoming requests as well as wait on all outstanding index updates. This adds a lot more complexity for what seems to a potentially modest performance upgrade, but may be worth it in some situations.
 - Support Append, Increment operations
	- These follow a slightly different path through the HRegion that don't make them as amenable to WALEdit modification. This will likely require some changes to HBase, but will be technically very similar to the Put and Delete implementations.
 - Cleaner HTableInterface reference management in Indexer
 	- right now, its a little heavy-handed, creating a new set of HTables for each index request (in fact, each Put/Delete). Ideally, we would want to use some sort of time-based, LRU(ish) cache to keep track of the HTables; you don't want to keep open connections around that aren't being regularly used, but you don't want to throw away regularly used tables (so a strict, single size LRU could easily start to thrash).
- (Possible) Look into supporting multiple WALs as there is now a per-region WAL in hbase-0.94.6
 	- this is part of a bigger issue with supporting multiple releases of HBase with different internals
