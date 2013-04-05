package com.salesforce.hbase.stats.serialization;


/**
Overview of Statistics Serialization
<p>
<ul>
<li><a href="#overview">Overview</a></li>
<li><a href="#reading">Reading the StatisticsTable</a></li>
<li><a href="#enable>Enabling Statistics</a></li>
</ul>
</p>
<h2><a name="overview">Overview</a></h2> These are the various pieces necessary to serialize a
statistic to a list of {@link KeyValue}s. Right now, this is implemented with a single
serialization order in the row key:

<pre>
table | statistic name | region | column family
</pre>

where table, region and column family are in reference to the source table.
<p>
This lets you easily aggregate a a single statistics over a given region or quickly access a
single statistic for a given column in a given region.
<p>
For cases you you want to know a statistic for a single family, but across all regions, you would
need to do the same scan as in the above case, but filter out other columns, which can be
inefficient, but isn't a killer because we won't have <i>that many</i> stores (perhaps on the
order of several thousand across all regions).
<p>
We could extend this serialization to be more flexible (different key-part ordering for different
statistics based on desired access patterns), but this is orders of magnitude simpler.
<h2><a name="reading">Reading the StatisticsTable</a></h2> Some statistics can be read directly
from the statistics table since they are just simple point values. For instance, the
{@link com.salesforce.hbase.stats.impl.EqualDepthHistogramStatisticTracker} can be read using a
simple
{@link org.apache.hadoop.hbase.statistics.serialization.IndividualStatisticReader.HistogramStatisticReader}
. like this:
<pre>

</pre>
Other statistics have a slightly more complicated internal structure - i.e the use multiple
column qualifiers - and should provide a special reader. For instance,
{@link com.salesforce.hbase.stats.impl.MinMaxKey} provides a custom reader than can be used like:

<pre>
 StatisticReader&lt;StatisticValue&gt; reader = MinMaxKey.getStatistcReader(primary);
 StatisticsTable statTable = new StatisticsTable(UTIL.getConfiguration(), primary);
 List&lt;MinMaxStat&gt; results = MinMaxKey.interpret(statTable.read(reader));
</pre>
 */