package com.salesforce.hbase.index.builder;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.salesforce.hbase.index.IndexUtil;
import com.salesforce.hbase.index.Indexer;

/**
 * Interface to build updates ({@link Mutation}s) to the index tables, based on the primary table
 * updates.
 * <p>
 * Either all the index updates will be applied to all tables or the primary table will kill itself
 * and will attempt to replay the index edits through the WAL replay mechanism.
 */
public interface IndexBuilder {

  /**
   * This is always called exactly once on install of {@link Indexer}, before any calls
   * {@link #getIndexUpdate} on
   * @param conf {@link Configuration} containing any properties specified in
   *          {@link IndexUtil#enableIndexing(HTableDescriptor, Class, Map)}
   */
  public void setup(Configuration conf);

  /**
   * Your opportunity to update any/all index tables based on the delete of the primary table row.
   * Its up to your implementation to ensure that timestamps match between the primary and index
   * tables.
   * @param put {@link Put} to the primary table that may be indexed
   * @return a Map of the mutations to make -> target index table name
   */
  public Map<Mutation, String> getIndexUpdate(Put put);

  /**
   * The counter-part to {@link #getIndexUpdate(Put)} - your opportunity to update any/all index
   * tables based on the delete of the primary table row. Its up to your implementation to ensure
   * that timestamps match between the primary and index tables.
   * @param delete {@link Delete} to the primary table that may be indexed
   * @return a {@link Map} of the mutations to make -> target index table name
   */
  public Map<Mutation, String> getIndexUpdate(Delete delete);

  /** Helper method signature to ensure people don't attempt to extend this class directly */
  public void extendBaseIndexBuilderInstead();
}
