/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Ensure that the log isn't rolled while we are the in middle of doing a pending index write.
 * <p>
 * The problem we are trying to solve is the following sequence:
 * <ol>
 * <li>Write to the indexed table</li>
 * <li>Write the index-containing WALEdit</li>
 * <li>Start writing to the index tables in the postXXX hook</li>
 * <li>WAL gets rolled and archived</li>
 * <li>An index update fails, in which case we should kill ourselves to get WAL replay</li>
 * <li>Since the WAL got archived, we won't get the replay of the index writes</li>
 * </ol>
 * <p>
 * The usual course of events should be:
 * <ol>
 * <li>In a preXXX hook,
 * <ol>
 * <li>Build the {@link WALEdit} + index information</li>
 * <li>Lock the {@link IndexLogRollSynchronizer#INDEX_UPDATE_LOCK}</li>
 * <ul>
 * <li>This is a reentrant readlock on the WAL archiving, so we can make multiple WAL/index updates
 * concurrently</li>
 * </ul>
 * </li>
 * </ol>
 * </li>
 * <li>Pass that {@link WALEdit} to the WAL, ensuring its durable and replayable</li>
 * <li>In the corresponding postXXX,
 * <ol>
 * <li>make the updates to the index tables</li>
 * <li>Unlock {@link IndexLogRollSynchronizer#INDEX_UPDATE_LOCK}</li>
 * </ol>
 * </li> </ol>
 * <p>
 * <tt>this</tt> should be added as a {@link WALActionsListener} by updating
 */
public class IndexLogRollSynchronizer implements WALActionsListener {

  private static final Log LOG = LogFactory.getLog(IndexLogRollSynchronizer.class);
  private WriteLock logArchiveLock;

  public IndexLogRollSynchronizer(WriteLock logWriteLock){
    this.logArchiveLock = logWriteLock;
  }


  @Override
  public void preLogArchive(Path oldPath, Path newPath) throws IOException {
    //take a write lock on the index - any pending index updates will complete before we finish
    LOG.debug("Taking INDEX_UPDATE writelock");
    logArchiveLock.lock();
    LOG.debug("Got the INDEX_UPDATE writelock");
  }
  
  @Override
  public void postLogArchive(Path oldPath, Path newPath) throws IOException {
    // done archiving the logs, any WAL updates will be replayed on failure
    LOG.debug("Releasing INDEX_UPDATE writelock");
    logArchiveLock.unlock();
  }

  @Override
  public void logCloseRequested() {
    // don't care- before this is called, all the HRegions are closed, so we can't get any new
    // requests and all pending request can finish before the WAL closes.
  }

  @Override
  public void preLogRoll(Path oldPath, Path newPath) throws IOException {
    // noop
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
    // noop
  }

  @Override
  public void logRollRequested() {
    // noop
  }

  @Override
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit) {
    // noop
  }

  @Override
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
    // noop
  }
}