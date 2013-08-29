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

package com.salesforce.phoenix.pig.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.salesforce.phoenix.pig.PhoenixPigConfiguration;

/**
 * 
 * {@link RecordWriter} implementation for Phoenix
 * 
 * @author pkommireddi
 *
 */
public class PhoenixRecordWriter extends RecordWriter<NullWritable, PhoenixRecord> {
	
	private static final Log LOG = LogFactory.getLog(PhoenixRecordWriter.class);
	
	private long numRecords = 0;
	
	private final Connection conn;
	private final PreparedStatement statement;
	private final PhoenixPigConfiguration config;
	private final long batchSize;
	
	public PhoenixRecordWriter(Connection conn, PhoenixPigConfiguration config) throws SQLException {
		this.conn = conn;
		this.config = config;
		this.batchSize = config.getBatchSize();
		this.statement = this.conn.prepareStatement(config.getUpsertStatement());
	}

	/**
	 * Commit only at the end of task. This should ideally reside in an implementation of
	 * {@link OutputCommitter} since that will allow us to rollback in case of task failure.
	 * 
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		try {
			if (conn == null || conn.isClosed()) {
				throw new IOException("Trying to commit a connection that is null or closed: "+ conn);
			}
		} catch (SQLException e) {
			throw new IOException("Exception calling isClosed on connection", e);
		}
		
		LOG.info("commit called during close");
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new IOException("Exception while committing to database." + e);
		}
		try {
			conn.close();
		} catch (SQLException e) {
			throw new IOException("Exception while closing database connection." + e);
		}
	}

	@Override
	public void write(NullWritable n, PhoenixRecord record) throws IOException, InterruptedException {		
		try {
			record.write(statement, config.getColumnMetadataList());
			numRecords++;
			
			if(numRecords % batchSize == 0) {
				LOG.info("commit called on a batch of size : "+batchSize);
				conn.commit();
			}
			
		} catch (SQLException e) {
			throw new IOException("Exception while committing to database.", e);
		}		
	}

}
