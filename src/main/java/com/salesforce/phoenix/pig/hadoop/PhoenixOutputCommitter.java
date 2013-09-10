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
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.salesforce.phoenix.jdbc.PhoenixStatement;

/**
 * 
 * {@link OutputCommitter} implementation for Pig tasks using Phoenix
 * connections to upsert to HBase
 * 
 * @author pkommireddi
 *
 */
public class PhoenixOutputCommitter extends OutputCommitter {
	private final Log LOG = LogFactory.getLog(PhoenixOutputCommitter.class);
	
	private final PhoenixOutputFormat outputFormat;
	
	public PhoenixOutputCommitter(PhoenixOutputFormat outputFormat) {
		if(outputFormat == null) {
			throw new IllegalArgumentException("PhoenixOutputFormat must not be null.");
		}
		this.outputFormat = outputFormat;
	}

	/**
	 *  TODO implement rollback functionality. 
	 *  
	 *  {@link PhoenixStatement#execute(String)} is buffered on the client, this makes 
	 *  it difficult to implement rollback as once a commit is issued it's hard to go 
	 *  back all the way to undo. 
	 */
	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {
		commit(outputFormat.getConnection(context.getConfiguration()));
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
		return true;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {		
	}

	@Override
	public void setupTask(TaskAttemptContext context) throws IOException {
	}

	/**
	 * Commit a transaction on task completion
	 * 
	 * @param connection
	 * @throws IOException
	 */
	private void commit(Connection connection) throws IOException {
		try {
			if (connection == null || connection.isClosed()) {
				throw new IOException("Trying to commit a connection that is null or closed: "+ connection);
			}
		} catch (SQLException e) {
			throw new IOException("Exception calling isClosed on connection", e);
		}

		try {
			LOG.debug("Commit called on task completion");
			connection.commit();
		} catch (SQLException e) {
			throw new IOException("Exception while trying to commit a connection. ", e);
		} finally {
			try {
				LOG.debug("Closing connection to database on task completion");
				connection.close();
			} catch (SQLException e) {
				LOG.warn("Exception while trying to close database connection", e);
			}
		}
	}
}
