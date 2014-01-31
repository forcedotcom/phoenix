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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.salesforce.phoenix.pig.PhoenixPigConfiguration;

/**
 * {@link OutputFormat} implementation for Phoenix
 * 
 * @author pkommireddi
 *
 */
public class PhoenixOutputFormat extends OutputFormat<NullWritable, PhoenixRecord> {
	private static final Log LOG = LogFactory.getLog(PhoenixOutputFormat.class);
	
	private Connection connection;
	private PhoenixPigConfiguration config;

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {		
	}

	/**
	 * TODO Implement {@link OutputCommitter} to rollback in case of task failure
	 */
	
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new PhoenixOutputCommitter(this);
	}

	@Override
	public RecordWriter<NullWritable, PhoenixRecord> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		try {
			return new PhoenixRecordWriter(getConnection(context.getConfiguration()), config);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * This method creates a database connection. A single instance is created
	 * and passed around for re-use.
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	synchronized Connection getConnection(Configuration configuration) throws IOException {
	    if (connection != null) { 
	    	return connection; 
	    }
	    
	    config = new PhoenixPigConfiguration(configuration);	    
		try {
			LOG.info("Initializing new Phoenix connection...");
			connection = config.getConnection();
			LOG.info("Initialized Phoenix connection, autoCommit="+ connection.getAutoCommit());
			return connection;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
}
