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
package com.salesforce.phoenix.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.salesforce.phoenix.pig.hadoop.PhoenixOutputFormat;
import com.salesforce.phoenix.pig.hadoop.PhoenixRecord;

/**
 * StoreFunc that uses Phoenix to store data into HBase.
 * 
 * Example usage: A = load 'testdata' as (a:chararray, b:chararray, c:chararray,
 * d:chararray, e: datetime); STORE A into 'hbase://CORE.ENTITY_HISTORY' using
 * com.salesforce.bdaas.PhoenixHBaseStorage('localhost','-batchSize 5000');
 * 
 * The above reads a file 'testdata' and writes the elements to HBase. First
 * argument to this StoreFunc is the server, the 2nd argument is the batch size
 * for upserts via Phoenix.
 * 
 * Note that Pig types must be in sync with the target Phoenix data types. This
 * StoreFunc tries best to cast based on input Pig types and target Phoenix data
 * types, but it is recommended to supply appropriate schema.
 * 
 * This is only a STORE implementation. LoadFunc coming soon.
 * 
 * @author pkommireddi
 * 
 */
@SuppressWarnings("rawtypes")
public class PhoenixHBaseStorage implements StoreFuncInterface {

	private PhoenixPigConfiguration config;
	private String tableName;
	private RecordWriter<NullWritable, PhoenixRecord> writer;
	private String contextSignature = null;
	private ResourceSchema schema;	
	private long batchSize;
	private final PhoenixOutputFormat outputFormat = new PhoenixOutputFormat();

	// Set of options permitted
	private final static Options validOptions = new Options();
	private final static CommandLineParser parser = new GnuParser();
	private final static String SCHEMA = "_schema";

	private final CommandLine configuredOptions;
	private final String server;

	public PhoenixHBaseStorage(String server) throws ParseException {
		this(server, null);
	}

	public PhoenixHBaseStorage(String server, String optString)
			throws ParseException {
		populateValidOptions();
		this.server = server;

		String[] optsArr = optString == null ? new String[0] : optString.split(" ");
		try {
			configuredOptions = parser.parse(validOptions, optsArr);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("[-batchSize]", validOptions);
			throw e;
		}

		batchSize = Long.parseLong(configuredOptions.getOptionValue("batchSize"));
	}

	private static void populateValidOptions() {
		validOptions.addOption("batchSize", true, "Specify upsert batch size");
	}

	/**
	 * Returns UDFProperties based on <code>contextSignature</code>.
	 */
	private Properties getUDFProperties() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { contextSignature });
	}

	
	/**
	 * Parse the HBase table name and configure job
	 */
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		String prefix = "hbase://";
		if (location.startsWith(prefix)) {
			tableName = location.substring(prefix.length());
		}
		config = new PhoenixPigConfiguration(job.getConfiguration());
		config.configure(server, tableName, batchSize);

		String serializedSchema = getUDFProperties().getProperty(contextSignature + SCHEMA);
		if (serializedSchema != null) {
			schema = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
		}
	}

	@SuppressWarnings("unchecked")
    @Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer =writer;
	}

	@Override
	public void putNext(Tuple t) throws IOException {
        ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();      
        
        PhoenixRecord record = new PhoenixRecord(fieldSchemas);
        
        for(int i=0; i<t.size(); i++) {
        	record.add(t.get(i));
        }
        
		try {
			writer.write(null, record);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
        
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
		return location;
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return outputFormat;
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		schema = s;
		getUDFProperties().setProperty(contextSignature + SCHEMA, ObjectSerializer.serialize(schema));
	}

}