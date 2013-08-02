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
import java.sql.*;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.pig.*;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ColumnInfo;
import com.salesforce.phoenix.util.QueryUtil;

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

	private List<ColumnInfo> columnMetadataList = new LinkedList<ColumnInfo>();

	private static final Log LOG = LogFactory.getLog(PhoenixHBaseStorage.class);

	private String tableName;
	private PreparedStatement statement;
	private final String server;
	private PhoenixConnection conn;
	
	private int batchSize;
	private int rowCount;

	// Set of options permitted
	private final static Options validOptions = new Options();
	private final CommandLine configuredOptions;
	private final static CommandLineParser parser = new GnuParser();
	
	private String contextSignature = null;
	private ResourceSchema schema;
	private static final String SCHEMA = "_schema";

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

		rowCount = 0;
		batchSize = Integer.parseInt(configuredOptions.getOptionValue("batchSize"));
	}

	private static void populateValidOptions() {
		validOptions.addOption("batchSize", true, "Specify upsert batch size");
	}

	/**
	 * Returns UDFProperties based on <code>contextSignature</code>.
	 */
	private Properties getUDFProperties() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] { contextSignature });
	}

	
	/**
	 * Parse the HBase table name and configure job
	 */
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		if (location.startsWith("hbase://")) {
			tableName = location.substring(8);
		}

		Configuration conf = job.getConfiguration();
		PhoenixPigConfiguration.configure(conf);
		
        String serializedSchema = getUDFProperties().getProperty(contextSignature + SCHEMA);
        if (serializedSchema!= null) {
            schema = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
        }

	}

	/**
	 * This method gets called before putNext(Tuple t). Initialize
	 * driver/connections here
	 */
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		try {
			Properties props = new Properties();
			conn = DriverManager.getConnection(QueryUtil.getUrl(server), props).unwrap(PhoenixConnection.class);
			// Default to config defined upsert batch size if user did not specify it.
			batchSize = batchSize <= 0 ? conn.getMutateBatchSize() : batchSize;
			String[] tableMetadata = getTableMetadata(tableName);
			ResultSet rs = conn.getMetaData().getColumns(null, tableMetadata[0], tableMetadata[1], null);
			while (rs.next()) {
				columnMetadataList.add(new ColumnInfo(rs.getString(QueryUtil.COLUMN_NAME_POSITION), rs.getInt(QueryUtil.DATA_TYPE_POSITION)));
			}
			
			// Generating UPSERT statement without column name information.
			String upsertStmt = QueryUtil.constructUpsertStatement(null, tableName, columnMetadataList.size());
			LOG.info("Phoenix Upsert Statement: " + upsertStmt);
			statement = conn.prepareStatement(upsertStmt);

		} catch (SQLException e) {
			LOG.error("Error in constructing PreparedStatement: " + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		Object upsertValue = null;
        ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();      
        
        
		try {
			for (int i = 0; i < columnMetadataList.size(); i++) {
				Object o = t.get(i);
				byte type = (fieldSchemas == null) ? DataType.findType(t.get(i)) : fieldSchemas[i].getType();
				upsertValue = convertTypeSpecificValue(o, type, columnMetadataList
						.get(i).getSqlType());

				if (upsertValue != null) {
					statement.setObject(i + 1, upsertValue, columnMetadataList
							.get(i).getSqlType());
				} else {
					statement.setNull(i + 1, columnMetadataList.get(i)
							.getSqlType());
				}
			}

			statement.execute();
			// Commit when batch size is reached
			if(++rowCount % batchSize == 0) {
				conn.commit();
				LOG.info("Rows upserted: "+rowCount);
			}
		} catch (SQLException e) {
			LOG.error("Error during upserting to HBase table "+tableName, e);
		}

	}

	private Object convertTypeSpecificValue(Object o, byte type, Integer sqlType) {
		PDataType pDataType = PDataType.fromSqlType(sqlType);

		return TypeUtil.castPigTypeToPhoenix(o, type, pDataType);
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
        // Commit the remaining executes after the last commit in putNext()
        if (rowCount % batchSize != 0) {
            try {
                conn.commit();
                LOG.info("Rows upserted: " + rowCount);
            } catch (SQLException e) {
                LOG.error("Error during upserting to HBase table " + tableName, e);
            }
        }
    }

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return location;
	}

	/**
	 * This method sets {@link OutputFormat} to be used while writing data. Note
	 * we do not need an OutputFormat as Phoenix encapsulates the writes to
	 * HBase. Pig needs an OutputFormat to be specified, using
	 * {@link NullOutputFormat}
	 */
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new NullOutputFormat();
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		schema = s;
		getUDFProperties().setProperty(contextSignature + SCHEMA,
				ObjectSerializer.serialize(schema));
	}

	private String[] getTableMetadata(String table) {
		String[] schemaAndTable = table.split("\\.");
		assert schemaAndTable.length >= 1;

		if (schemaAndTable.length == 1) {
			return new String[] { "", schemaAndTable[0] };
		}

		return new String[] { schemaAndTable[0], schemaAndTable[1] };
	}
}