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
package com.salesforce.phoenix.coprocessor;

import java.io.*;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableImpl;
import com.salesforce.phoenix.util.MetaDataUtil;


/**
 * 
 * Coprocessor protocol for Phoenix DDL. Phoenix stores the table metadata in
 * an HBase table named SYSTEM.TABLE. Each table is represented by:
 * - one row for the table
 * - one row per column in the tabe
 * Upto {@link #DEFAULT_MAX_META_DATA_VERSIONS} versions are kept. The time
 * stamp of the metadata must always be increasing. The timestamp of the key
 * values in the data row corresponds to the schema that it's using.
 *
 * TODO: dynamically prune number of schema version kept based on whether or
 * not the data table still uses it (based on the min time stamp of the data
 * table).
 * 
 * @author jtaylor
 * @since 0.1
 */
public interface MetaDataProtocol extends CoprocessorProtocol {
    public static final int PHOENIX_MAJOR_VERSION = 2;
    public static final int PHOENIX_MINOR_VERSION = 0;
    public static final int PHOENIX_PATCH_NUMBER = 0;
    public static final int PHOENIX_VERSION = 
            MetaDataUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER);
    
    public static final long MIN_TABLE_TIMESTAMP = 0;
    // Increase MIN_SYSTEM_TABLE_TIMESTAMP by one for each schema change SYSTEM.TABLE schema changes.
    // For 1.0,1.1,1.2,and 1.2.1 we used MetaDataProtocol.MIN_TABLE_TIMESTAMP+1
    // For 2.0 and above, we use MetaDataProtocol.MIN_TABLE_TIMESTAMP+5 so that we can add the three new
    // columns to the existing system table and see all these changes
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP = MIN_TABLE_TIMESTAMP + 5;
    public static final int DEFAULT_MAX_META_DATA_VERSIONS = 1000;

    public enum MutationCode {
        TABLE_ALREADY_EXISTS,
        TABLE_NOT_FOUND, 
        COLUMN_NOT_FOUND, 
        COLUMN_ALREADY_EXISTS,
        CONCURRENT_TABLE_MUTATION,
        TABLE_NOT_IN_REGION,
        NEWER_TABLE_FOUND,
        UNALLOWED_TABLE_MUTATION,
        NO_PK_COLUMNS,
        PARENT_TABLE_NOT_FOUND 
    };
    
    public static class MetaDataMutationResult implements Writable {
        private MutationCode returnCode;
        private long mutationTime;
        private PTable table;
        
        public MetaDataMutationResult() {
        }

        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table) {
            this.returnCode = returnCode;
            this.mutationTime = currentTime;
            this.table = table;
        }
        
        public MutationCode getMutationCode() {
            return returnCode;
        }
        
        public long getMutationTime() {
            return mutationTime;
        }
        
        public PTable getTable() {
            return table;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            this.returnCode = MutationCode.values()[WritableUtils.readVInt(input)];
            this.mutationTime = input.readLong();
            boolean hasTable = input.readBoolean();
            if (hasTable) {
                this.table = new PTableImpl();
                this.table.readFields(input);
            }
        }

        @Override
        public void write(DataOutput output) throws IOException {
            WritableUtils.writeVInt(output, returnCode.ordinal());
            output.writeLong(mutationTime);
            output.writeBoolean(table != null);
            if (table != null) {
                table.write(output);
            }
        }
    }
    
    /**
     * The the latest Phoenix table at or before the given clientTimestamp. If the
     * client already has the latest (based on the tableTimestamp), then no table
     * is returned.
     * @param schemaName
     * @param tableName
     * @param tableTimestamp
     * @param clientTimestamp
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult getTable(byte[] schemaName, byte[] tableName, long tableTimestamp, long clientTimestamp) throws IOException;

    /**
     * Create a new Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult createTable(List<Mutation> tableMetadata) throws IOException;

    /**
     * Drop an existing Phoenix table
     * @param tableMetadata
     * @param tableType
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult dropTable(List<Mutation> tableMetadata, String tableType) throws IOException;

    /**
     * Add a column to an existing Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult addColumn(List<Mutation> tableMetadata) throws IOException;
    
    /**
     * Drop a column from an existing Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult dropColumn(List<Mutation> tableMetadata) throws IOException;
    
    MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata) throws IOException;

    /**
     * Clears the server-side cache of table meta data. Used between test runs to
     * ensure no side effects.
     * 
     * @throws IOException
     */
    void clearCache();
    
    /**
     * Get the version of the server-side HBase and phoenix.jar. Used when initially connecting
     * to a cluster to ensure that the client and server jars are compatible.
     * 
     * @throws IOException
     */
    long getVersion();
}
