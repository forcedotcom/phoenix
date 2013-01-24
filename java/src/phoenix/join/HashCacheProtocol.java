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
package phoenix.join;

import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * 
 * EndPoint coprocessor to send the table being cached in a hash join to a region server.
 * Used in conjunction with {@link HashJoiningRegionObserver} to perform a hash join.
 *
 * @author jtaylor
 * @since 0.1
 */
public interface HashCacheProtocol extends CoprocessorProtocol {
    /**
     * Add the table being cached in a hash join to the region server attributes.  One side
     * of the join is queried in advance and cached on each server for quicker retrievals. 
     * @param tenantId the tenantId or null if not applicable
     * @param joinId unique identifier of a hash join between two tables
     * @param hashCache binary representation of table being cached
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean addHashCache(byte[] tenantId, byte[] joinId, ImmutableBytesWritable hashCache) throws SQLException;
    /**
     * Remove the cached table from the region server attributes.  Called upon completion of
     * hash join when cache is no longer needed.
     * @param tenantId the tenantId or null if not applicable
     * @param joinId unique identifier of a hash join between two tables
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean removeHashCache(byte[] tenantId, byte[] joinId) throws SQLException;
}
