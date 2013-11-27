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
package com.salesforce.phoenix.query;

import java.sql.SQLException;

import com.salesforce.phoenix.schema.TableRef;


/**
 * 
 * Interface for managing and caching table statistics.
 * The frequency of updating the table statistics are controlled
 * by {@link com.salesforce.phoenix.query.QueryServices#STATS_UPDATE_FREQ_MS_ATTRIB}.
 * Table stats may also be manually updated through {@link #updateStats(TableRef)}.
 * 
 *
 * @author jtaylor
 * @since 0.1
 */
public interface StatsManager {
    /**
     * Get the minimum key for the given table
     * @param table the table
     * @return the minimum key or null if unknown
     */
    byte[] getMinKey(TableRef table);
    
    /**
     * Get the maximum key for the given table
     * @param table the table
     * @return the maximum key or null if unknown
     */
    byte[] getMaxKey(TableRef table);
    
    /**
     * Manually update the cached table statistics
     * @param table the table
     * @throws SQLException
     */
    void updateStats(TableRef table) throws SQLException;
}
