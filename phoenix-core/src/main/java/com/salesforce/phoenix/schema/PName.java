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
package com.salesforce.phoenix.schema;

import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.ByteUtil;


/**
 * 
 * Interface to encapsulate both the client-side name
 * together with the server-side name for a named object
 *
 * @author jtaylor
 * @since 0.1
 */
public interface PName {
    public static PName EMPTY_NAME = new PName() {
        @Override
        public String getString() {
            return "";
        }

        @Override
        public byte[] getBytes() {
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        
        @Override
        public String toString() {
            return getString();
        }
    };
    public static PName EMPTY_COLUMN_NAME = new PName() {
        @Override
        public String getString() {
            return QueryConstants.EMPTY_COLUMN_NAME;
        }

        @Override
        public byte[] getBytes() {
            return QueryConstants.EMPTY_COLUMN_BYTES;
        }
        
        @Override
        public String toString() {
            return getString();
        }
    };
    /**
     * Get the client-side, normalized name as referenced
     * in a SQL statement.
     * @return the normalized string name
     */
    String getString();
    
    /**
     * Get the server-side name as referenced in HBase-related
     * APIs such as Scan, Filter, etc.
     * @return the name as a byte array
     */
    byte[] getBytes();
}
