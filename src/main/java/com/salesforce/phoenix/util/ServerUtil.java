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
package com.salesforce.phoenix.util;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class ServerUtil {
    private ServerUtil() {
    }
    
    public static void throwIOException(String msg, Throwable t) throws IOException {
        // First unwrap any SQLExceptions
        if (t instanceof SQLException) {
            Throwable cause = t.getCause();
            if (cause instanceof IOException) {
                t = cause;
            }
        }
        // Throw immediately if DoNotRetryIOException
        if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
        } else if (t instanceof IOException) {
            Throwable cause = t.getCause();
            if (cause == null || cause instanceof IOException) {
                throw (IOException)t;
            }
            // Else assume it's been wrapped, so throw as DoNotRetryIOException to prevent client hanging while retrying
            throw new DoNotRetryIOException(t.getMessage(), cause);
        }
        throw new DoNotRetryIOException(msg + ": " + t.getMessage(), t);
    }

    // Parse the server side exception to unwrap the underlying error messages.
    public static SQLException parseServerException(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return new SQLException(t);
    }
}
