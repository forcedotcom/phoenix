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

import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.Iterables;


/**
 * Utilities for operating on {@link SQLCloseable}s.
 * 
 * @author jtaylor
 * @since 0.1
 */
public class SQLCloseables {
    /** Not constructed */
    private SQLCloseables() { }
    
    /**
     * Allows you to close as many of the {@link SQLCloseable}s as possible.
     * 
     * If any of the close's fail with an IOException, those exception(s) will
     * be thrown after attempting to close all of the inputs.
     */
    public static void closeAll(Iterable<? extends SQLCloseable> iterable) throws SQLException {
        SQLException ex = closeAllQuietly(iterable);
        if (ex != null) throw ex;
    }
 
    public static SQLException closeAllQuietly(Iterable<? extends SQLCloseable> iterable) {
        if (iterable == null) return null;
        
        LinkedList<SQLException> exceptions = null;
        for (SQLCloseable closeable : iterable) {
            try {
                closeable.close();
            } catch (SQLException x) {
                if (exceptions == null) exceptions = new LinkedList<SQLException>();
                exceptions.add(x);
            }
        }
        
        SQLException ex = MultipleCausesSQLException.fromSQLExceptions(exceptions);
        return ex;
    }

    /**
     * A subclass of {@link SQLException} that allows you to chain multiple 
     * causes together.
     * 
     * @author jtaylor
     * @since 0.1
     * @see SQLCloseables
     */
    static private class MultipleCausesSQLException extends SQLException {
		private static final long serialVersionUID = 1L;

		static SQLException fromSQLExceptions(Collection<? extends SQLException> exceptions) {
            if (exceptions == null || exceptions.isEmpty()) return null;
            if (exceptions.size() == 1) return Iterables.getOnlyElement(exceptions);
            
            return new MultipleCausesSQLException(exceptions);
        }
        
        private final Collection<? extends SQLException> exceptions;
        private boolean hasSetStackTrace;
        
        /**
         * Use the {@link #fromIOExceptions(Collection) factory}.
         */
        private MultipleCausesSQLException(Collection<? extends SQLException> exceptions) {
            this.exceptions = exceptions;
        }

        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder(this.exceptions.size() * 50);
            int exceptionNum = 0;
            for (SQLException ex : this.exceptions) {
                sb.append("Cause Number " + exceptionNum + ": " + ex.getMessage() + "\n");
                exceptionNum++;
            }
            return sb.toString();
        }
        
        @Override
        public StackTraceElement[] getStackTrace() {
            if (!this.hasSetStackTrace) {
                ArrayList<StackTraceElement> frames = new ArrayList<StackTraceElement>(this.exceptions.size() * 20);
                
                int exceptionNum = 0;
                for (SQLException exception : this.exceptions) {
                    StackTraceElement header = new StackTraceElement(MultipleCausesSQLException.class.getName(), 
                            "Exception Number " + exceptionNum, 
                            "<no file>",
                            0);
                    
                    frames.add(header);
                    for (StackTraceElement ste : exception.getStackTrace()) {
                        frames.add(ste);
                    }
                    exceptionNum++;
                }
                
                setStackTrace(frames.toArray(new StackTraceElement[frames.size()]));
                this.hasSetStackTrace = true;
            }        
            
            return super.getStackTrace();
        }

    }
    
}
