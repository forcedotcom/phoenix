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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Iterables;


/**
 * Utilities for operating on {@link Closeable}s.
 * 
 */
public class Closeables {
    /** Not constructed */
    private Closeables() { }
    
    /**
     * Allows you to close as many of the {@link Closeable}s as possible.
     * 
     * If any of the close's fail with an IOException, those exception(s) will
     * be thrown after attempting to close all of the inputs.
     */
    public static void closeAll(Iterable<? extends Closeable> iterable) throws IOException {
        IOException ex = closeAllQuietly(iterable);
        if (ex != null) throw ex;
    }
 
    public static IOException closeAllQuietly(Iterable<? extends Closeable> iterable) {
        if (iterable == null) return null;
        
        LinkedList<IOException> exceptions = null;
        for (Closeable closeable : iterable) {
            try {
                closeable.close();
            } catch (IOException x) {
                if (exceptions == null) exceptions = new LinkedList<IOException>();
                exceptions.add(x);
            }
        }
        
        IOException ex = MultipleCausesIOException.fromIOExceptions(exceptions);
        return ex;
    }

    static private class MultipleCausesIOException extends IOException {
    	private static final long serialVersionUID = 1L;

        static IOException fromIOExceptions(Collection<? extends IOException> exceptions) {
            if (exceptions == null || exceptions.isEmpty()) return null;
            if (exceptions.size() == 1) return Iterables.getOnlyElement(exceptions);
            
            return new MultipleCausesIOException(exceptions);
        }
        
        private final Collection<? extends IOException> exceptions;
        private boolean hasSetStackTrace;
        
        /**
         * Use the {@link #fromIOExceptions(Collection) factory}.
         */
        private MultipleCausesIOException(Collection<? extends IOException> exceptions) {
            this.exceptions = exceptions;
        }

        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder(this.exceptions.size() * 50);
            int exceptionNum = 0;
            for (IOException ex : this.exceptions) {
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
                for (IOException exception : this.exceptions) {
                    StackTraceElement header = new StackTraceElement(MultipleCausesIOException.class.getName(), 
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
