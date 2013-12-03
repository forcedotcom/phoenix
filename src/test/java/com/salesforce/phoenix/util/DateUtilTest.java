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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.junit.Test;

/**
 * 
 * Test class for {@link DateUtil}
 *
 * @author samarth.jain
 * @since 2.1.3
 */
public class DateUtilTest {
    
    @Test
    public void testDemonstrateSetNanosOnTimestampLosingMillis() {
        Timestamp ts1 = new Timestamp(120055);
        ts1.setNanos(60);
        
        Timestamp ts2 = new Timestamp(120100);
        ts2.setNanos(60);
        
        /*
         * This really should have been assertFalse() because we started with timestamps that 
         * had different milliseconds 120055 and 120100. THe problem is that the timestamp's 
         * constructor converts the milliseconds passed into seconds and assigns the left-over
         * milliseconds to the nanos part of the timestamp. If setNanos() is called after that
         * then the previous value of nanos gets overwritten resulting in loss of milliseconds.
         */
        assertTrue(ts1.equals(ts2));
        
        /*
         * The right way to deal with timestamps when you have both milliseconds and nanos to assign
         * is to use the DateUtil.getTimestamp(long millis, int nanos).
         */
        ts1 = DateUtil.getTimestamp(120055,  60);
        ts2 = DateUtil.getTimestamp(120100, 60);
        assertFalse(ts1.equals(ts2));
        assertTrue(ts2.after(ts1));
    }
}
