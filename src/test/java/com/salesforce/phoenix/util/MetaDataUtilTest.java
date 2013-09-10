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

import static org.junit.Assert.*;

import org.junit.Test;


public class MetaDataUtilTest {

    @Test
    public void testEncode() {
        assertEquals(MetaDataUtil.encodeVersion("0.94.5"),MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.1-mapR")>MetaDataUtil.encodeVersion("0.94"));
    }
    
    @Test
    public void testCompatibility() {
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,1), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,10), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,0), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,255), 1, 2));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(2,2,0), 2, 1));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(3,1,10), 4, 2));
    }
}
