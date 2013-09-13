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
package com.salesforce.hbase.index.covered.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.covered.update.ColumnTracker;

public class TestColumnTracker {

  @Test
  public void testEnsureGuarranteedMinValid() {
    assertFalse("Guarranted min wasn't recognized as having newer timestamps!",
      ColumnTracker.isNewestTime(ColumnTracker.GUARANTEED_NEWER_UPDATES));
  }

  @Test
  public void testOnlyKeepsOlderTimestamps() {
    Collection<ColumnReference> columns = new ArrayList<ColumnReference>();
    ColumnTracker tracker = new ColumnTracker(columns);
    tracker.setTs(10);
    assertEquals("Column tracker didn't set original TS", 10, tracker.getTS());
    tracker.setTs(12);
    assertEquals("Column tracker allowed newer timestamp to be set.", 10, tracker.getTS());
    tracker.setTs(9);
    assertEquals("Column tracker didn't decrease set timestamp for smaller value", 9,
      tracker.getTS());
  }

  @Test
  public void testHasNewerTimestamps() throws Exception {
    Collection<ColumnReference> columns = new ArrayList<ColumnReference>();
    ColumnTracker tracker = new ColumnTracker(columns);
    assertFalse("Tracker has newer timestamps when no ts set", tracker.hasNewerTimestamps());
    tracker.setTs(10);
    assertTrue("Tracker doesn't have newer timetamps with set ts", tracker.hasNewerTimestamps());
  }
}