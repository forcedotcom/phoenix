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
package com.salesforce.phoenix.flume;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Assert;
import org.junit.Test;

import com.salesforce.phoenix.end2end.BaseHBaseManagedTimeTest;
import com.salesforce.phoenix.flume.serializer.EventSerializers;
import com.salesforce.phoenix.flume.sink.PhoenixSink;
import com.salesforce.phoenix.util.TestUtil;

public class TestPhoenixSink extends BaseHBaseManagedTimeTest{

    private Context sinkContext;
    private PhoenixSink sink;
   
   
    @Test
    public void testSinkCreation() {
        SinkFactory factory = new DefaultSinkFactory ();
        Sink sink = factory.create("PhoenixSink__", "com.salesforce.phoenix.flume.sink.PhoenixSink");
        Assert.assertNotNull(sink);
        Assert.assertTrue(PhoenixSink.class.isInstance(sink));
    }
    @Test
    public void testConfiguration () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    
    
    @Test(expected= NullPointerException.class)
    public void testInvalidConfiguration () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidConfigurationOfSerializer () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,"csv");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    @Test
    public void testInvalidTable() {
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "flume_test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
      
        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        try {
            sink.start();
            fail();
        }catch(Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1012 (42M03): Table undefined."));
        }
    }
    
    @Test
    public void testSinkLifecycle () {
        
        String ddl = "CREATE TABLE flume_test " +
                "  (flume_time timestamp not null, col1 varchar , col2 varchar" +
                "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "flume_test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_TABLE_DDL, ddl);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_REGULAR_EXPRESSION,"^([^\t]+)\t([^\t]+)$");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        
        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
        Assert.assertEquals(LifecycleState.IDLE, sink.getLifecycleState());
      
        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        
        sink.start();
        Assert.assertEquals(LifecycleState.START, sink.getLifecycleState());
        sink.stop();
        Assert.assertEquals(LifecycleState.STOP, sink.getLifecycleState());
    }
    
    @Test
    public void testCreateTable () throws Exception {
        
        String ddl = "CREATE TABLE flume_test " +
                "  (flume_time timestamp not null, col1 varchar , col2 varchar" +
                "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";

        final String fullTableName = "FLUME_TEST";
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, fullTableName);
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, TestUtil.PHOENIX_JDBC_URL);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_TABLE_DDL, ddl);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_REGULAR_EXPRESSION,"^([^\t]+)\t([^\t]+)$");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        
        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
        Assert.assertEquals(LifecycleState.IDLE, sink.getLifecycleState());
      
        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        
        sink.start();
        HBaseAdmin admin = driver.getConnectionQueryServices(TestUtil.PHOENIX_JDBC_URL, TestUtil.TEST_PROPERTIES).getAdmin();
        try {
            boolean exists = admin.tableExists(fullTableName);
            Assert.assertTrue(exists);
        }finally {
            admin.close();
        }
    }
    
    private Channel initChannel() {
        //Channel configuration
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("memorychannel");
        Configurables.configure(channel, channelContext);
        return channel;
    }
    
    
}
