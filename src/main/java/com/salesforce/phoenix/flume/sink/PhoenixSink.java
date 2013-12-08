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
package com.salesforce.phoenix.flume.sink;

import static com.salesforce.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jboss.netty.channel.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.flume.FlumeConstants;
import com.salesforce.phoenix.flume.SchemaHandler;
import com.salesforce.phoenix.flume.serializer.EventSerializer;
import com.salesforce.phoenix.flume.serializer.EventSerializers;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.util.QueryUtil;

public final class PhoenixSink  extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixSink.class);
    private static AtomicInteger counter = new AtomicInteger();
    private static final String NAME   = "Phoenix Sink__";
  
    private SinkCounter sinkCounter;
    private Connection connection;
    private String     createTableDdl;
    private String     tableName;
    private Integer    batchSize;
    private EventSerializer serializer;
    private String     jdbcUrl;

    public PhoenixSink(){
    }
    
    @Override
    public void configure(Context context){
        this.setName(NAME + counter.incrementAndGet());
        this.createTableDdl = context.getString(FlumeConstants.CONFIG_TABLE_DDL);
        this.tableName = context.getString(FlumeConstants.CONFIG_TABLE);
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        final String eventSerializerType = context.getString(FlumeConstants.CONFIG_SERIALIZER);
        final String zookeeperQuorum = context.getString(FlumeConstants.CONFIG_ZK_QUORUM);
        final String ipJdbcURL = context.getString(FlumeConstants.CONFIG_JDBC_URL);
        
        Preconditions.checkNotNull(this.tableName,"Table name cannot be empty, please specify in the configuration file");
        Preconditions.checkNotNull(eventSerializerType,"Event serializer cannot be empty, please specify in the configuration file");
        if(!Strings.isNullOrEmpty(zookeeperQuorum)) {
            this.jdbcUrl = QueryUtil.getUrl(zookeeperQuorum);
        }
        if(!Strings.isNullOrEmpty(ipJdbcURL)) {
            this.jdbcUrl = ipJdbcURL;
        }
        Preconditions.checkNotNull(this.jdbcUrl,"Please specify either the zookeeper quorum or the jdbc url in the configuration file");
        logger.debug(" the jdbcUrl is {}",jdbcUrl);
        initializeSerializer(context,eventSerializerType);
        this.sinkCounter = new SinkCounter(this.getName());
    }

    /**
     * Initializes the serializer for flume events.
     * @param eventSerializerType
     */
    private void initializeSerializer(final Context context,final String eventSerializerType) {
        
       EventSerializers eventSerializer = null;
       try {
               eventSerializer =  EventSerializers.valueOf(eventSerializerType.toUpperCase());
        } catch(IllegalArgumentException iae) {
               logger.error("An invalid eventSerializer {} was passed. Please specify one of {} ",eventSerializerType,
                       Joiner.on(",").skipNulls().join(EventSerializers.values()));
               Throwables.propagate(iae);
        }
       
       final Context serializerContext = new Context();
       serializerContext.putAll(context.getSubProperties(FlumeConstants.CONFIG_SERIALIZER_PREFIX));
       try {
         @SuppressWarnings("unchecked")
         Class<? extends EventSerializer> clazz = (Class<? extends EventSerializer>) Class.forName(eventSerializer.getClassName());
         serializer = clazz.newInstance();
         serializer.configure(serializerContext);
         
       } catch (Exception e) {
         logger.error("Could not instantiate event serializer." , e);
         Throwables.propagate(e);
       }
    }

    @Override
    public void start() {
        logger.info("Starting sink {} ",this.getName());
        openConnection();  
        try {
            serializer.initialize(this.connection, this.tableName);
        } catch(Exception ex) {
            logger.error("Error {} in initializing the serializer.",ex.getMessage());
            Throwables.propagate(ex);
       }
        sinkCounter.start();
        super.start();
    }
    
    @Override
    public void stop(){
      super.stop();
      closeConnectionQuietly();
      sinkCounter.incrementConnectionClosedCount();
      sinkCounter.stop();
    }

    private void openConnection () {
        logger.info("Opening connection {} ",this.getName());
        final Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, String.valueOf(this.batchSize)); 
        try {
            connection = DriverManager.getConnection(this.jdbcUrl, props).unwrap(PhoenixConnection.class);
            connection.setAutoCommit(false);
            if(this.createTableDdl != null) {
                 SchemaHandler.createTable(connection,createTableDdl);
            }
        } catch (SQLException e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("Error occurred : {} ",e.getMessage());
            throw new FlumeException("Error occured during the start of Phoenix sink",e);
        }
        sinkCounter.incrementConnectionCreatedCount();
    }
  
    private void closeConnectionQuietly() {
       if(connection != null) {
           try {
            connection.close();
        } catch (SQLException e) {
           logger.error(" Error while closing connection {} for sink {} ",e.getMessage(),this.getName());
        }
      }
    }

    @Override
    public Status process() throws EventDeliveryException {
        
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        List<Event>  events = Lists.newArrayListWithExpectedSize(this.batchSize); 
        Stopwatch watch = new Stopwatch().start();
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            
            for(long i = 0; i < this.batchSize; i++) {
                Event event = channel.take();
                if(event == null){
                  status = Status.BACKOFF;
                  if (i == 0) {
                    sinkCounter.incrementBatchEmptyCount();
                  } else {
                    sinkCounter.incrementBatchUnderflowCount();
                  }
                  break;
                } else {
                  events.add(event);
                }
            }
            if (!events.isEmpty()) {
               if (events.size() == this.batchSize) {
                    sinkCounter.incrementBatchCompleteCount();
                }
                else {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                }
                // save to Hbase
                serializer.upsertEvents(events, connection);
                sinkCounter.addToEventDrainSuccessCount(events.size());
            }
            else {
                logger.debug("no events to process ");
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
            }
            transaction.commit();
        } catch (ChannelException e) {
            transaction.rollback();
            status = Status.BACKOFF;
            sinkCounter.incrementConnectionFailedCount();
        }
        catch (SQLException e) {
            sinkCounter.incrementConnectionFailedCount();
            transaction.rollback();
            logger.error("exception while persisting to Hbase ", e);
            throw new EventDeliveryException("Failed to persist message to Hbase", e);
        }
        catch (Throwable e) {
            transaction.rollback();
            logger.error("exception while processing in Phoenix Sink", e);
            throw new EventDeliveryException("Failed to persist message", e);
        }
        finally {
            logger.error(String.format("Time taken to process [%s] events was [%s] seconds",events.size(),watch.stop().elapsedTime(TimeUnit.SECONDS)));
            if( transaction != null ) {
                transaction.close();
            }
        }
        return status;
   }

}
