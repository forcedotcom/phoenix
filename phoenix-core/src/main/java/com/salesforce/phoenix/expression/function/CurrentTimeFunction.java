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
package com.salesforce.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.CurrentDateTimeFunction;
import com.salesforce.phoenix.parse.CurrentTimeParseNode;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function that returns the current date accurate to the millisecond. Note that this
 * function is never evaluated on the server-side, instead the server side date is
 * retrieved (piggy-backed on the call to check that the metadata is up-to-date) and
 * passed into this function at create time.
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=CurrentTimeFunction.NAME, nodeClass=CurrentTimeParseNode.class, args={} )
public class CurrentTimeFunction extends CurrentDateTimeFunction {
    public static final String NAME = "CURRENT_TIME";
    private final ImmutableBytesWritable currentDate = new ImmutableBytesWritable(new byte[PDataType.TIME.getByteSize()]);
    
    public CurrentTimeFunction() {
        this(System.currentTimeMillis());
    }

    public CurrentTimeFunction(long timeStamp) {
        getDataType().getCodec().encodeLong(timeStamp, currentDate);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(currentDate.get(), 0, PDataType.TIME.getByteSize());
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PDataType.TIME;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
