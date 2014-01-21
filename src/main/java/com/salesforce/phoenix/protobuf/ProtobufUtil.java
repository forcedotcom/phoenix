/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source
 * and binary forms, with or without modification, are permitted provided that the following
 * conditions are met: Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary form must reproduce
 * the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of
 * Salesforce.com nor the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission. THIS SOFTWARE IS PROVIDED
 * BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcController;
import com.salesforce.phoenix.coprocessor.generated.MetaDataProtos;
import com.salesforce.phoenix.coprocessor.generated.ServerCachingProtos;
import com.salesforce.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import com.salesforce.phoenix.coprocessor.generated.MetaDataProtos.MutationCode;
import com.salesforce.phoenix.coprocessor.generated.PTableProtos;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableImpl;
import com.salesforce.phoenix.schema.PTableType;

public class ProtobufUtil {

    /**
     * Stores an exception encountered during RPC invocation so it can be passed back through to the
     * client.
     * @param controller the controller instance provided by the client when calling the service
     * @param ioe the exception encountered
     */
    public static void setControllerException(RpcController controller, IOException ioe) {
        if (controller != null) {
            if (controller instanceof ServerRpcController) {
                ((ServerRpcController) controller).setFailedOn(ioe);
            } else {
                controller.setFailed(StringUtils.stringifyException(ioe));
            }
        }
    }

    public static PTableProtos.PTableType toPTableTypeProto(PTableType type) {
        return PTableProtos.PTableType.values()[type.ordinal()];
    }

    public static PTableType toPTableType(PTableProtos.PTableType type) {
        return PTableType.values()[type.ordinal()];
    }

    public static PTableProtos.PDataType toPDataTypeProto(PDataType type) {
        return PTableProtos.PDataType.values()[type.ordinal()];
    }

    public static PDataType toPDataType(PTableProtos.PDataType type) {
        return PDataType.values()[type.ordinal()];
    }

    public static List<Mutation> getMutations(MetaDataProtos.CreateTableRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.DropTableRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.AddColumnRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.DropColumnRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.UpdateIndexStateRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    /**
     * Each ByteString entry is a byte array serialized from MutationProto instance
     * @param mutations
     * @throws IOException
     */
    private static List<Mutation> getMutations(List<ByteString> mutations)
            throws IOException {
        List<Mutation> result = new ArrayList<Mutation>();
        for (ByteString mutation : mutations) {
            MutationProto mProto = MutationProto.parseFrom(mutation);
            result.add(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(mProto));
        }
        return result;
    }

    public static MutationProto toProto(Mutation mutation) throws IOException {
        MutationType type;
        if (mutation instanceof Put) {
            type = MutationType.PUT;
        } else if (mutation instanceof Delete) {
            type = MutationType.DELETE;
        } else {
            throw new IllegalArgumentException("Only Put and Delete are supported");
        }
        return org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(type, mutation);
    }
    
    public static ServerCachingProtos.ImmutableBytesWritable toProto(ImmutableBytesWritable w) {
        ServerCachingProtos.ImmutableBytesWritable.Builder builder = 
        		ServerCachingProtos.ImmutableBytesWritable.newBuilder();
        builder.setByteArray(ByteString.copyFrom(w.get()));
        builder.setOffset(w.getOffset());
        builder.setLength(w.getLength());
        return builder.build();
    }
    
    public static ImmutableBytesWritable toImmutableBytesWritable(ServerCachingProtos.ImmutableBytesWritable proto) {
    	return new ImmutableBytesWritable(proto.getByteArray().toByteArray(), proto.getOffset(), proto.getLength());
    }
}
