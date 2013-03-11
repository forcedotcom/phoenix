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
package com.salesforce.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SkipScanFilter implements Filter {
    private static final byte[] FIN = {};
    private static final Configuration conf = HBaseConfiguration.create();
    private Filter filter;
    private List<List<Pair<byte[],byte[]>>> cnf;
    private byte[] hint = null;

    public SkipScanFilter() {
    }

    public SkipScanFilter setFilter(Filter filter) {
        Preconditions.checkState(this.filter == null);
        Preconditions.checkNotNull(filter);
        this.filter = filter;
        return this;
    }

    public SkipScanFilter setCnf(List<List<byte[]>> cnf) {
        Preconditions.checkState(this.cnf == null);
        Preconditions.checkNotNull(cnf);
        this.cnf = new ArrayList<List<Pair<byte[],byte[]>>>();
        for (int i=0; i<cnf.size(); i++) {
            int bytes = Math.max(cnf.get(i).get(0).length, cnf.get(i).get(1).length);
            assert bytes != 0 : "not sure how to handle EVERYTHING segments yet";
            Preconditions.checkArgument(cnf.get(i).size() % 2 == 0);
            List<Pair<byte[],byte[]>> ors = new ArrayList<Pair<byte[],byte[]>>();
            this.cnf.add(ors);
            for (int j=0; j<cnf.get(i).size(); j+=2) {
                byte[] first = cnf.get(i).get(j);
                byte[] second = cnf.get(i).get(j+1);
                Preconditions.checkArgument(first.length == 0 || first.length == bytes);
                Preconditions.checkArgument(second.length == 0 || second.length == bytes);
                Preconditions.checkArgument(first.length == 0 || second.length == 0 || Bytes.compareTo(first, second) <= 0);
                if (j+2 < cnf.get(i).size()) {
                    Preconditions.checkArgument(second.length != 0);
                    Preconditions.checkArgument(cnf.get(i).get(j+2).length != 0);
                    Preconditions.checkArgument(Bytes.compareTo(second, cnf.get(i).get(j+2)) < 0);
                }
                ors.add(Pair.newPair(first, second));
            }
        }
        return this;
    }
    
    public Filter getFilter() {
        return filter;
    }
    
    @Override
    public void reset() {
        if (FIN != this.hint) {
            this.hint = null;
        }
        filter.reset();
    }

    @Override public boolean filterAllRemaining() {
        return FIN == this.hint || filter.filterAllRemaining();
    }
    
    @Override
    public boolean filterRowKey(final byte[] data, int offset, int length) {
        int mylen = 0;
        for (int i=0; i<cnf.size(); i++) {
            mylen += Math.max(cnf.get(i).get(0).getFirst().length, cnf.get(i).get(0).getSecond().length);
        }
        byte[] hint = new byte[mylen];
        int hintoffset = 0;
        outer: for (int i=0; i<cnf.size(); i++) {
            List<Pair<byte[],byte[]>> ors = cnf.get(i);
            for (int j=0; j<ors.size(); j++) {
                Pair<byte[],byte[]> range = ors.get(j);
                int cmpLower = range.getFirst().length == 0 ? 1 : Bytes.compareTo(data, offset + hintoffset, range.getFirst().length, range.getFirst(), 0, range.getFirst().length);
                int cmpUpper = range.getSecond().length == 0 ? -1 : Bytes.compareTo(data, offset + hintoffset, range.getSecond().length, range.getSecond(), 0, range.getSecond().length);
                if (cmpLower < 0) {
                    assert cmpUpper < 0;
                    System.arraycopy(range.getFirst(), 0, hint, hintoffset, range.getFirst().length);
                    hintoffset += range.getFirst().length;
                    while (++i < cnf.size()) {
                        Pair<byte[],byte[]> first = cnf.get(i).get(0);
                        System.arraycopy(first.getFirst(), 0, hint, hintoffset, first.getFirst().length);
                        hintoffset += first.getFirst().length;
                    }
                    this.hint = hint;
                    break outer;
                } else if (cmpUpper <= 0) {
                    System.arraycopy(data, offset + hintoffset, hint, hintoffset, range.getFirst().length);
                    hintoffset += range.getFirst().length;
                    continue outer;
                }
            }
            // went past the end!
            this.hint = FIN;
        }
        boolean removeEntireRow = filter.filterRowKey(data, offset, length);
        if (this.hint == null) {
            return removeEntireRow;
        } else {
            return false;
        }
    }
    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
        ReturnCode code = filter.filterKeyValue(v);
        if (this.hint != null) {
            // we can skip!
            assert code == ReturnCode.SKIP || code == ReturnCode.NEXT_ROW: "skip hint and subfilter disagree: " + code;
            if (this.hint == FIN) {
                return ReturnCode.SKIP;
            }
            return ReturnCode.SEEK_NEXT_USING_HINT;
        } else {
            return code;
        }
    }

    @Override public KeyValue getNextKeyHint(KeyValue currentKV) {
        return KeyValue.createFirstOnRow(hint);
    }
    
    @Override public boolean hasFilterRow() {
        return filter.hasFilterRow();
    }

    @Override public void filterRow(List<KeyValue> kvs) {
        assert filter.hasFilterRow();
        filter.filterRow(kvs);
    }

    @Override
    public boolean filterRow() {
        return filter.filterRow();
    }

    @Override public void readFields(DataInput in) throws IOException {
        filter = (Filter)HbaseObjectWritable.readObject(in, conf);
        int n = in.readInt();
        cnf = new ArrayList<List<Pair<byte[],byte[]>>>();
        for (int i=0; i<n; i++) {
            int orlen = in.readInt();
            List<Pair<byte[],byte[]>> orclause = new ArrayList<Pair<byte[],byte[]>>();
            cnf.add(orclause);
            for (int j=0; j<orlen; j++) {
                orclause.add(Pair.newPair(WritableUtils.readCompressedByteArray(in), WritableUtils.readCompressedByteArray(in)));
            }
        }
    }

    @Override public void write(DataOutput out) throws IOException {
        HbaseObjectWritable.writeObject(out, filter, Writable.class, conf);
        out.writeInt(cnf.size());
        for (List<Pair<byte[],byte[]>> orclause : cnf) {
            out.writeInt(orclause.size());
            for (Pair<byte[],byte[]> arr : orclause) {
                WritableUtils.writeCompressedByteArray(out, arr.getFirst());
                WritableUtils.writeCompressedByteArray(out, arr.getSecond());
            }
        }
    }

    @Override public KeyValue transform(KeyValue v) {
        return filter.transform(v);
    }

    @Override public boolean isFamilyEssential(byte[] name) {
        return filter.isFamilyEssential(name);
    }

    @Override public int hashCode() {
        HashFunction hf = Hashing.goodFastHash(32);
        Hasher h = hf.newHasher();
        h.putInt(filter.hashCode()); 
        h.putInt(cnf.size());
        for (int i=0; i<cnf.size(); i++) {
            h.putInt(cnf.get(i).size());
            for (int j=0; j<cnf.size(); j++) {
                h.putBytes(cnf.get(i).get(j).getFirst());
                h.putBytes(cnf.get(i).get(j).getSecond());
            }
        }
        return h.hash().asInt();
    }
    
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof SkipScanFilter)) return false;
        SkipScanFilter other = (SkipScanFilter)obj;
        if (!Objects.equal(filter, other.filter)) return false;
        if (cnf.size() != other.cnf.size()) return false;
        for (int i=0; i<cnf.size(); i++) {
            if (cnf.get(i).size() != other.cnf.get(i).size()) return false;
            for (int j=0; j<cnf.size(); j++) {
                if (!Arrays.equals(cnf.get(i).get(j).getFirst(), other.cnf.get(i).get(j).getFirst())) return false;
                if (!Arrays.equals(cnf.get(i).get(j).getSecond(), other.cnf.get(i).get(j).getSecond())) return false;
            }
        }
        return true;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i=0; i<cnf.size(); i++) {
            if (i>0) sb.append(", ");
            sb.append("[");
            for (int j=0; j<cnf.get(i).size(); j++) {
                if (j>0) sb.append(", ");
                sb.append("[").append(Bytes.toStringBinary(cnf.get(i).get(j).getFirst()));
                sb.append(", ").append(Bytes.toStringBinary(cnf.get(i).get(j).getFirst())).append("]");
            }
            sb.append("]");
        }
        sb.append("]");
        sb.append(", filter=").append(filter);
        return sb.toString();
    }
}
