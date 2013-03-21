package com.salesforce.phoenix.expression.function;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.salesforce.phoenix.compile.KeyPart;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.util.ByteUtil;

abstract public class PrefixFunction extends ScalarFunction {
    public PrefixFunction() {
    }

    public PrefixFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return preservesOrder() ? 0 : NO_TRAVERSAL;
    }
    
    protected boolean extractNode() {
        return false;
    }

    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return extractNode() ? Collections.<Expression>singletonList(PrefixFunction.this) : Collections.<Expression>emptyList();
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, byte[] key) {
                KeyRange range;
                switch (op) {
                case EQUAL:
                    range = KeyRange.getKeyRange(key, true, ByteUtil.nextKey(key), false);
                    break;
                case GREATER:
                    range = KeyRange.getKeyRange(ByteUtil.nextKey(key), true, KeyRange.UNBOUND_UPPER, false);
                    break;
                case LESS_OR_EQUAL:
                    range = KeyRange.getKeyRange(KeyRange.UNBOUND_LOWER, false, ByteUtil.nextKey(key), false);
                    break;
                default:
                    return childPart.getKeyRange(op, key);
                }
                Integer length = getColumn().getByteSize();
                return length == null ? range : range.fill(length);
            }
        };
    }


}
