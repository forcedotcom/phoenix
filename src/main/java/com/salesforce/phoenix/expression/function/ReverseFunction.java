package com.salesforce.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.StringUtil;

@BuiltInFunction(name=ReverseFunction.NAME,  args={
        @Argument(allowedTypes={PDataType.VARCHAR})} )
public class ReverseFunction extends ScalarFunction {
    public static final String NAME = "REVERSE";
    
    public ReverseFunction() {
    }

    public ReverseFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int targetOffset = ptr.getLength();
        if (targetOffset == 0) {
            return true;
        }

        byte[] source = ptr.get();
        byte[] target = new byte[targetOffset];
        int sourceOffset = ptr.getOffset(); 
        int endOffset = sourceOffset + ptr.getLength();
        ColumnModifier modifier = arg.getColumnModifier();
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], modifier);
            targetOffset -= nBytes;
            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);
            sourceOffset += nBytes;
        }
        ptr.set(target);
        return true;
    }

    @Override
    public ColumnModifier getColumnModifier() {
        return getChildren().get(0).getColumnModifier();
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
