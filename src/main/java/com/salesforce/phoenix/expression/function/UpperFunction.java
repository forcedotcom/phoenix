package com.salesforce.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

@FunctionParseNode.BuiltInFunction(name=UpperFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PDataType.VARCHAR})} )
public class UpperFunction extends ScalarFunction {
    public static final String NAME = "UPPER";

    public UpperFunction() {
    }

    public UpperFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }

        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr);
        if (sourceStr == null) {
            return true;
        }

        ptr.set(PDataType.VARCHAR.toBytes(sourceStr.toUpperCase()));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return getStrExpression().getDataType();
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getStrExpression() {
        return children.get(0);
    }
}
