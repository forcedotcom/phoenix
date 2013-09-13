package com.salesforce.phoenix.expression.function;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

@FunctionParseNode.BuiltInFunction(name=MD5Function.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PDataType.VARCHAR})} )
public class MD5Function extends ScalarFunction {
    public static final String NAME = "MD5";

    public MD5Function() {
    }

    public MD5Function(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }

        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr, getStrExpression().getColumnModifier());

        if (sourceStr == null) {
            return true;
        }

        try {
          ptr.set(MessageDigest.getInstance("MD5").digest(sourceStr.getBytes()));
          return true;
        } catch (NoSuchAlgorithmException e) {
          throw new IllegalArgumentException(e);
        }
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
