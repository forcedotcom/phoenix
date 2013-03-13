package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.DataInput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@FunctionParseNode.BuiltInFunction(name=LowerFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PDataType.VARCHAR})} )
public class LowerFunction extends ScalarFunction {
    public static final String NAME = "LOWER";
    private boolean isFixedWidth;
    private Integer byteSize;

    public LowerFunction() {
    }

    public LowerFunction(List<Expression> children) throws SQLException {
        super(children);
        init();
    }

    private void init() {
        isFixedWidth = getStrExpression().getDataType().isFixedWidth();
        byteSize = getStrExpression().getByteSize();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }

        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr);
        if (sourceStr == null) {
            return false;
        }

        ptr.set(PDataType.VARCHAR.toBytes(sourceStr.toLowerCase()));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return isFixedWidth ? PDataType.CHAR : PDataType.VARCHAR;
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable() || !isFixedWidth;
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public boolean preservesOrder() {
        return true;
    }

    @Override
    public KeyFormationDirective getKeyFormationDirective() {
        return preservesOrder() ? KeyFormationDirective.TRAVERSE_AND_EXTRACT : KeyFormationDirective.UNTRAVERSABLE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getStrExpression() {
        return children.get(0);
    }
}
