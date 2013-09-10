package com.salesforce.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.salesforce.phoenix.schema.PDataType;

public class CoerceExpressionTest {
    @Test
    public void testCoerceExpression() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant(1, PDataType.INTEGER);
        CoerceExpression e = new CoerceExpression(v, PDataType.DECIMAL);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        Boolean result = (Boolean)e.getDataType().toObject(ptr);
        assertTrue(evaluated);
        assertEquals(Boolean.FALSE,result);
    }
}
