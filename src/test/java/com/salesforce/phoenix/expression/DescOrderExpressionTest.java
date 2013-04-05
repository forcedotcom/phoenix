package com.salesforce.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.function.LTrimFunction;
import com.salesforce.phoenix.expression.function.RTrimFunction;
import com.salesforce.phoenix.expression.function.SubstrFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

public class DescOrderExpressionTest {
    
    @Test
    public void substrDoesNotInvert() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(args), "ah", Expectation.EvaluationDidNotInvertBits);
    }
    
    @Test
    public void ltrimInverts() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        evaluateAndAssertResult(new LTrimFunction(args), "blah", Expectation.EvaluationInvertedBits);        
    }
    
    @Test
    public void substrOfLTrimResult() throws Exception {
        List<Expression> ltrimArgs = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        Expression ltrim = new LTrimFunction(ltrimArgs);
        List<Expression> substrArgs = Lists.newArrayList(ltrim, getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(substrArgs), "ah", Expectation.EvaluationInvertedBits);
    }
    
    @Test
    public void rtrimInverts() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah    ", PDataType.CHAR));
        evaluateAndAssertResult(new RTrimFunction(args), "blah", Expectation.EvaluationInvertedBits);        
    }
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult, Expectation expectation) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(expression.evaluate(null, ptr));
        PDataType dataType = expression.getDataType();
        ColumnModifier columnModifier = expectation == Expectation.EvaluationDidNotInvertBits ? ColumnModifier.SORT_DESC : null;
        String result = (String)dataType.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), dataType, columnModifier);
        assertEquals(expectedResult, result);
    }
    
    private Expression getLiteral(Object value) {
        return LiteralExpression.newConstant(value);
    }
    
    private Expression getInvertedLiteral(Object literal, PDataType dataType) throws Exception {
        return LiteralExpression.newConstant(literal, dataType, ColumnModifier.SORT_DESC);
    }
    
    private enum Expectation {
        EvaluationInvertedBits, EvaluationDidNotInvertBits;
    }
}
