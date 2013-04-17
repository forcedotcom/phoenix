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
package com.salesforce.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.expression.function.LTrimFunction;
import com.salesforce.phoenix.expression.function.LengthFunction;
import com.salesforce.phoenix.expression.function.LowerFunction;
import com.salesforce.phoenix.expression.function.RTrimFunction;
import com.salesforce.phoenix.expression.function.RegexpReplaceFunction;
import com.salesforce.phoenix.expression.function.RegexpSubstrFunction;
import com.salesforce.phoenix.expression.function.RoundFunction;
import com.salesforce.phoenix.expression.function.SqlTypeNameFunction;
import com.salesforce.phoenix.expression.function.SubstrFunction;
import com.salesforce.phoenix.expression.function.ToCharFunction;
import com.salesforce.phoenix.expression.function.ToDateFunction;
import com.salesforce.phoenix.expression.function.ToNumberFunction;
import com.salesforce.phoenix.expression.function.TrimFunction;
import com.salesforce.phoenix.expression.function.UpperFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.DateUtil;

public class DescColumnSortOrderExpressionTest {
    
    @Test
    public void substr() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(args), "ah");
    }
    
    @Test
    public void regexpSubstr() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral("l.h"), getLiteral(2));
        evaluateAndAssertResult(new RegexpSubstrFunction(args), "lah");
    }
    
    @Test
    public void regexpReplace() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral("l.h"), getLiteral("foo"));
        evaluateAndAssertResult(new RegexpReplaceFunction(args), "bfoo");
    }
    
    @Test
    public void ltrim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        evaluateAndAssertResult(new LTrimFunction(args), "blah");
    }
    
    @Test
    public void substrLtrim() throws Exception {
        List<Expression> ltrimArgs = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        Expression ltrim = new LTrimFunction(ltrimArgs);
        List<Expression> substrArgs = Lists.newArrayList(ltrim, getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(substrArgs), "ah");
    }
    
    @Test
    public void rtrim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah    ", PDataType.CHAR));
        evaluateAndAssertResult(new RTrimFunction(args), "blah");
    }
    
    @Test
    public void lower() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("BLAH", PDataType.CHAR));
        evaluateAndAssertResult(new LowerFunction(args), "blah");        
    }
    
    @Test
    public void upper() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR));
        evaluateAndAssertResult(new UpperFunction(args), "BLAH");        
    }
    
    @Test
    public void length() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR));
        evaluateAndAssertResult(new LengthFunction(args), 4);
    }
    
    @Test
    public void round() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(date(12, 11, 2001), PDataType.DATE), getLiteral("hour"), getLiteral(1));
        evaluateAndAssertResult(new RoundFunction(args), date(12, 11, 2001));
    }
    
    @Test
    public void sqlTypeName() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(12, PDataType.INTEGER));
        evaluateAndAssertResult(new SqlTypeNameFunction(args), "VARCHAR");        
    }
    
    @Test
    public void toChar() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(date(12, 11, 2001), PDataType.DATE));
        evaluateAndAssertResult(new ToCharFunction(args, null, new SimpleDateFormat()), "12/11/01 12:00 AM");
    }
    
    @Test
    public void toDate() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("2001-11-30 01:00:00:0 PDT", PDataType.VARCHAR));
        evaluateAndAssertResult(new ToDateFunction(args, null, DateUtil.getDateParser("yyyy-MM-dd HH:mm:ss:S z")), date(11, 30, 2001));
    }
    
    @Test
    public void toNumber() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("10", PDataType.VARCHAR));
        evaluateAndAssertResult(new ToNumberFunction(args), new BigDecimal(BigInteger.valueOf(1), -1));
    }
    
    @Test
    public void trim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("   blah    ", PDataType.CHAR));
        evaluateAndAssertResult(new TrimFunction(args), "blah");
    }
    
    @Test
    public void divide() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalDivideExpression(args), BigDecimal.valueOf(5));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongDivideExpression(args), 5l);
        
    }
    
    @Test
    public void multiply() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalMultiplyExpression(args), new BigDecimal(BigInteger.valueOf(2), -1));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongMultiplyExpression(args), 20l);        
    }
        
    @Test
    public void compareNumbers() throws Exception {
        PDataType[] numberDataTypes = new PDataType[]{PDataType.INTEGER, PDataType.LONG, PDataType.DECIMAL, PDataType.UNSIGNED_INT, PDataType.UNSIGNED_LONG};
        for (PDataType lhsDataType : numberDataTypes) {
            for (PDataType rhsDataType : numberDataTypes) {
                runCompareTest(CompareOp.GREATER, true, 10, lhsDataType, 2, rhsDataType);
            }
        }
    }
    
    @Test
    public void compareCharacters() throws Exception {
        PDataType[] textDataTypes = new PDataType[]{PDataType.CHAR, PDataType.VARCHAR};
        for (PDataType lhsDataType : textDataTypes) {
            for (PDataType rhsDataType : textDataTypes) {
                runCompareTest(CompareOp.GREATER, true, "xxx", lhsDataType, "bbb", rhsDataType);
            }
        }
    }
    
    @Test
    public void compareBooleans() throws Exception {
        // see review comment in PDataType.toBytes(Object object, ColumnModifier columnModifier)
        runCompareTest(CompareOp.GREATER, true, true, PDataType.BOOLEAN, false, PDataType.BOOLEAN);        
    }
    
    private void runCompareTest(CompareOp op, boolean expectedResult, Object lhsValue, PDataType lhsDataType, Object rhsValue, PDataType rhsDataType) throws Exception {
        List<Expression> args = Lists.newArrayList(getLiteral(lhsValue, lhsDataType), getLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhsDataType: " + lhsDataType + " rhsDataType: " + rhsDataType);
        
        args = Lists.newArrayList(getInvertedLiteral(lhsValue, lhsDataType), getLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhs (inverted) dataType: " + lhsDataType + " rhsDataType: " + rhsDataType);
        
        args = Lists.newArrayList(getLiteral(lhsValue, lhsDataType), getInvertedLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhsDataType: " + lhsDataType + " rhs (inverted) dataType: " + rhsDataType);
        
        args = Lists.newArrayList(getInvertedLiteral(lhsValue, lhsDataType), getInvertedLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhs (inverted) dataType: " + lhsDataType + " rhs (inverted) dataType: " + rhsDataType);                
    }
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult) {
        evaluateAndAssertResult(expression, expectedResult, null);
    }
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult, String context) {
        context = context == null ? "" : context;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(expression.evaluate(null, ptr));
        PDataType dataType = expression.getDataType();
        ColumnModifier columnModifier = expression.getColumnModifier();
        Object result = dataType.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), dataType, columnModifier);
        assertEquals(context, expectedResult, result);
    }
    
    private Expression getLiteral(Object value) throws Exception {
        return LiteralExpression.newConstant(value);
    }
    
    private Expression getLiteral(Object value, PDataType dataType) throws Exception {
        return LiteralExpression.newConstant(value, dataType);
    }    
    
    private Expression getInvertedLiteral(Object literal, PDataType dataType) throws Exception {
        return LiteralExpression.newConstant(literal, dataType, ColumnModifier.SORT_DESC);
    }
    
    private static Date date(int month, int day, int year) {
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.MONTH, month-1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        Date d = new Date(cal.getTimeInMillis()); 
        return d;
    }    
}
