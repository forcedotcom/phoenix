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
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(expression.evaluate(null, ptr));
        PDataType dataType = expression.getDataType();
        ColumnModifier columnModifier = expression.getColumnModifier();
        Object result = dataType.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), dataType, columnModifier);
        assertEquals(expectedResult, result);
    }
    
    private Expression getLiteral(Object value) {
        return LiteralExpression.newConstant(value);
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
