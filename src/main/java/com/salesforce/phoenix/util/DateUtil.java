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
package com.salesforce.phoenix.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.function.TimeUnit;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.IllegalDataException;
import com.salesforce.phoenix.schema.PDataType;



@SuppressWarnings("serial")
public class DateUtil {
    public static final TimeZone DATE_TIME_ZONE = TimeZone.getTimeZone("GMT");
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // This is the format the app sets in NLS settings for every connection.
    public static final Format DEFAULT_DATE_FORMATTER = FastDateFormat.getInstance(DEFAULT_DATE_FORMAT, DATE_TIME_ZONE);

    public static final String DEFAULT_MS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final Format DEFAULT_MS_DATE_FORMATTER = FastDateFormat.getInstance(DEFAULT_MS_DATE_FORMAT, DATE_TIME_ZONE);

    private DateUtil() {
    }

    public static final Expression millisLiteralExpression = init();

    private static Expression init() {
        try {
            return LiteralExpression.newConstant(TimeUnit.MILLISECOND.name(), PDataType.VARCHAR);
        } catch (SQLException e) {
            //not possible
            return null;
        }
    }

    public static Format getDateParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Date(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getTimeParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Time(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getTimestampParser(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern) {
            @Override
            public java.util.Date parseObject(String source) throws ParseException {
                java.util.Date date = super.parse(source);
                return new java.sql.Timestamp(date.getTime());
            }
        };
        format.setTimeZone(DateUtil.DATE_TIME_ZONE);
        return format;
    }

    public static Format getDateFormatter(String pattern) {
        return DateUtil.DEFAULT_DATE_FORMAT.equals(pattern) ? DateUtil.DEFAULT_DATE_FORMATTER : FastDateFormat.getInstance(pattern, DateUtil.DATE_TIME_ZONE);
    }

    private static ThreadLocal<Format> dateFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getDateParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Date parseDate(String dateValue) {
        try {
            return (Date)dateFormat.get().parseObject(dateValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
    }

    private static ThreadLocal<Format> timeFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getTimeParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Time parseTime(String timeValue) {
        try {
            return (Time)timeFormat.get().parseObject(timeValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
    }

    private static ThreadLocal<Format> timestampFormat =
            new ThreadLocal < Format > () {
        @Override protected Format initialValue() {
            return getTimestampParser(DEFAULT_DATE_FORMAT);
        }
    };

    public static Timestamp parseTimestamp(String timeValue) {
        try {
            return (Timestamp)timestampFormat.get().parseObject(timeValue);
        } catch (ParseException e) {
            throw new IllegalDataException(e);
        }
    }

    /**
     * Utility function to work around the weirdness of the {@link Timestamp} constructor.
     * This method takes the milli-seconds that spills over to the nanos part as part of 
     * constructing the {@link Timestamp} object.
     * If we just set the nanos part of timestamp to the nanos passed in param, we 
     * end up losing the sub-second part of timestamp. 
     */
    public static Timestamp getTimestamp(long millis, int nanos) {
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(ts.getNanos() + nanos);
        return ts;
    }

    /**
     * Utility function to convert a {@link BigDecimal} value to {@link Timestamp}.
     */
    public static Timestamp getTimestamp(BigDecimal bd) {
        return DateUtil.getTimestamp(bd.longValue(), ((bd.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(QueryConstants.MILLIS_TO_NANOS_CONVERTOR))).intValue()));
    }
}
