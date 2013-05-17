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
package com.salesforce.phoenix.schema;

import java.math.*;
import java.sql.*;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Longs;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.util.*;


/**
 * The data types of PColumns
 *
 * @author wmacklem
 * @author jtaylor
 * @since 0.1
 *
 * TODO: cleanup implementation to reduce copy/paste duplication
 */
@SuppressWarnings("rawtypes")
public enum PDataType {
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, null) {
        @Override
        public byte[] toBytes(Object object) {
            // TODO: consider using avro UTF8 object instead of String
            // so that we get get the size easily
            if (object == null) {
                return ByteUtil.EMPTY_BYTE_ARRAY;
            }
            return Bytes.toBytes((String)object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                return 0;
            }
            byte[] b = toBytes(object); // TODO: no byte[] allocation: use CharsetEncoder
            System.arraycopy(b, 0, bytes, offset, b.length);
            return b.length;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            return length == 0 ? null : Bytes.toString(bytes, offset, length);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            switch (actualType) {
            case VARCHAR:
            case CHAR:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // TODO: should CHAR not be here?
            return this == targetType || targetType == CHAR || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (isCoercibleTo(targetType)) {
                if (targetType == PDataType.CHAR) {
                    return value != null;
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if (srcType == PDataType.CHAR && maxLength != null && desiredMaxLength != null) {
                return maxLength <= desiredMaxLength;
            }
            return true;
        }

        @Override
        public boolean isFixedWidth() {
            return false;
        }

        @Override
        public int estimateByteSize(Object o) {
            String value = (String) o;
            return value == null ? 1 : value.length();
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return ((String)lhs).compareTo((String)rhs);
        }

        @Override
        public Object toObject(String value) {
            return value;
        }

        @Override
        protected boolean isBytesComparableWith(PDataType otherType) {
            return this == otherType || this == CHAR;
        }

    },
    /**
     * Fixed length single byte characters
     */
    CHAR("CHAR", Types.CHAR, String.class, null) { // Delegate to VARCHAR
        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            byte[] b = VARCHAR.toBytes(object);
            if (b.length != ((String) object).length()) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
            }
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            int len = VARCHAR.toBytes(object, bytes, offset);
            if (len != ((String) object).length()) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
            }
            return len;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) { // TODO: have isCoercibleTo that takes bytes, offset?
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            if (length == 0) {
                return null;
           }
           String s = Bytes.toString(bytes, offset, length);
           if (length != s.length()) {
               throw new IllegalDataException("CHAR types may only contain single byte characters (" + s + ")");
           }
           return s;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            switch (actualType) {
            case VARCHAR:
            case CHAR:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARCHAR || targetType == BINARY || targetType == VARBINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if ((srcType == PDataType.VARCHAR && ((String)value).length() != b.length) ||
                    (maxLength != null && desiredMaxLength != null && maxLength > desiredMaxLength)){
                return false;
            }
            return true;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public int estimateByteSize(Object o) {
            String value = (String) o;
            return value.length();
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return VARCHAR.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            if (StringUtil.hasMultiByteChars(value)) {
                throw new IllegalDataException("CHAR types may only contain single byte characters (" + value + ")");
            }
            return value;
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            return length;
        }

        @Override
        protected boolean isBytesComparableWith(PDataType otherType) {
            return this == otherType || this == VARCHAR;
        }
    },
    LONG("BIGINT", Types.BIGINT, Long.class, new LongCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
                return object;
            case UNSIGNED_INT:
            case INTEGER:
                int i = (Integer) object;
                return (long) i;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.longValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
            case INTEGER:
            case UNSIGNED_INT:
                return actualType.getCodec().decodeLong(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // In general, don't allow conversion of LONG to INTEGER. There are times when
            // we check isComparableTo for a more relaxed check and then throw a runtime
            // exception if we overflow
            return this == targetType || targetType == RAW_LONG
                    || targetType == UNSIGNED_LONG || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                long l;
                switch (targetType) {
                    case UNSIGNED_LONG:
                        l = (Long) value;
                        return l >= 0;
                    case UNSIGNED_INT:
                        l = (Long) value;
                        return (l >= 0 && l <= Integer.MAX_VALUE);
                    case INTEGER:
                        l = (Long) value;
                        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG;
        }

        @Override
        public Integer getMaxLength(Object o) {
            return LONG_PRECISION;
        }

        @Override
        public Integer getScale(Object o) {
            return ZERO;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).longValue()));
            }
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    INTEGER("INTEGER", Types.INTEGER, Integer.class, new IntCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeInt(((Number)object).intValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case UNSIGNED_INT:
                return object;
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
                long v = (Long)object;
                if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                    throw new IllegalDataException("Long value " + v + " cannot be cast to Integer without changing its value");
                }
                return (int)v;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.intValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
            case INTEGER:
            case UNSIGNED_INT:
                return actualType.getCodec().decodeInt(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_LONG:
                    case UNSIGNED_INT:
                        int i = (Integer) value;
                        return i >= 0;
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == LONG
                    || targetType == RAW_LONG || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_INT;
        }

        @Override
        public Integer getMaxLength(Object o) {
            return INT_PRECISION;
        }

        @Override
        public Integer getScale(Object o) {
            return ZERO;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return LONG.compareTo(lhs,rhs,rhsType);
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class, null) {
        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                return ByteUtil.EMPTY_BYTE_ARRAY;
            }
            BigDecimal v = (BigDecimal) object;
            v = NumberUtil.normalize(v);
            int len = getLength(v);
            byte[] result = new byte[Math.min(len, MAX_BIG_DECIMAL_BYTES)];
            PDataType.toBytes(v, result, 0, len);
            return result;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                return 0;
            }
            BigDecimal v = (BigDecimal) object;
            v = NumberUtil.normalize(v);
            int len = getLength(v);
            return PDataType.toBytes(v, bytes, offset, len);
        }

        private int getLength(BigDecimal v) {
            int signum = v.signum();
            if (signum == 0) { // Special case for zero
                return 1;
            }
            /*
             * Size of DECIMAL includes:
             * 1) one byte for exponent
             * 2) one byte for terminal byte if negative
             * 3) one byte for every two digits with the following caveats:
             *    a) add one to round up in the case when there is an odd number of digits
             *    b) add one in the case that the scale is odd to account for 10x of lowest significant digit
             *       (basically done to increase the range of exponents that can be represented)
             */
            return (signum < 0 ? 2 : 1) + (v.precision() +  1 + (v.scale() % 2 == 0 ? 0 : 1)) / 2;
        }

        @Override
        public int estimateByteSize(Object o) {
            if (o == null) {
                return 1;
            }
            BigDecimal v = (BigDecimal) o;
            // TODO: should we strip zeros and round here too?
            return Math.min(getLength(v),MAX_BIG_DECIMAL_BYTES);
        }

        @Override
        public Integer getMaxLength(Object o) {
            if (o == null) {
                return null;
            }
            BigDecimal v = (BigDecimal) o;
            return v.precision();
        }

        @Override
        public Integer getScale(Object o) {
            if (o == null) {
                return null;
            }
            BigDecimal v = (BigDecimal) o;
            return v.scale();
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case DECIMAL:
                return toBigDecimal(b, o, l);
            case LONG:
            case RAW_LONG:
            case INTEGER:
            case UNSIGNED_LONG:
            case UNSIGNED_INT:
                return BigDecimal.valueOf(actualType.getCodec().decodeLong(b, o, null));
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case UNSIGNED_INT:
                return BigDecimal.valueOf((Integer)object);
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
                return BigDecimal.valueOf((Long)object);
            case DECIMAL:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isFixedWidth() {
            return false;
        }

        @Override
        public Integer getByteSize() {
            return MAX_BIG_DECIMAL_BYTES;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return ((BigDecimal)lhs).compareTo((BigDecimal)rhs);
            }
            return -rhsType.compareTo(rhs, lhs, this);
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                BigDecimal bd;
                switch (targetType) {
                    case UNSIGNED_LONG:
                    case UNSIGNED_INT:
                        bd = (BigDecimal) value;
                        if (bd.signum() == -1) {
                            return false;
                        }
                    case LONG:
                    case RAW_LONG:
                        bd = (BigDecimal) value;
                        try {
                            bd.longValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    case INTEGER:
                        bd = (BigDecimal) value;
                        try {
                            bd.intValueExact();
                            return true;
                        } catch (ArithmeticException e) {
                            return false;
                        }
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            // Get precision and scale if it is not already passed in and either the object or byte values
            // is meaningful.
            if (maxLength == null && scale == null) {
                if (value != null) {
                    BigDecimal v = (BigDecimal) value;
                    maxLength = v.precision();
                    scale = v.scale();
                } else if (b != null && b.length > 0) {
                    int[] v = getDecimalPrecisionAndScale(b, 0, b.length);
                    maxLength = v[0];
                    scale = v[1];
                } else {
                    // the value does not contains maxLength nor scale. Just return true.
                    return true;
                }
            }
            if (desiredMaxLength != null && desiredScale != null && maxLength != null && scale != null &&
                    (desiredMaxLength - desiredScale) < (maxLength - scale)) {
                return false;
            }
            return true;
        }

        @Override
        public byte[] coerceBytes(byte[] b, Object object, PDataType actualType, Integer maxLength, Integer scale,
                Integer desiredMaxLength, Integer desiredScale) {
            if (desiredScale == null) {
                // scale or deiredScale not available, delegate to parents.
                return super.coerceBytes(b, object, actualType);
            }
            if (scale == null) {
                if (object != null) {
                    BigDecimal v = (BigDecimal) object;
                    scale = v.scale();
                } else if (b != null && b.length > 0) {
                    int[] v = getDecimalPrecisionAndScale(b, 0, b.length);
                    scale = v[1];
                } else {
                    // Neither the object value nor byte value is meaningful, delegate to super.
                    return super.coerceBytes(b, object, actualType);
                }
            }
            if (this == actualType && scale <= desiredScale) {
                // No coerce and rescale necessary
                return b;
            } else {
                BigDecimal decimal;
                // Rescale is necessary.
                if (object != null) { // value object is passed in.
                    decimal = (BigDecimal) toObject(object, actualType);
                } else { // only value bytes is passed in, need to convert to object first.
                    decimal = (BigDecimal) toObject(b);
                }
                decimal = decimal.setScale(desiredScale, BigDecimal.ROUND_DOWN);
                return toBytes(decimal);
            }
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            // No association of runtime byte size from decimal precision.
            return null;
        }
    },
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class, new DateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            byte[] bytes = new byte[getByteSize()];
            toBytes(object, bytes, 0);
            return bytes;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            int size = Bytes.SIZEOF_LONG;
            Timestamp value = (Timestamp)object;
            offset = Bytes.putLong(bytes, offset, value.getTime());
            Bytes.putInt(bytes, offset, value.getNanos());
            size += Bytes.SIZEOF_INT;
            return size;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
                return new Timestamp(((Date)object).getTime());
            case TIME:
                return new Timestamp(((Time)object).getTime());
            case TIMESTAMP:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP:
                Timestamp v = new Timestamp(Bytes.toLong(b, o, Bytes.SIZEOF_LONG));
                v.setNanos(Bytes.toInt(b, o + Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT));
                return v;
            case DATE:
            case TIME:
                return new Timestamp(getCodec().decodeLong(b, o, null));
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }
        
        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == DATE || targetType == TIME 
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == TIMESTAMP) {
                return ((Timestamp)lhs).compareTo((Timestamp)rhs);
            }
            int c = ((Date)rhs).compareTo((Date)lhs);
            if (c != 0) return c;
            return ((Timestamp)lhs).getNanos();
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseTimestamp(value);
        }
    },
    TIME("TIME", Types.TIME, Time.class, new DateCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return Bytes.toBytes(((Time)object).getTime());
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            Bytes.putLong(bytes, offset, ((Time)object).getTime());
            return this.getByteSize();
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP: // TODO: throw if nanos?
            case DATE:
            case TIME:
                return new Time(this.getCodec().decodeLong(b, o, null));
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case DATE:
                return new Time(((Date)object).getTime());
            case TIMESTAMP:
                return new Time(((Timestamp)object).getTime());
            case TIME:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == DATE || targetType == TIMESTAMP
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == TIMESTAMP) {
                return -TIMESTAMP.compareTo(rhs, lhs, TIME);
            }
            return ((Date)rhs).compareTo((Date)lhs);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseTime(value);
        }

        @Override
        protected boolean isBytesComparableWith(PDataType otherType) {
            return this == otherType || this == DATE;
        }
    },
    DATE("DATE", Types.DATE, Date.class, new DateCodec()) { // After TIMESTAMP and DATE to ensure toLiteral finds those first

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return Bytes.toBytes(((Date)object).getTime());
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            Bytes.putLong(bytes, offset, ((Date)object).getTime());
            return this.getByteSize();
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case TIME:
                return new Date(((Time)object).getTime());
            case TIMESTAMP:
                return new Date(((Timestamp)object).getTime());
            case DATE:
                return object;
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case TIMESTAMP: // TODO: throw if nanos?
            case DATE:
            case TIME:
                return new Date(this.getCodec().decodeLong(b, o, null));
            default:
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == TIME || targetType == TIMESTAMP
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return TIME.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return DateUtil.parseDate(value);
        }

        @Override
        protected boolean isBytesComparableWith(PDataType otherType) {
            return this == otherType || this == TIME;
        }
    },
    /**
     * Unsigned long type that restricts values to be from 0 to {@link java.lang.Long#MAX_VALUE} inclusive. May be used to map to existing HTable values created through {@link org.apache.hadoop.hbase.util.Bytes#toBytes(long)}
     * as long as all values are non negative (the leading sign bit of negative numbers would cause them to sort ahead of positive numbers when
     * they're used as part of the row key when using the HBase utility methods).
     */
    UNSIGNED_LONG("UNSIGNED_LONG", 10 /* no constant available in Types */, Long.class, new UnsignedLongCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_LONG:
                return object;
            case LONG:
            case RAW_LONG:
                long l = (Long)object;
                if (l < 0) {
                    throw new IllegalDataException();
                }
                return object;
            case UNSIGNED_INT:
            case INTEGER:
                int i = (Integer) object;
                if (i < 0) {
                    throw new IllegalDataException();
                }
                return (long) i;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                if (d.signum() == -1) {
                    throw new IllegalDataException();
                }
                return d.longValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_INT:
                return actualType.getCodec().decodeLong(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == LONG
                    || targetType == RAW_LONG || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                switch (targetType) {
                    case UNSIGNED_INT:
                    case INTEGER:
                        long l = (Long) value;
                        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).longValue()));
            }
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                Long l = Long.parseLong(value);
                if (l.longValue() < 0) {
                    throw new IllegalDataException("Value may not be negative(" + l + ")");
                }
                return l;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },

    RAW_LONG("RAW_LONG", 110 /* no constant available in Types */, Long.class, new RawLongCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_LONG];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeLong(((Number)object).longValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case RAW_LONG:
            case UNSIGNED_LONG:
            case LONG:
                return object;
            case UNSIGNED_INT:
            case INTEGER:
                int i = (Integer) object;
                return (long) i;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.longValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case INTEGER:
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
            case UNSIGNED_INT:
                return actualType.getCodec().decodeLong(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            // In general, don't allow conversion of LONG to INTEGER. There are times when
            // we check isComparableTo for a more relaxed check and then throw a runtime
            // exception if we overflow
            return this == targetType || targetType == LONG || targetType == UNSIGNED_LONG
                    || targetType == DECIMAL || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType, Object value) {
            if (value != null) {
                long l;
                switch (targetType) {
                    case UNSIGNED_LONG:
                        l = (Long) value;
                        return l >= 0;
                    case UNSIGNED_INT:
                        l = (Long) value;
                        return (l >= 0 && l <= Integer.MAX_VALUE);
                    case INTEGER:
                        l = (Long) value;
                        return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE);
                    default:
                        break;
                }
            }
            return super.isCoercibleTo(targetType, value);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_LONG;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (rhsType == DECIMAL) {
                return -((BigDecimal)rhs).compareTo(BigDecimal.valueOf(((Number)lhs).longValue()));
            }
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        // We need to override this case negative RAW_LONG type could not be able to compare with bytes directly.
        @Override
        public int compareTo(byte[] b1, int offset1, int length1, ColumnModifier mod1, byte[] b2, int offset2, int length2, ColumnModifier mod2) {
            Object lhs = this.toObject(b1, offset1, length1, this, mod1);
            Object rhs = this.toObject(b2, offset2, length2, this, mod2);
            return Longs.compare(((Number)lhs).longValue(), ((Number)rhs).longValue());
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    
    
    /**
     * Unsigned integer type that restricts values to be from 0 to {@link java.lang.Integer#MAX_VALUE} inclusive. May be used to map to existing HTable values created through {@link org.apache.hadoop.hbase.util.Bytes#toBytes(int)}
     * as long as all values are non negative (the leading sign bit of negative numbers would cause them to sort ahead of positive numbers when
     * they're used as part of the row key when using the HBase utility methods).
     */
    UNSIGNED_INT("UNSIGNED_INT", 9 /* no constant available in Types */, Integer.class, new UnsignedIntCodec()) {

        @Override
        public byte[] toBytes(Object object) {
            byte[] b = new byte[Bytes.SIZEOF_INT];
            toBytes(object, b, 0);
            return b;
        }

        @Override
        public int toBytes(Object object, byte[] b, int o) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return this.getCodec().encodeInt(((Number)object).intValue(), b, o);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            if (object == null) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_INT:
                return object;
            case INTEGER:
                Integer i = (Integer) object;
                if (i < 0) {
                    throw new IllegalDataException();
                }
                return i;
            case LONG:
            case RAW_LONG:
            case UNSIGNED_LONG:
                long v = (Long)object;
                if (v < 0 || v > Integer.MAX_VALUE) {
                    throw new IllegalDataException("Long value " + v + " cannot be cast to Unsigned Integer without changing its value");
                }
                return (int)v;
            case DECIMAL:
                BigDecimal d = (BigDecimal)object;
                return d.intValueExact();
            default:
                return super.toObject(object, actualType);
            }
        }

        @Override
        public Object toObject(byte[] b, int o, int l, PDataType actualType) {
            if (l == 0) {
                return null;
            }
            switch (actualType) {
            case UNSIGNED_LONG:
            case LONG:
            case RAW_LONG:
            case UNSIGNED_INT:
            case INTEGER:
                return actualType.getCodec().decodeInt(b, o, null);
            default:
                return super.toObject(b,o,l,actualType);
            }
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == INTEGER
                    || targetType == UNSIGNED_LONG || targetType == LONG
                    || targetType == RAW_LONG || targetType == DECIMAL
                    || targetType == VARBINARY || targetType == BINARY;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return Bytes.SIZEOF_INT;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return LONG.compareTo(lhs, rhs, rhsType);
        }

        @Override
        public boolean isComparableTo(PDataType targetType) {
            return DECIMAL.isComparableTo(targetType);
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            try {
                Integer i = Integer.parseInt(value);
                if (i.intValue() < 0) {
                    throw new IllegalDataException("Value may not be negative(" + i + ")");
                }
                return i;
            } catch (NumberFormatException e) {
                throw new IllegalDataException(e);
            }
        }
    },
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, null) {

        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return ((Boolean)object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            bytes[offset] = ((Boolean)object).booleanValue() ? TRUE_BYTE : FALSE_BYTE;
            return BOOLEAN_LENGTH;
        }

        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            // Override to prevent any byte allocation
            if (columnModifier == null) {
                return ((Boolean)object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
            }
            return ((Boolean)object).booleanValue() ? MOD_TRUE_BYTES[columnModifier.ordinal()] : MOD_FALSE_BYTES[columnModifier.ordinal()];
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType targetType) {
            if (!isCoercibleTo(targetType)) {
                throw new ConstraintViolationException(this + " cannot be coerced to " + targetType);
            }
            return length == 0 ? null : bytes[offset] == FALSE_BYTE ? Boolean.FALSE : Boolean.TRUE;
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public Integer getByteSize() {
            return BOOLEAN_LENGTH;
        }

        @Override
        public int estimateByteSize(Object o) {
            return BOOLEAN_LENGTH;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            return Booleans.compare((Boolean)lhs, (Boolean)rhs);
        }

        @Override
        public Object toObject(String value) {
            return Boolean.parseBoolean(value);
        }
    },
    VARBINARY("VARBINARY", Types.VARBINARY, byte[].class, null) {
        @Override
        public byte[] toBytes(Object object) {
            if (object == null) {
                return ByteUtil.EMPTY_BYTE_ARRAY;
            }
            return (byte[])object;
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                return 0;
            }
            byte[] o = (byte[])object;
            // assumes there's enough room
            System.arraycopy(bytes, offset, o, 0, o.length);
            return o.length;
        }

        /**
         * Override because we must always create a new byte array
         */
        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            byte[] bytes = toBytes(object);
            // Override because we need to allocate a new buffer in this case
            if (columnModifier != null) {
                return columnModifier.apply(bytes, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (length == 0) {
                return null;
            }
            if (offset == 0 && bytes.length == length) {
                return bytes;
            }
            byte[] o = new byte[length];
            System.arraycopy(bytes, offset, o, 0, length);
            return o;
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            return actualType.toBytes(object);
        }

        @Override
        public boolean isFixedWidth() {
            return false;
        }

        @Override
        public int estimateByteSize(Object o) {
            byte[] value = (byte[]) o;
            return value == null ? 1 : value.length;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == BINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if (srcType == PDataType.BINARY && maxLength != null && desiredMaxLength != null) {
                return maxLength <= desiredMaxLength;
            }
            return true;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (lhs == null && rhs == null) {
                return 0;
            } else if (lhs == null) {
                return -1;
            } else if (rhs == null) {
                return 1;
            }
            if (rhsType == PDataType.VARBINARY || rhsType == PDataType.BINARY) {
                return Bytes.compareTo((byte[])lhs, (byte[])rhs);
            } else {
                byte[] rhsBytes = rhsType.toBytes(rhs);
                return Bytes.compareTo((byte[])lhs, rhsBytes);
            }
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return Base64.decode(value);
        }
    },
    BINARY("BINARY", Types.BINARY, byte[].class, null) {
        @Override
        public byte[] toBytes(Object object) { // Deligate to VARBINARY
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return VARBINARY.toBytes(object);
        }

        @Override
        public int toBytes(Object object, byte[] bytes, int offset) {
            if (object == null) {
                throw new ConstraintViolationException(this + " may not be null");
            }
            return VARBINARY.toBytes(object, bytes, offset);
            
        }

        @Override
        public byte[] toBytes(Object object, ColumnModifier columnModifier) {
            byte[] bytes = toBytes(object);
            if (columnModifier != null) {
                return columnModifier.apply(bytes, new byte[bytes.length], 0, bytes.length);
            }
            return bytes;
        }

        @Override
        public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) {
            if (!actualType.isCoercibleTo(this)) {
                throw new ConstraintViolationException(actualType + " cannot be coerced to " + this);
            }
            return VARBINARY.toObject(bytes, offset, length, actualType);
        }

        @Override
        public Object toObject(Object object, PDataType actualType) {
            return actualType.toBytes(object);
        }

        @Override
        public boolean isFixedWidth() {
            return true;
        }

        @Override
        public int estimateByteSize(Object o) {
            byte[] value = (byte[]) o;
            return value == null ? 1 : value.length;
        }

        @Override
        public boolean isCoercibleTo(PDataType targetType) {
            return this == targetType || targetType == VARBINARY;
        }

        @Override
        public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
                Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
            if ((srcType == PDataType.VARBINARY && ((String)value).length() != b.length) ||
                    (maxLength != null && desiredMaxLength != null && maxLength > desiredMaxLength)){
                return false;
            }
            return true;
        }

        @Override
        public Integer estimateByteSizeFromLength(Integer length) {
            return length;
        }

        @Override
        public Integer getByteSize() {
            return null;
        }

        @Override
        public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
            if (lhs == null && rhs == null) {
                return 0;
            } else if (lhs == null) {
                return -1;
            } else if (rhs == null) {
                return 1;
            }
            if (rhsType == PDataType.VARBINARY || rhsType == PDataType.BINARY) {
                return Bytes.compareTo((byte[])lhs, (byte[])rhs);
            } else {
                byte[] rhsBytes = rhsType.toBytes(rhs);
                return Bytes.compareTo((byte[])lhs, rhsBytes);
            }
        }

        @Override
        public Object toObject(String value) {
            if (value == null || value.length() == 0) {
                return null;
            }
            return Base64.decode(value);
        }
    },
    ;

    private final String sqlTypeName;
    private final int sqlType;
    private final Class clazz;
    private final byte[] clazzNameBytes;
    private final byte[] sqlTypeNameBytes;
    private final PDataCodec codec;

    private PDataType(String sqlTypeName, int sqlType, Class clazz, PDataCodec codec) {
        this.sqlTypeName = sqlTypeName;
        this.sqlType = sqlType;
        this.clazz = clazz;
        this.clazzNameBytes = Bytes.toBytes(clazz.getName());
        this.sqlTypeNameBytes = Bytes.toBytes(sqlTypeName);
        this.codec = codec;
    }

    public final PDataCodec getCodec() {
        return codec;
    }

    protected boolean isBytesComparableWith(PDataType otherType) {
        return this == otherType;
    }

    public int estimateByteSize(Object o) {
        if (isFixedWidth()) {
            return getByteSize();
        }
        // Non fixed width types must override this
        throw new UnsupportedOperationException();
    }

    public Integer getMaxLength(Object o) {
        return null;
    }

    public Integer getScale(Object o) {
        return null;
    }

    /**
     * Estimate the byte size from the type length. For example, for char, byte size would be the
     * same as length. For decimal, byte size would have no correlation with the length.
     */
    public Integer estimateByteSizeFromLength(Integer length) {
        if (isFixedWidth()) {
            return getByteSize();
        }
        // If not fixed width, default to say the byte size is the same as length.
        return length;
    }

    public final String getSqlTypeName() {
        return sqlTypeName;
    }

    public final int getSqlType() {
        return sqlType;
    }

    public final Class getJavaClass() {
        return clazz;
    }

    public final int compareTo(byte[] lhs, int lhsOffset, int lhsLength, ColumnModifier lhsColumnModifier,
                               byte[] rhs, int rhsOffset, int rhsLength, ColumnModifier rhsColumnModifier, PDataType rhsType) {
        if (this.isBytesComparableWith(rhsType)) { // directly compare the bytes
            return compareTo(lhs, lhsOffset, lhsLength, lhsColumnModifier, rhs, rhsOffset, rhsLength, rhsColumnModifier);
        }
        PDataCodec lhsCodec = this.getCodec();
        if (lhsCodec == null) { // no lhs native type representation, so convert rhsType to bytes representation of lhsType
            byte[] rhsConverted = this.toBytes(this.toObject(rhs, rhsOffset, rhsLength, rhsType, rhsColumnModifier));            
            if (rhsColumnModifier != null) {
                rhsColumnModifier = null;
            }            
            if (lhsColumnModifier != null) {
                lhs = lhsColumnModifier.apply(lhs, new byte[lhs.length], lhsOffset, lhsLength);
            }                        
            return Bytes.compareTo(lhs, lhsOffset, lhsLength, rhsConverted, 0, rhsConverted.length);
        }
        PDataCodec rhsCodec = rhsType.getCodec();
        if (rhsCodec == null) {
            byte[] lhsConverted = rhsType.toBytes(rhsType.toObject(lhs, lhsOffset, lhsLength, this, lhsColumnModifier));
            if (lhsColumnModifier != null) {
                lhsColumnModifier = null;
            }
            if (rhsColumnModifier != null) {
                rhs = rhsColumnModifier.apply(rhs, new byte[rhs.length], rhsOffset, rhsLength);
            }            
            return Bytes.compareTo(lhsConverted, 0, lhsConverted.length, rhs, rhsOffset, rhsLength);
        }
        // convert to native and compare
        return Longs.compare(this.getCodec().decodeLong(lhs, lhsOffset, lhsColumnModifier), rhsType.getCodec().decodeLong(rhs, rhsOffset, rhsColumnModifier));
    }

    public static interface PDataCodec {
        public long decodeLong(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier);
        public int decodeInt(ImmutableBytesWritable ptr, ColumnModifier columnModifier);
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier);

        public int encodeLong(long v, ImmutableBytesWritable ptr);
        public int encodeLong(long v, byte[] b, int o);
        public int encodeInt(int v, ImmutableBytesWritable ptr);
        public int encodeInt(int v, byte[] b, int o);
    }

    public static abstract class BaseCodec implements PDataCodec {
        @Override
        public int decodeInt(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeInt(ptr.get(), ptr.getOffset(), columnModifier);
        }

        @Override
        public long decodeLong(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
            return decodeLong(ptr.get(),ptr.getOffset(), columnModifier);
        }

        @Override
        public int encodeInt(int v, ImmutableBytesWritable ptr) {
            return encodeInt(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeLong(long v, ImmutableBytesWritable ptr) {
            return encodeLong(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }
    }


    public static class LongCodec extends BaseCodec {

        private LongCodec() {
        }

        @Override
        public long decodeLong(byte[] bytes, int o, ColumnModifier columnModifier) {
            long v;
            byte b = bytes[o];
            if (columnModifier == null) {
                v = b ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
                    b = bytes[o + i];
                    v = (v << 8) + (b & 0xff);
                }
            } else { // ColumnModifier.SORT_DESC
                b = (byte)(b ^ 0xff);
                v = b ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
                    b = bytes[o + i];
                    b ^= 0xff;
                    v = (v << 8) + (b & 0xff);
                }
            }
            return v;
        }


        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            long v = decodeLong(b, o, columnModifier);
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be cast to Integer without changing its value");
            }
            return (int)v;
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            b[o + 0] = (byte) ((v >> 56) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
            b[o + 1] = (byte) (v >> 48);
            b[o + 2] = (byte) (v >> 40);
            b[o + 3] = (byte) (v >> 32);
            b[o + 4] = (byte) (v >> 24);
            b[o + 5] = (byte) (v >> 16);
            b[o + 6] = (byte) (v >> 8);
            b[o + 7] = (byte) v;
            return Bytes.SIZEOF_LONG;
        }
    }

    public static class RawLongCodec extends LongCodec {

        private RawLongCodec() {
        }

        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            long v = 0;
            if (columnModifier == null) {
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                  v <<= 8;
                  v ^= b[i] & 0xFF;
                }
            } else { // ColumnModifier.SORT_DESC
                // FIXME : Actually, since RAW_LONG could only be used in Non PK column
                // we should never reach here. Add an assert?
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                    v <<= 8;
                    v ^= (b[i] & 0xFF) ^ 0xFF;
                  }
            }
            return v;
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            Bytes.putLong(b, o, v);
            return Bytes.SIZEOF_LONG;
        }
    }

    public static class IntCodec extends BaseCodec {

        private IntCodec() {
        }

        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            return decodeInt(b, o, columnModifier);
        }

        @Override
        public int decodeInt(byte[] bytes, int o, ColumnModifier columnModifier) {            
            int v;
            if (columnModifier == null) {
                v = bytes[o] ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
                    v = (v << 8) + (bytes[o + i] & 0xff);
                }
            } else { // ColumnModifier.SORT_DESC
                v = bytes[o] ^ 0xff ^ 0x80; // Flip sign bit back
                for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
                    v = (v << 8) + ((bytes[o + i] ^ 0xff) & 0xff);
                }
            }
            return v;
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
            b[o + 1] = (byte) (v >> 16);
            b[o + 2] = (byte) (v >> 8);
            b[o + 3] = (byte) v;
            return Bytes.SIZEOF_INT;
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalDataException("Value " + v + " cannot be encoded as an Integer without changing its value");
            }
            return encodeInt((int)v,b,o);
        }
    }

    public static class UnsignedLongCodec extends LongCodec {

        private UnsignedLongCodec() {
        }

        @Override
        public long decodeLong(byte[] b, int o, ColumnModifier columnModifier) {
            long v = 0;
            if (columnModifier == null) {
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                  v <<= 8;
                  v ^= b[i] & 0xFF;
                }
            } else { // ColumnModifier.SORT_DESC
                for(int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
                    v <<= 8;
                    v ^= (b[i] & 0xFF) ^ 0xFF;
                  }
            }
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putLong(b, o, v);
            return Bytes.SIZEOF_LONG;
        }
    }

    public static class UnsignedIntCodec extends IntCodec {

        private UnsignedIntCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            if (columnModifier != null) {
                b = columnModifier.apply(b, new byte[b.length], o, Bytes.SIZEOF_INT);
            }
            int v = Bytes.toInt(b, o);
            if (v < 0) {
                throw new IllegalDataException();
            }
            return v;
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            if (v < 0) {
                throw new IllegalDataException();
            }
            Bytes.putInt(b, o, v);
            return Bytes.SIZEOF_INT;
        }
    }

    public static class DateCodec extends UnsignedLongCodec {

        private DateCodec() {
        }

        @Override
        public int decodeInt(byte[] b, int o, ColumnModifier columnModifier) {
            throw new UnsupportedOperationException();
        }
    }

    public static final int MAX_PRECISION = 31; // Max precision guaranteed to fit into a long (and this should be plenty)
    public static final int MIN_DECIMAL_AVG_SCALE = 4;
    public static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);
    public static final int DEFAULT_SCALE = 0;

    private static final Integer MAX_BIG_DECIMAL_BYTES = 21;

    private static final byte ZERO_BYTE = (byte)0x80;
    private static final byte NEG_TERMINAL_BYTE = (byte)102;
    private static final int EXP_BYTE_OFFSET = 65;
    private static final int POS_DIGIT_OFFSET = 1;
    private static final int NEG_DIGIT_OFFSET = 101;
    private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    private static final long MAX_LONG_FOR_DESERIALIZE = Long.MAX_VALUE / 1000;
    private static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);

    private static final byte FALSE_BYTE = 0;
    private static final byte TRUE_BYTE = 1;
    public static final byte[] FALSE_BYTES = new byte[] {FALSE_BYTE};
    public static final byte[] TRUE_BYTES = new byte[] {TRUE_BYTE};
    public static final byte[] MOD_FALSE_BYTES[] = new byte[ColumnModifier.values().length][];
    static {
        for (ColumnModifier columnModifier : ColumnModifier.values()) {
            MOD_FALSE_BYTES[columnModifier.ordinal()] = columnModifier.apply(FALSE_BYTES, new byte[FALSE_BYTES.length], 0, FALSE_BYTES.length);
        }
    }
    public static final byte[] MOD_TRUE_BYTES[] = new byte[ColumnModifier.values().length][];
    static {
        for (ColumnModifier columnModifier : ColumnModifier.values()) {
            MOD_TRUE_BYTES[columnModifier.ordinal()] = columnModifier.apply(TRUE_BYTES, new byte[TRUE_BYTES.length], 0, TRUE_BYTES.length);
        }
    }
    public static final byte[] NULL_BYTES = ByteUtil.EMPTY_BYTE_ARRAY;
    private static final Integer BOOLEAN_LENGTH = 1;

    public final static Integer ZERO = 0;
    public final static Integer INT_PRECISION = 10;
    public final static Integer LONG_PRECISION = 19;

    /**
     * Serialize a BigDecimal into a variable length byte array in such a way that it is
     * binary comparable.
     * @param v the BigDecimal
     * @param result the byte array to contain the serialized bytes.  Max size
     * necessary would be 21 bytes.
     * @param length the number of bytes required to store the big decimal. May be
     * adjusted down if it exceeds {@link #MAX_BIG_DECIMAL_BYTES}
     * @return the number of bytes that make up the serialized BigDecimal
     */
    private static int toBytes(BigDecimal v, byte[] result, final int offset, int length) {
        // From scale to exponent byte (if BigDecimal is positive):  (-(scale+(scale % 2 == 0 : 0 : 1)) / 2 + 65) | 0x80
        // If scale % 2 is 1 (i.e. it's odd), then multiple last base-100 digit by 10
        // For example: new BigDecimal(BigInteger.valueOf(1), -4);
        // (byte)((-(-4+0) / 2 + 65) | 0x80) = -61
        // From scale to exponent byte (if BigDecimal is negative): ~(-(scale+1)/2 + 65 + 128) & 0x7F
        // For example: new BigDecimal(BigInteger.valueOf(1), 2);
        // ~(-2/2 + 65 + 128) & 0x7F = 63
        int signum = v.signum();
        if (signum == 0) {
            result[offset] = ZERO_BYTE;
            return 1;
        }
        int index = offset + length;
        int scale = v.scale();
        int expOffset = scale % 2 * (scale < 0 ? -1 : 1);
        // In order to get twice as much of a range for scale, it
        // is multiplied by 2. If the scale is an odd number, then
        // the first digit is multiplied by 10 to make up for the
        // scale being off by one.
        int multiplyBy;
        BigInteger divideBy;
        if (expOffset == 0) {
            multiplyBy = 1;
            divideBy = ONE_HUNDRED;
        } else {
            multiplyBy = 10;
            divideBy = BigInteger.TEN;
        }
        // Normalize the scale based on what is necessary to end up with a base 100 decimal (i.e. 10.123e3)
        int digitOffset;
        BigInteger compareAgainst;
        if (signum == 1) {
            digitOffset = POS_DIGIT_OFFSET;
            compareAgainst = MAX_LONG;
            scale -= (length - 2) * 2;
            result[offset] = (byte)((-(scale+expOffset)/2 + EXP_BYTE_OFFSET) | 0x80);
        } else {
            digitOffset = NEG_DIGIT_OFFSET;
            compareAgainst = MIN_LONG;
            // Scale adjustment shouldn't include terminal byte in length
            scale -= (length - 2 - 1) * 2;
            result[offset] = (byte)(~(-(scale+expOffset)/2 + EXP_BYTE_OFFSET + 128) & 0x7F);
            if (length <= MAX_BIG_DECIMAL_BYTES) {
                result[--index] = NEG_TERMINAL_BYTE;
            } else {
                // Adjust length and offset down because we don't have enough room
                length = MAX_BIG_DECIMAL_BYTES;
                index = offset + length - 1;
            }
        }
        BigInteger bi = v.unscaledValue();
        // Use BigDecimal arithmetic until we can fit into a long
        while (bi.compareTo(compareAgainst) * signum > 0) {
            BigInteger[] dandr = bi.divideAndRemainder(divideBy);
            bi = dandr[0];
            int digit = dandr[1].intValue();
            result[--index] = (byte)(signum * digit * multiplyBy + digitOffset);
            multiplyBy = 1;
            divideBy = ONE_HUNDRED;
        }
        long l = bi.longValue();
        do {
            long divBy = 100/multiplyBy;
            long digit = l % divBy;
            l /= divBy;
            result[--index] = (byte)(digit * multiplyBy + digitOffset);
            multiplyBy = 1;
        } while (l != 0);

        return length;
    }

    /**
     * Deserialize a variable length byte array into a BigDecimal. Note that because of
     * the normalization that gets done to the scale, if you roundtrip a BigDecimal,
     * it may not be equal before and after. However, the before and after number will
     * always compare to be equal (i.e. <nBefore>.compareTo(<nAfter>) == 0)
     * @param bytes the bytes containing the number
     * @param offset the offset into the byte array
     * @param length the length of the serialized BigDecimal
     * @return the BigDecimal value.
     */
    private static BigDecimal toBigDecimal(byte[] bytes, int offset, int length) {
        // From exponent byte back to scale: (<exponent byte> & 0x7F) - 65) * 2
        // For example, (((-63 & 0x7F) - 65) & 0xFF) * 2 = 0
        // Another example: ((-64 & 0x7F) - 65) * 2 = -2 (then swap the sign for the scale)
        // If number is negative, going from exponent byte back to scale: (byte)((~<exponent byte> - 65 - 128) * 2)
        // For example: new BigDecimal(new BigInteger("-1"), -2);
        // (byte)((~61 - 65 - 128) * 2) = 2, so scale is -2
        // Potentially, when switching back, the scale can be added by one and the trailing zero dropped
        // For digits, just do a mod 100 on the BigInteger. Use long if BigInteger fits
        if (length == 1 && bytes[offset] == ZERO_BYTE) {
            return BigDecimal.ZERO;
        }
        int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
        int scale;
        int index;
        int digitOffset;
        long multiplier = 100L;
        int begIndex = offset + 1;
        if (signum == 1) {
            scale = (byte)(((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte)((~bytes[offset] - 65 - 128) * -2);
            index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
            digitOffset = -NEG_DIGIT_OFFSET;
        }
        length = index - offset;
        long l = signum * bytes[--index] - digitOffset;
        if (l % 10 == 0) { // trailing zero
            scale--; // drop trailing zero and compensate in the scale
            l /= 10;
            multiplier = 10;
        }
        // Use long arithmetic for as long as we can
        while (index > begIndex) {
            if (l >= MAX_LONG_FOR_DESERIALIZE || multiplier >= Long.MAX_VALUE / 100) {
                multiplier = LongMath.divide(multiplier, 100L, RoundingMode.UNNECESSARY);
                break; // Exit loop early so we don't overflow our multiplier
            }
            int digit100 = signum * bytes[--index] - digitOffset;
            l += digit100*multiplier;
            multiplier = LongMath.checkedMultiply(multiplier, 100);
        }

        BigInteger bi;
        // If still more digits, switch to BigInteger arithmetic
        if (index > begIndex) {
            bi = BigInteger.valueOf(l);
            BigInteger biMultiplier = BigInteger.valueOf(multiplier).multiply(ONE_HUNDRED);
            do {
                int digit100 = signum * bytes[--index] - digitOffset;
                bi = bi.add(biMultiplier.multiply(BigInteger.valueOf(digit100)));
                biMultiplier = biMultiplier.multiply(ONE_HUNDRED);
            } while (index > begIndex);
            if (signum == -1) {
                bi = bi.negate();
            }
        } else {
            bi = BigInteger.valueOf(l * signum);
        }
        // Update the scale based on the precision
        scale += (length - 2) * 2;
        BigDecimal v = new BigDecimal(bi, scale);
        return v;
    }

    // Calculate the precisioin and scale of a raw decimal bytes. Returns the values as an int
    // array. The first value is precision, the second value is scale.
    public static int[] getDecimalPrecisionAndScale(byte[] bytes, int offset, int length) {
        // 0, which should have no precision nor scale.
        if (length == 1 && bytes[offset] == ZERO_BYTE) {
            return new int[] {0, 0};
        }
        int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
        int scale;
        int index;
        int digitOffset;
        if (signum == 1) {
            scale = (byte)(((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte)((~bytes[offset] - 65 - 128) * -2);
            index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
            digitOffset = -NEG_DIGIT_OFFSET;
        }
        length = index - offset;
        int precision = 2 * (length - 1);
        int d = signum * bytes[--index] - digitOffset;
        if (d % 10 == 0) { // trailing zero
            // drop trailing zero and compensate in the scale and precision.
            d /= 10;
            scale--;
            precision -= 1;
        }
        d = signum * bytes[offset+1] - digitOffset;
        if (d < 10) { // Leading single digit
            // Compensate in the precision.
            precision -= 1;
        }
        // Update the scale based on the precision
        scale += (length - 2) * 2;
        if (scale < 0) {
            precision = precision - scale;
            scale = 0;
        }
        return new int[] {precision, scale};
    }

    public boolean isCoercibleTo(PDataType targetType) {
        return this == targetType || targetType == VARBINARY;
    }

    // Specialized on enums to take into account type hierarchy (i.e. UNSIGNED_LONG is comparable to INTEGER)
    public boolean isComparableTo(PDataType targetType) {
        return targetType.isCoercibleTo(this) || this.isCoercibleTo(targetType);
    }

    public boolean isCoercibleTo(PDataType targetType, Object value) {
        return isCoercibleTo(targetType);
    }

    public boolean isSizeCompatible(PDataType srcType, Object value, byte[] b,
            Integer maxLength, Integer desiredMaxLength, Integer scale, Integer desiredScale) {
        return true;
    }

    public int compareTo(byte[] b1, byte[] b2) {
        return compareTo(b1, 0, b1.length, null, b2, 0, b2.length, null);
    }

    public int compareTo(ImmutableBytesWritable ptr1, ImmutableBytesWritable ptr2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), null, ptr2.get(), ptr2.getOffset(), ptr2.getLength(), null);
    }

    public int compareTo(byte[] b1, int offset1, int length1, ColumnModifier mod1, byte[] b2, int offset2, int length2, ColumnModifier mod2) {
        int resultMultiplier = -1;
        // TODO: have compare go through ColumnModifier?
        boolean invertResult = (mod1 == ColumnModifier.SORT_DESC && mod2 == ColumnModifier.SORT_DESC);
        if (!invertResult) {
            if (mod1 != null) {
                b1 = mod1.apply(b1, new byte[b1.length], offset1, length1);
            }
            if (mod2 != null) {
                b2 = mod2.apply(b2, new byte[b2.length], offset2, length2);
            }            
            resultMultiplier = 1;
        }
        return Bytes.compareTo(b1, offset1, length1, b2, offset2, length2) * resultMultiplier;
    }

    public int compareTo(ImmutableBytesWritable ptr1, ColumnModifier ptr1ColumnModifier, ImmutableBytesWritable ptr2, ColumnModifier ptr2ColumnModifier, PDataType type2) {
        return compareTo(ptr1.get(), ptr1.getOffset(), ptr1.getLength(), ptr1ColumnModifier, ptr2.get(), ptr2.getOffset(), ptr2.getLength(), ptr2ColumnModifier, type2);
    }

    public abstract int compareTo(Object lhs, Object rhs, PDataType rhsType);

    public int compareTo(Object lhs, Object rhs) {
        return compareTo(lhs,rhs,this);
    }

    public abstract boolean isFixedWidth();
    public abstract Integer getByteSize();

    public abstract byte[] toBytes(Object object);
    
    public byte[] toBytes(Object object, ColumnModifier columnModifier) {
    	byte[] bytes = toBytes(object);
    	if (columnModifier != null) {
            columnModifier.apply(bytes, bytes, 0, bytes.length);
    	}
    	return bytes;
    }

    /**
     * Convert from the object representation of a data type value into
     * the serialized byte form.
     * @param object the object to convert
     * @param bytes the byte array into which to put the serialized form of object
     * @param offset the offset from which to start writing the serialized form
     * @return the byte length of the serialized object
     */
    public abstract int toBytes(Object object, byte[] bytes, int offset);

    public byte[] coerceBytes(byte[] b, Object object, PDataType actualType) {
        if (this == actualType) { // No coerce necessary
            return b;
        } else { // TODO: optimize in specific cases
            Object coercedValue = toObject(object, actualType);
            return toBytes(coercedValue);
        }
    }

    public byte[] coerceBytes(byte[] b, Object object, PDataType actualType, Integer maxLength, Integer scale,
            Integer desiredMaxLength, Integer desiredScale) {
        return coerceBytes(b, object, actualType);
    }

    /**
     * Convert from a string to the object representation of a given type
     * @param value a stringified value
     * @return the object representation of a string value
     */
    public abstract Object toObject(String value);
    
    public Object toObject(Object object, PDataType actualType) {
    	return toObject(object, actualType, null); 
    }

    public Object toObject(Object object, PDataType actualType, ColumnModifier sortOrder) {
        if (actualType != this) {
            byte[] b = actualType.toBytes(object, sortOrder);
            return this.toObject(b, 0, b.length, actualType);
        }
        return object;
    }
    
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType) { 
        return toObject(bytes, offset, length, actualType, null);
    }

    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, ColumnModifier columnModifier) {
    	if (columnModifier != null) {
    	    bytes = columnModifier.apply(bytes, new byte[bytes.length], offset, length);
    	}
        Object o = actualType.toObject(bytes, offset, length);
        return this.toObject(o, actualType);
    }
    
    public Object toObject(ImmutableBytesWritable ptr, PDataType actualType) {
        return toObject(ptr, actualType, null);
    }    
    
    public Object toObject(ImmutableBytesWritable ptr, PDataType actualType, ColumnModifier sortOrder) { 
        return this.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), actualType, sortOrder);
    }

    public Object toObject(ImmutableBytesWritable ptr) {
        return toObject(ptr.get(), ptr.getOffset(), ptr.getLength());
    }
    
    public Object toObject(ImmutableBytesWritable ptr, ColumnModifier columnModifier) {
        return toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), this, columnModifier);        
    }    

    public Object toObject(byte[] bytes, int offset, int length) {
        return toObject(bytes, offset, length, this);
    }

    public Object toObject(byte[] bytes) {
        return toObject(bytes, 0, bytes.length, this);
    }

    private static final Map<String, PDataType> SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE;
    static {
        ImmutableMap.Builder<String, PDataType> builder =
            ImmutableMap.<String, PDataType>builder();
        for (PDataType dataType : PDataType.values()) {
            builder.put(dataType.getSqlTypeName(), dataType);
        }
        SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE = builder.build();
    }

    public static PDataType fromSqlTypeName(String sqlTypeName) {
        PDataType dataType = SQL_TYPE_NAME_TO_PCOLUMN_DATA_TYPE.get(sqlTypeName);
        if (dataType != null) {
            return dataType;
        }
        throw new IllegalDataException("Unsupported sql type: " + sqlTypeName);
    }

    private static final int SQL_TYPE_OFFSET;
    private static final PDataType[] SQL_TYPE_TO_PCOLUMN_DATA_TYPE;
    static {
        int minSqlType = Integer.MAX_VALUE;
        int maxSqlType = Integer.MIN_VALUE;
        for (PDataType dataType : PDataType.values()) {
            int sqlType = dataType.getSqlType();
            if (sqlType < minSqlType) {
                minSqlType = sqlType;
            }
            if (sqlType > maxSqlType) {
                maxSqlType = sqlType;
            }
        }
        SQL_TYPE_OFFSET = minSqlType;
        SQL_TYPE_TO_PCOLUMN_DATA_TYPE = new PDataType[maxSqlType-minSqlType+1];
        for (PDataType dataType : PDataType.values()) {
            int sqlType = dataType.getSqlType();
            SQL_TYPE_TO_PCOLUMN_DATA_TYPE[sqlType-SQL_TYPE_OFFSET] = dataType;
        }
    }

    public static PDataType fromSqlType(Integer sqlType) {
        int offset = sqlType - SQL_TYPE_OFFSET;
        if (offset >= 0 && offset < SQL_TYPE_TO_PCOLUMN_DATA_TYPE.length) {
            PDataType type = SQL_TYPE_TO_PCOLUMN_DATA_TYPE[offset];
            if (type != null) {
                return type;
            }
        }
        throw new IllegalDataException("Unsupported sql type: " + sqlType);
    }

    public String getJavaClassName() {
        return getJavaClass().getName();
    }

    public byte[] getJavaClassNameBytes() {
        return clazzNameBytes;
    }

    public byte[] getSqlTypeNameBytes() {
        return sqlTypeNameBytes;
    }

    public KeyRange getKeyRange(byte[] point) {
        return getKeyRange(point, true, point, true);
    }
    
    public KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        /*
         * Force lower bound to be inclusive for fixed width keys because it makes
         * comparisons less expensive when you can count on one bound or the other
         * being inclusive. Comparing two fixed width exclusive bounds against each
         * other is inherently more expensive, because you need to take into account
         * if the bigger key is equal to the next key after the smaller key. For
         * example:
         *   (A-B] compared against [A-B)
         * An exclusive lower bound A is bigger than an exclusive upper bound B.
         * Forcing a fixed width exclusive lower bound key to be inclusive prevents
         * us from having to do this extra logic in the compare function.
         */
        if (lowerRange != KeyRange.UNBOUND && !lowerInclusive && isFixedWidth()) {
            lowerRange = ByteUtil.nextKey(lowerRange);
            lowerInclusive = true;
        }
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
    }

    public static PDataType fromLiteral(Object value) {
        if (value == null) {
            return null;
        }
        for (PDataType type : PDataType.values()) {
            if (type.getJavaClass().isInstance(value)) {
                return type;
            }
        }
        throw new UnsupportedOperationException("Unsupported literal value [" + value + "] of type " + value.getClass().getName());
    }
}
