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

import java.math.*;

import com.salesforce.phoenix.schema.PDataType;

/**
 * Utility methods for numbers like decimal, long, etc.
 *
 * @author elevine
 * @since 0.1
 */
public class NumberUtil {

    public static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(PDataType.MAX_PRECISION, RoundingMode.HALF_UP);

    /**
     * Strip all trailing zeros to ensure that no digit will be zero and
     * round using our default context to ensure precision doesn't exceed max allowed.
     * @return new {@link BigDecimal} instance
     */
    public static BigDecimal normalize(BigDecimal bigDecimal) {
        return bigDecimal.stripTrailingZeros().round(DEFAULT_MATH_CONTEXT);
    }

    public static BigDecimal rescaleDecimal(BigDecimal decimal, NumericOperators op, int lp, int rp, int ls, int rs) {
        int desiredPrecision = getDecimalPrecision(op, lp, rp, ls, rs);
        int desiredScale = getDecimalScale(op, lp, rp, ls, rs);
        decimal = setDecimalWidthAndScale(decimal, desiredPrecision, desiredScale);
        return decimal;
    }

    public static BigDecimal setDecimalWidthAndScale(BigDecimal decimal, int precision, int scale) {
        // If we could not fit all the digits before decimal point into the new desired precision and
        // scale, return null and the caller method should handle the error.
        if (((precision - scale) < (decimal.precision() - decimal.scale()))){
            return null;
        }
        decimal = decimal.setScale(scale, BigDecimal.ROUND_DOWN);
        return decimal;
    }

    public static int getDecimalPrecision(NumericOperators op, int lp, int rp, int ls, int rs) {
        int val;
        switch (op) {
        case MULTIPLY:
            val = lp + rp;
        case DIVIDE:
            val = Math.min(PDataType.MAX_PRECISION, getDecimalScale(op, lp, rp, ls, rs) + lp - ls + rp);
        case ADD:
        case MINUS:
        default:
            val = getDecimalScale(op, lp, rp, ls, rs) + Math.max(lp - ls, rp - rs) + 1;
        }
        val = Math.min(PDataType.MAX_PRECISION, val);
        return val;
    }

    public static int getDecimalScale(NumericOperators op, int lp, int rp, int ls, int rs) {
        int val;
        switch (op) {
        case MULTIPLY:
            val = ls + rs;
        case DIVIDE:
            val = Math.max(PDataType.MAX_PRECISION - lp + ls - rs, 0);
        case ADD:
        case MINUS:
        default:
            val = Math.max(ls, rs);
        }
        val = Math.min(PDataType.MAX_PRECISION, val);
        return val;
    }
}
