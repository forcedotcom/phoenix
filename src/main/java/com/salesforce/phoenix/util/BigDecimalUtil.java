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

import org.apache.hadoop.hbase.util.Pair;

/**
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class BigDecimalUtil {

    private static final BigDecimal TWO = BigDecimal.valueOf(2L);

    /**
     * @param lo The dividend
     * @param ro The divisor
     * @return
     */
    public static BigDecimal divide(BigDecimal lo, BigDecimal ro) {
        Pair<Integer, Integer> precisionScale = BigDecimalUtil.getResultPrecisionScale(Operation.DIVIDE, lo, ro);
        BigDecimal result = lo.divide(ro, new MathContext(precisionScale.getFirst(), RoundingMode.HALF_UP));
        result.setScale(precisionScale.getSecond(), RoundingMode.HALF_UP);
        return result;
    }

    // http://blog.udby.com/archives/17
    public static BigDecimal sqrt(BigDecimal x, MathContext mc) {
        if (mc.getPrecision() <= MathContext.DECIMAL64.getPrecision()) {
            return new BigDecimal(StrictMath.sqrt(x.doubleValue()), mc);
        }
        BigDecimal g = x.divide(TWO, mc);
        boolean done = false;
        final int maxIterations = mc.getPrecision() + 1;
        for (int i = 0; !done && i < maxIterations; i++) {
            // r = (x/g + g) / 2
            BigDecimal r = x.divide(g, mc);
            r = r.add(g);
            r = r.divide(TWO, mc);
            done = r.equals(g);
            g = r;
        }
        return g;
    }
    
    // http://db.apache.org/derby/docs/10.0/manuals/reference/sqlj124.html#HDRSII-SQLJ-36146
    public static Pair<Integer, Integer> getResultPrecisionScale(Operation op, BigDecimal... operands) {
        if (operands.length < 2) throw new IllegalArgumentException("Atleast 2 operands required");
        int lp = operands[0].precision();
        int ls = operands[0].scale();
        int rp, rs;
        int resultPrec = 0, resultScale = 0;
        for (int i = 1; i < operands.length; i++) {
            rp = operands[i].precision();
            rs = operands[i].scale();
            switch (op) {
            case MULTIPLY:
                resultPrec = lp + rp;
                resultScale = ls + rs;
                break;
            case DIVIDE:
                resultPrec = lp - ls + rp + Math.max(ls + rp - rs + 1, 4);
                resultScale = 31 - lp + ls - rs;
                break;
            case ADD:
                resultPrec = 2 * (lp - ls) + ls; // Is this correct? The page says addition -> 2 * (p - s) + s.
                resultScale = Math.max(ls, rs);
                break;
            case AVG:
                resultPrec = Math.max(lp - ls, rp - rs) + 1 + Math.max(ls, rs);
                resultScale = Math.max(Math.max(ls, rs), 4);
                break;
            case OTHERS:
                resultPrec = Math.max(lp - ls, rp - rs) + 1 + Math.max(ls, rs);
                resultScale = Math.max(ls, rs);
            }
            lp = resultPrec;
            ls = resultScale;
        }
        return new Pair<Integer, Integer>(resultPrec, resultScale);
    }
    
    public static enum Operation {
        MULTIPLY, DIVIDE, ADD, AVG, OTHERS;
    }
}