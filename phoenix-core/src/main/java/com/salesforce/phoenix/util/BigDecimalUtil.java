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

import org.apache.hadoop.hbase.util.Pair;

/**
 * 
 * @author anoopsjohn
 * @since 1.2.1
 */
public class BigDecimalUtil {

    /**
     * Calculates the precision and scale for BigDecimal arithmetic operation results. It uses the algorithm mentioned
     * <a href="http://db.apache.org/derby/docs/10.0/manuals/reference/sqlj124.html#HDRSII-SQLJ-36146">here</a>
     * @param lp precision of the left operand
     * @param ls scale of the left operand
     * @param rp precision of the right operand
     * @param rs scale of the right operand
     * @param op The operation type
     * @return {@link Pair} comprising of the precision and scale.
     */
    public static Pair<Integer, Integer> getResultPrecisionScale(int lp, int ls, int rp, int rs, Operation op) {
        int resultPrec = 0, resultScale = 0;
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
        return new Pair<Integer, Integer>(resultPrec, resultScale);
    }
    
    public static enum Operation {
        MULTIPLY, DIVIDE, ADD, AVG, OTHERS;
    }
}