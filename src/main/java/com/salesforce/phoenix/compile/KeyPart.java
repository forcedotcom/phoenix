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
package com.salesforce.phoenix.compile;

import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.schema.PColumn;

/**
 * 
 * Interface that determines how a key part contributes to
 * the forming of the key (start/stop of scan and SkipScanFilter)
 * for each part of a multi-part primary key. It acts as the glue
 * between a built-in function and the setting of the scan key
 * during query compilation.
 * 
 * @author jtaylor
 * @since 0.12
 */
public interface KeyPart {
    /**
     * Calculate the key range given an operator and the key on
     * the RHS of an expression. For example, given the expression
     * SUBSTR(foo,1,3) = 'bar', the key range would be ['bar','bas'),
     * and if foo was fixed length, the upper and lower key range
     * bytes would be filled out to the fixed length.
     * @param op comparison operator (=, <=, <, >=, >, !=)
     * @param rhs the constant on the RHS of an expression.
     * @return the key range that encompasses the range for the
     *  expression for which this keyPart is associated or null if the
     *  the expression cannot possibly be satisfied.
     *  
     * @see com.salesforce.phoenix.expression.function.ScalarFunction#newKeyPart(KeyPart)
     */
    public KeyRange getKeyRange(CompareOp op, Expression rhs);
    
    /**
     * Determines whether an expression gets extracted from the
     * WHERE clause if it contributes toward the building of the
     * scan key. For example, the SUBSTR built-in function may
     * be extracted, since it may be completely represented
     * through a key range. However, the REGEXP_SUBSTR must be
     * left in the WHERE clause, since only the constant prefix
     * part of the evaluation can be represented through a key
     * range (i.e. rows may pass through that will fail when
     * the REGEXP_SUBSTR is evaluated).
     * 
     * @return an empty list if the expression should remain in
     * the WHEERE clause for post filtering or a singleton list
     * containing the expression if it should be removed.
     */
    public List<Expression> getExtractNodes();
    
    /**
     * Gets the primary key column associated with the start of this key part.
     * @return the primary key column for this key part
     */
    public PColumn getColumn();
}