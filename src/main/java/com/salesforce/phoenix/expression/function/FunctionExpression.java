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
package com.salesforce.phoenix.expression.function;

import java.util.List;

import com.salesforce.phoenix.expression.BaseCompoundExpression;
import com.salesforce.phoenix.expression.Expression;

/**
 * 
 * Compiled representation of a built-in function
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class FunctionExpression extends BaseCompoundExpression {
    public FunctionExpression() {
    }
    
    public FunctionExpression(List<Expression> children) {
        super(children);
    }
    
    /**
     * Method used to maintain certain query optimization that may be possible even through a function invocation.
     * If the function invocatin will not cause the output to be ordered differently than the input, then true should
     * be returned. Cases in which equals would be returned for the output while the input would return greater than
     * may be ok.  For example, SUBSTR(foo,1,3) would return true, since the order produced by the input and the order
     * produced by the output are the both the same.
     * @return true if the function invocation will preserve order for the inputs versus the outputs and false otherwise
     */
    public boolean preservesOrder() {
        return false;
    }

    public enum KeyFormationDirective {
        /**
         * The function is not usable to help form the scan start/stop key.
         * For example: WHERE TRUNC(date,'DAY') < TO_DATE('2013-01-01 12:00:00')
         * cannot be used to form the row key.
         */
        UNTRAVERSABLE, 
        /**
         * The function may be used to help form the scan key, but should
         * be left in the filter since all rows in the scan key range may
         * not match the filter. An example would be:
         * WHERE REGEXP_REPLACE(name,'\w') = 'he'
         * which would set the start key to 'hat' and then would filter
         * cases like 'help' and 'heap'.
         */
        TRAVERSE_AND_LEAVE, 
        /**
         * The function may be used to help form the scan key and it may
         * be removed from the where clause since all rows in the scan key
         * range will match the filter. An example would be:
         * WHERE SUBSTR(entity_id,1,3) = '001'
         * which would not require including the SUBSTR in the filter, since
         * the scan key would filter everything correctly.
         */
        TRAVERSE_AND_EXTRACT};

    /**
     * Method used to maintain certain query optimization that may be possible even through a function invocation.
     * If the function invocatin will not cause the output to be ordered differently than the input, then UseAndLeave should
     * be returned. Cases in which equals would be returned for the output while the input would return greater than
     * may be ok.  For example, SUBSTR(foo,1,3) would return true, since the order produced by the input and the order
     * produced by the output are the both the same.
     * @return the scan key formation directive to guide in the formation of the scan key
     */
    public KeyFormationDirective getKeyFormationDirective() {
        return KeyFormationDirective.UNTRAVERSABLE;
    }
    
    /**
     * Zero-based index of child expression that determines the potential column used to form the row key during optimization.
     * For example, SUBSTR(prefix,1,3), the first child expression (prefix) would determine the row key column used to
     * help form the scan start/stop key
     * @return int
     */
    public int getKeyFormationTraversalIndex() {
        return 0;
    }

    abstract public String getName();
    
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder(getName() + "(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
    
}
