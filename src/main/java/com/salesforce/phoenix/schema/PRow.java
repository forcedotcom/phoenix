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

import java.util.List;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;

import com.salesforce.phoenix.execute.MutationValue;

/**
 * 
 * Provide a client API for updating rows. The updates are processed in
 * the calling order. Calling setValue after calling delete will cause the
 * delete to be canceled.  Conversely, calling delete after calling
 * setValue will cause all prior setValue calls to be canceled.
 *
 * @author jtaylor
 * @since 0.1
 */
public interface PRow {
    /**
     * Get the list of {@link org.apache.hadoop.hbase.client.Mutation} used to
     * update an HTable after all mutations through calls to
     * {@link #setValue(PColumn, Object)} or {@link #delete()}.
     * @return the list of mutations representing all changes made to a row
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public List<Mutation> toRowMutations();

    /**
     * Get the Increment {@link org.apache.hadoop.hbase.client.Increment} used to
     * update an HTable after all mutations through calls to
     * {@link #setValue(PColumn, Object)} or {@link #delete()}.
     * @return the Increment representing the Increment made to a row
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public Increment toIncrement();

    /**
     * Set a column value in the row
     * @param col the column for which the value is being set
     * @param value the value
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public void setValue(PColumn col, Object value);
    
    /**
     * Set a column value in the row
     * @param col the column for which the value is being set
     * @param value the value
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public void setValue(PColumn col, MutationValue value);
    
    /**
     * Delete the row. Note that a delete take precedence over any
     * values that may have been set before or after the delete call.
     */
    public void delete();
}
