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
package phoenix.iterate;

import static phoenix.query.QueryConstants.*;

import java.sql.SQLException;

import phoenix.expression.aggregator.Aggregators;
import phoenix.schema.tuple.SingleKeyValueTuple;
import phoenix.schema.tuple.Tuple;
import phoenix.util.KeyValueUtil;

public class UngroupedAggregatingResultIterator extends GroupedAggregatingResultIterator {
    private boolean hasRows = false;

    public UngroupedAggregatingResultIterator( PeekingResultIterator resultIterator, Aggregators aggregators) {
        super(resultIterator, aggregators);
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple result = super.next();
        // Ensure ungrouped aggregregation always returns a row, even if the underlying iterator doesn't.
        if (result == null && !hasRows) {
            // Generate value using unused ClientAggregators
            byte[] value = aggregators.toBytes(aggregators.getAggregators());
            result = new SingleKeyValueTuple(
                    KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, 
                            SINGLE_COLUMN_FAMILY, 
                            SINGLE_COLUMN, 
                            AGG_TIMESTAMP, 
                            value));
        }
        hasRows = true;
        return result;
    }
}
