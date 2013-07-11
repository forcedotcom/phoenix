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
package com.salesforce.phoenix.expression.aggregator;

import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ImmutableBytesPtr;

/**
 * 
 * Built-in function for PERCENTILE_DIST(<expression>) WITHIN GROUP (ORDER BY <expression> ASC/DESC) aggregate function
 *
 * @author ramkrishna
 * @since 1.2.1
 */
public class PercentileDiscClientAggregator extends
		DistinctValueWithCountClientAggregator {

	private final List<Expression> exps;
	private byte[] cachedResult = null;
	ColumnExpression columnExp = null;

	public PercentileDiscClientAggregator(List<Expression> exps) {
		this.exps = exps;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		// Reset buffer so that it gets initialized with the current datatype of the column
		buffer = null;
		if (cachedResult == null) {
			columnExp = (ColumnExpression)exps.get(0);
			// Second exp will be a LiteralExpression of Boolean type indicating
			// whether the ordering to be ASC/DESC
			LiteralExpression isAscendingExpression = (LiteralExpression) exps
					.get(1);
			boolean isAscending = (Boolean) isAscendingExpression.getValue();

			// Third expression will be LiteralExpression
			LiteralExpression percentileExp = (LiteralExpression) exps.get(2);
			float p = ((Number) percentileExp.getValue()).floatValue();
			Entry<ImmutableBytesPtr, Integer>[] entries = getSortedValueVsCount(isAscending);
			int currValue = 0;
			byte[] result = null;
			// Here the Percentile_disc returns the cum_dist() that is greater or equal to the
			// Percentile (p) specified in the query.  So the result set will be of that of the
			// datatype of the column being selected
			for (Entry<ImmutableBytesPtr, Integer> entry : entries) {
				ImmutableBytesPtr pi1 = entry.getKey();
				result = pi1.get();
				Integer value = entry.getValue();
				currValue += value;
				float cum_dist = (float) currValue / (float) totalCount;
				if (cum_dist >= p) {
					break;
				}
			}
			this.cachedResult = result;
		}
		if (buffer == null) {
			// Initialize based on the datatype
			// columnExp cannot be null
			buffer = new byte[columnExp.getDataType().getByteSize()];
		}
		// Copy the result to the buffer.
		System.arraycopy(this.cachedResult, 0, buffer, 0, cachedResult.length);
		ptr.set(buffer);
		return true;
	}

	@Override
	public void reset() {
		super.reset();
		this.cachedResult = null;
	}

	@Override
	protected int getBufferLength() {
		// Will be used in the aggregate() call
		return PDataType.DECIMAL.getByteSize();
	}
	
}
