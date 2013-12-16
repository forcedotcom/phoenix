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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.ArrayColumnNode;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.ParseException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = ArrayIndexFunction.NAME, nodeClass = ArrayColumnNode.class, args = {
		@Argument(allowedTypes = { PDataType.BINARY_ARRAY,
				PDataType.BOOLEAN_ARRAY, PDataType.CHAR_ARRAY,
				PDataType.DATE_ARRAY, PDataType.DOUBLE_ARRAY,
				PDataType.DECIMAL_ARRAY, PDataType.FLOAT_ARRAY,
				PDataType.INTEGER_ARRAY, PDataType.LONG_ARRAY,
				PDataType.SMALLINT_ARRAY, PDataType.TIME_ARRAY,
				PDataType.TIMESTAMP_ARRAY, PDataType.TINYINT_ARRAY,
				PDataType.UNSIGNED_DOUBLE_ARRAY,
				PDataType.UNSIGNED_FLOAT_ARRAY, PDataType.UNSIGNED_INT_ARRAY,
				PDataType.UNSIGNED_LONG_ARRAY,
				PDataType.UNSIGNED_SMALLINT_ARRAY,
				PDataType.UNSIGNED_TINYINT_ARRAY, PDataType.VARCHAR_ARRAY }),
		@Argument(allowedTypes = { PDataType.INTEGER}) })
public class ArrayIndexFunction extends ScalarFunction {

	public static final String NAME = "ARRAY_ELEM";

	private int index;
	private String colName;
	private PDataType dataType;

	public ArrayIndexFunction() {
	}

	public ArrayIndexFunction(List<Expression> children, int index,
			String colName, PDataType dataType) {
		super(children);
		if (index < 0) {
			throw new ParseException("Index cannot be negative :" + index);
		}
		this.index = index;
		this.colName = colName;
		if (!dataType.isArrayType()) {
			throw new ParseException("The column specified must of array type "
					+ colName);
		}
		this.dataType = dataType;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression expression = children.get(0);
		if (!expression.evaluate(tuple, ptr)) {
			return false;
		} else if (ptr.getLength() == 0) {
			return true;
		}

		byte[] bs = ptr.get();
		if (this.dataType == null) {
			this.dataType = getDataType();
		}
		Object object = this.dataType.toObject(bs, ptr.getOffset(),
				ptr.getLength(), this.dataType, this.index);
		byte[] bytes = this.dataType.toBytes(object);
		ptr.set(bytes);
		return true;
	}

	@Override
	public PDataType getDataType() {
		return children.get(0).getDataType();
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		index = WritableUtils.readVInt(input);
		colName = WritableUtils.readString(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		super.write(output);
		WritableUtils.writeVInt(output, index);
		WritableUtils.writeString(output, colName);
	}

}
