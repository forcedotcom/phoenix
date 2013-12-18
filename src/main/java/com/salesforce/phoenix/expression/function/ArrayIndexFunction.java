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

import java.sql.Types;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.parse.ParseException;
import com.salesforce.phoenix.schema.PArrayDataType;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = ArrayIndexFunction.NAME, args = {
		@Argument(allowedTypes = { PDataType.BINARY_ARRAY,
				PDataType.VARBINARY_ARRAY }),
		@Argument(allowedTypes = { PDataType.INTEGER }) })
public class ArrayIndexFunction extends ScalarFunction {

	public static final String NAME = "ARRAY_ELEM";

	public ArrayIndexFunction() {
	}

	public ArrayIndexFunction(List<Expression> children) {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression indexExpr = children.get(1);
		if (!indexExpr.evaluate(tuple, ptr)) {
		  return false;
		} else if (ptr.getLength() == 0) {
		  return true;
		}
		// Use Codec to prevent Integer object allocation
		int index = PDataType.INTEGER.getCodec().decodeInt(ptr, indexExpr.getColumnModifier());
		if(index < 0) {
			throw new ParseException("Index cannot be negative :" + index);
		}
		Expression arrayExpr = children.get(0);
		if (!arrayExpr.evaluate(tuple, ptr)) {
		  return false;
		} else if (ptr.getLength() == 0) {
		  return true;
		}

		// Given a ptr to the entire array, set ptr to point to a particular element within that array
		// given the type of an array element (see comments in PDataTypeForArray)
		PArrayDataType.positionAtArrayElement(ptr, index, getDataType());
		return true;
		
	}

	@Override
	public PDataType getDataType() {
		return PDataType.fromTypeId(children.get(0).getDataType().getSqlType()
				- Types.ARRAY);
	}

	@Override
	public String getName() {
		return NAME;
	}

}
