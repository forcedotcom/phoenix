package com.salesforce.phoenix.expression.function;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = HexToBytesFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR})})
public class HexToBytesFunction extends ScalarFunction {

	public static final String NAME = "HEX_TO_BYTES";

	public HexToBytesFunction() {
	}

	public HexToBytesFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + getExpression().hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final HexToBytesFunction other = (HexToBytesFunction) obj;
		return true;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression expression = getExpression();
		if (!expression.evaluate(tuple, ptr) || ptr.getLength() == 0) {
			return false;
		}
		PDataType type = expression.getDataType();
		String hexStr = (String) type.toObject(ptr, expression.getColumnModifier());

		byte[] out = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length(); i = i + 2) {
			out[i / 2] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
		}

		ptr.set(PDataType.BINARY.toBytes(out));

		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.BINARY;
	}

	@Override
	public boolean isNullable() {
		return getExpression().isNullable();
	}

	private Expression getExpression() {
		return children.get(0);
	}

	@Override
	public String getName() {
		return NAME;
	}
}
