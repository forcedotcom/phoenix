package com.salesforce.phoenix.expression;

import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.util.SchemaUtil;

public class IndexKeyValueColumnExpression extends KeyValueColumnExpression {

    public IndexKeyValueColumnExpression() {
    }

    public IndexKeyValueColumnExpression(PColumn column) {
        super(column);
    }
    
    @Override
    public String toString() {
        // Just display the column name part, since it's guaranteed to be unique
        return SchemaUtil.getColumnDisplayName(this.getColumnName());
    }

}
