package com.salesforce.phoenix.expression;

import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.util.IndexUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class IndexKeyValueColumnExpression extends KeyValueColumnExpression {
    public IndexKeyValueColumnExpression() {
    }

    public IndexKeyValueColumnExpression(PColumn column) {
        super(column);
    }
    
    @Override
    public String toString() {
        // Translate to the data table column name
        String indexColumnName = Bytes.toString(this.getColumnName());
        String dataFamilyName = IndexUtil.getDataColumnFamilyName(indexColumnName);
        String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
        return SchemaUtil.getColumnDisplayName(dataFamilyName, dataColumnName);
    }

}
