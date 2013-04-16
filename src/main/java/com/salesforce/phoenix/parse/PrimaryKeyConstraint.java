package com.salesforce.phoenix.parse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.util.SchemaUtil;

public class PrimaryKeyConstraint extends NamedNode {
    private final LinkedHashMap<String, ColumnModifier> columnNameToModifier;
    
    PrimaryKeyConstraint(String name, List<Pair<String, ColumnModifier>> columnNameAndModifier) {
        super(name);
        this.columnNameToModifier = new LinkedHashMap<String, ColumnModifier>(columnNameAndModifier.size());
        for (Pair<String, ColumnModifier> p : columnNameAndModifier) {
            this.columnNameToModifier.put(SchemaUtil.normalizeIdentifier(p.getFirst()), p.getSecond());
        }
    }

    public Set<String> getColumnNames() {
        return columnNameToModifier.keySet();
    }
    
    public ColumnModifier getColumnModifier(String columnName) {
    	return columnNameToModifier.get(columnName);
    }
}
