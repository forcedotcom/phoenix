package com.salesforce.phoenix.parse;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.util.SchemaUtil;

public class PrimaryKeyConstraint extends NamedNode {
    private final LinkedHashMap<String, ColumnModifier> columnNameToModifier;
    
    PrimaryKeyConstraint(String name, LinkedHashMap<String, String> columnNameToModifier) {
        super(name);
        this.columnNameToModifier = new LinkedHashMap<String, ColumnModifier>(columnNameToModifier.size());
        for (Map.Entry<String, String> entry : columnNameToModifier.entrySet()) {
            this.columnNameToModifier.put(SchemaUtil.normalizeIdentifier(entry.getKey()), ColumnModifier.fromDDLStatement(entry.getValue()));
        }
    }

    public Set<String> getColumnNames() {
        return columnNameToModifier.keySet();
    }
    
    public ColumnModifier getColumnModifier(String columnName) {
    	return columnNameToModifier.get(columnName);
    }
}
