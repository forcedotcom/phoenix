package com.salesforce.phoenix.parse;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.util.SchemaUtil;

public class PrimaryKeyConstraint extends NamedNode {
    private final Map<String, ColumnModifier> columnNameToModifier;
    
    PrimaryKeyConstraint(String name, Map<String, String> columnNameToModifier) {
        super(name);
        Map<String, ColumnModifier> m = Maps.newHashMapWithExpectedSize(columnNameToModifier.size());
        for (Map.Entry<String, String> entry : columnNameToModifier.entrySet()) {
            m.put(SchemaUtil.normalizeIdentifier(entry.getKey()), ColumnModifier.fromDDLStatement(entry.getValue()));
        }
        this.columnNameToModifier = Collections.unmodifiableMap(m);
    }

    public Set<String> getColumnNames() {
        return columnNameToModifier.keySet();
    }
    
    public ColumnModifier getColumnModifier(String columnName) {
    	return columnNameToModifier.get(columnName);
    }
}
