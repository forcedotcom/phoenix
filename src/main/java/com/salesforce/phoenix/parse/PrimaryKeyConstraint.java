package com.salesforce.phoenix.parse;

import java.util.List;
import java.util.Set;


import com.google.common.collect.ImmutableSet;
import com.salesforce.phoenix.util.SchemaUtil;

public class PrimaryKeyConstraint extends NamedNode {
    private final Set<String> columnNames;
    
    PrimaryKeyConstraint(String name, List<String> columnNames) {
        super(name);
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String columnName : columnNames) {
            builder.add(SchemaUtil.normalizeIdentifier(columnName));
        }
        this.columnNames = builder.build();
    }

    public Set<String> getColumnNames() {
        return columnNames;
    }
}
