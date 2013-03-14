package com.salesforce.phoenix.filter;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

public class FilterListWithEquals extends FilterList {
    public FilterListWithEquals(Operator op, Filter... filters) {
        super(op, filters);
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof FilterListWithEquals)) return false;
        FilterListWithEquals other = (FilterListWithEquals) obj;
        if (this.getOperator() != other.getOperator()) return false;
        return this.getFilters().equals(other.getFilters());
    }
}
