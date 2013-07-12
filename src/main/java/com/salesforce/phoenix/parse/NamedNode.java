package com.salesforce.phoenix.parse;

import com.salesforce.phoenix.util.SchemaUtil;

public class NamedNode {
    private final String name;
    private final boolean isCaseSensitive;
    
    public static NamedNode caseSensitiveNamedNode(String name) {
        return new NamedNode(name,true);
    }
    
    private NamedNode(String name, boolean isCaseSensitive) {
        this.name = name;
        this.isCaseSensitive = isCaseSensitive;
    }

    NamedNode(String name) {
        this.name = SchemaUtil.normalizeIdentifier(name);
        this.isCaseSensitive = name == null ? false : SchemaUtil.isCaseSensitive(name);
    }

    public String getName() {
        return name;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isCaseSensitive ? 1231 : 1237);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NamedNode other = (NamedNode)obj;
        if (isCaseSensitive != other.isCaseSensitive) return false;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        return true;
    }

}
