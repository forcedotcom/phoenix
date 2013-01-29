package phoenix.parse;

import phoenix.util.SchemaUtil;

public class NamedNode {
    private final String name;
    private final boolean isCaseSensitive;
    
    NamedNode(String name) {
        this.name = SchemaUtil.normalizeIdentifier(name);
        this.isCaseSensitive = SchemaUtil.isCaseSensitive(name);
    }

    public String getName() {
        return name;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
}
