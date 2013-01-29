package phoenix.parse;

public class ColumnDefName {
    private final NamedNode familyName;
    private final NamedNode columnName;
    
    ColumnDefName(String familyName, String columnName) {
        this.familyName = familyName == null ? null : new NamedNode(familyName);
        this.columnName = new NamedNode(columnName);
    }

    ColumnDefName(String columnName) {
        this(null, columnName);
    }

    public NamedNode getFamilyName() {
        return familyName;
    }

    public NamedNode getColumnName() {
        return columnName;
    }
}
