package com.salesforce.hbase.index.builder.covered;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A single Column (either a Column Family or a full Family:Qualifier pair) in a {@link ColumnGroup}
 * . If no column qualifier is specified (null), matches all known qualifiers of the family.
 */
public class CoveredColumn extends ColumnReference {

  public static final String SEPARATOR = ":";
  String familyString;

  public CoveredColumn(String family, byte[] qualifier) {
    super(Bytes.toBytes(family), qualifier == null ? ColumnReference.ALL_QUALIFIERS : qualifier);
    this.familyString = family;
  }

  public static CoveredColumn parse(String spec) {
    int sep = spec.indexOf(SEPARATOR);
    if (sep < 0) {
      throw new IllegalArgumentException(spec + " is not a valid specifier!");
    }
    String family = spec.substring(0, sep);
    String qual = spec.substring(sep + 1);
    byte[] column = qual.length() == 0 ? null : Bytes.toBytes(qual);
    return new CoveredColumn(family, column);
  }

  public String serialize() {
    return CoveredColumn.serialize(familyString, qualifier);
  }

  public static String serialize(String first, byte[] second) {
    String nextValue = first + CoveredColumn.SEPARATOR;
    if (second != null) {
      nextValue += Bytes.toString(second);
    }
    return nextValue;
  }

  /**
   * @param family2 to check
   * @return <tt>true</tt> if the passed family matches the family this column covers
   */
  public boolean matchesFamily(String family2) {
    return this.familyString.equals(family2);
  }

  @Override
  public boolean equals(Object o) {
    CoveredColumn other = (CoveredColumn) o;
    if (this.familyString.equals(other.familyString)) {
      return Bytes.equals(qualifier, other.qualifier);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = this.familyString.hashCode();
    if (this.qualifier != null) {
      hash += Bytes.hashCode(qualifier);
    }

    return hash;
  }

  @Override
  public String toString() {
    String qualString = qualifier == null ? "null" : Bytes.toString(qualifier);
    return "CoveredColumn:[" + familyString + ":" + qualString + "]";
  }
}