package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RexSimpleField extends RexSlot {

  public RexSimpleField(String name, int index, RelDataType type) {
    super(name, index, type);
  }

  @Override public int hashCode() {
    return index;
  }

  @Override public SqlKind getKind() {
    return SqlKind.INPUT_REF;
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return null;
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
   return null;
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof RexSimpleField
        && name == ((RexSimpleField) obj).name;
  }
}
