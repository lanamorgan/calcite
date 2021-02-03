/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class SqlMulti extends SqlCall {

  SqlAny child;
  SqlKind clauseType;
  public SqlMulti(SqlParserPos pos,
      @Nullable SqlAny child,
      SqlKind clauseType) {
    super(pos);
    this.child = child;
    this.clauseType = clauseType;
  }


  @Override public SqlOperator getOperator() {
    return SqlMultiOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MULTI;
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      setChild((SqlAny) operand);
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
  }

  @Override public boolean isExpanded() {
    return true;
  }

  public SqlNode getChild() { return child; }

  public void setChild(SqlAny child) { this.child = child; }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(child);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.getDialect().unparseCall(writer, this, leftPrec, rightPrec);
  }

}
