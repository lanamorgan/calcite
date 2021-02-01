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

import org.apache.calcite.rel.type.RelAnyType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlAnyOperator extends SqlOperator {
//~ Constructors -----------------------------------------------------------

  private SqlAnyOperator() {
    super("ANY", SqlKind.ANY, 20, true, ReturnTypes.SCOPE, null, null);

  }

  public static final SqlAnyOperator INSTANCE = new SqlAnyOperator();

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.PREFIX;
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    SqlAny any = (SqlAny) call;
    writer.literal("DANY");
    writer.literal("{");
    writer.list(SqlWriter.FrameTypeEnum.SIMPLE, SqlWriter.COMMA,
        any.children);
    writer.literal("}");
  }


}

