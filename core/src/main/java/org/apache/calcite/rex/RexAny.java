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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAnyOperator;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RexAny extends RexCall {

  public final SqlKind kind;
  protected int hash = 0;

  public RexAny(RelDataType type, List<RexNode> children, SqlKind kind){
    super(type, SqlAnyOperator.INSTANCE, children);
    this.kind = kind;
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitCall(this);
  }

  public RelDataType getType(){
    return type;
  }

  public boolean hasDiffChild(){
    List <RexNode> operands = getOperands();
    for(RexNode child: operands){
      if (child instanceof RexAny){
        return true;
      }
    }
    return false;
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitCall(this, arg);
  }

  @Override public boolean equals(@Nullable Object o){
    return false;
  }
}
