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

public class RexNodeList extends RexNode {

  protected int hash = 0;
  public RelDataType type;
  public List<RexNode> children;
  public String digest;
  public RexNodeList(RelDataType type, List<RexNode> children){
    //this should only appear where we need lists as choices for an ANY
    this.type = type;
    this.children = children;
    computeDigest();
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return null;
  }

  public RelDataType getType(){
    return type;
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return null;
  }

  @Override public boolean equals(@Nullable Object o){
    return false;
  }

  public static RexNodeList of(RelDataType type, List<RexNode> children){
    return new RexNodeList(type, children);
  }

  @Override public SqlKind getKind(){ return SqlKind.ANY; }

  @Override public int hashCode(){return hash;}

  public void computeDigest(){
    final StringBuilder sb = new StringBuilder("[");
    String sep = "";
    for (RexNode child: children){
      sb.append(sep);
      sb.append(child.toString());
      sep = ", ";
    }
    sb.append("]");
    digest = sb.toString();
  }

  @Override public String toString(){return digest;}

}
