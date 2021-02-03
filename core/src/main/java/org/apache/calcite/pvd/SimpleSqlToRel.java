package org.apache.calcite.pvd;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAny;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelAnyType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeList;
import org.apache.calcite.rex.RexSimpleField;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.Pair;


import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;



public class SimpleSqlToRel{

  protected final RelOptCluster cluster;
  private final RexBuilder rexBuilder;
  private final RelOptPlanner planner;
  private final Map<String, SimpleTable> catalog;

  public SimpleSqlToRel(RexBuilder rexBuilder,
      RelOptPlanner planner,
      Map<String, SimpleTable> catalog)
  {
    this.planner = planner;
    this.rexBuilder= rexBuilder;
    cluster = RelOptCluster.create(planner, rexBuilder);
    this.catalog = catalog;
  }

  public RelNode convertQuery(SqlNode root){
    if (!(root instanceof SqlSelect)){
      throw new RuntimeException("Not implemented");
    }
    SqlSelect select = (SqlSelect) root;
    RelNode from = convertFrom(select.getFrom());
    RelNode project = convertSelectList(select.getSelectList(), from);
    return project;
  }

  public RelNode convertSelectList(SqlNode selectList, RelNode input){
    SqlNodeList select = (SqlNodeList) selectList;
    List <RexNode> projectList = new ArrayList<RexNode>();
    List <RelDataTypeField> projectTypes = new ArrayList<RelDataTypeField>();
    RelDataType inputType = input.getRowType();
    for (SqlNode item:select){
        convertItem(item, inputType, projectList, projectTypes);
    }
    RelDataType outputType =
        new RelRecordType(StructKind.PEEK_FIELDS, projectTypes, true);
    return LogicalProject.create(input,
        ImmutableList.of(), projectList, outputType);
  }

  public void convertItem(SqlNode item, RelDataType inputType,
      List <RexNode> projectList,  List <RelDataTypeField> projectTypes){
    SqlKind kind = item.getKind();
    switch(kind) {
    case ANY:
      SqlAny any = (SqlAny) item;
      SqlNodeList children = any.getChildren();
      List <RexNode> innerExps = new ArrayList<RexNode>();
      List <RelDataTypeField> innerTypes = new ArrayList<RelDataTypeField>();
      List <RelDataType> anyChildTypes = new ArrayList<RelDataType>();
      int count = 0;
      for (SqlNode child: children){
        SqlNodeList childList = (SqlNodeList) child;
        List <RexNode> childExps = new ArrayList<RexNode>();
        List <RelDataTypeField> childTypes = new ArrayList<RelDataTypeField>();
        for(SqlNode childItem: childList) {
          convertItem(childItem, inputType, childExps, childTypes);
        }
        RelDataType childType =
            new RelRecordType(StructKind.PEEK_FIELDS, childTypes, true);
        innerExps.add(RexNodeList.of(childType, childExps));
        innerTypes.add(new RelDataTypeFieldImpl("", count, childType));
        count +=1;
      }
      RelDataType outputType = new RelAnyType(innerTypes, anyChildTypes);
      projectList.add(new RexAny(outputType, innerExps, SqlKind.ANY));
      projectTypes.add(new RelDataTypeFieldImpl("ANY", 0, outputType));
      break;
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) item;
      if (id.isStar()){
        //TODO: expand star to entire input row
        throw new RuntimeException("* identifier found");
      }
      String name = id.toString().toLowerCase();
      RelDataTypeField itemType = inputType.getField(name, false, false);
      RexNode rexItem = new RexSimpleField(name, 0, itemType.getType());
      projectList.add(rexItem);
      projectTypes.add(itemType);
      break;
    case OTHER_FUNCTION:

    default:
      throw new RuntimeException(kind + " found in project list: " + item.getClass());
    }
  }

  public RelNode convertFrom(SqlNode from){
    SqlKind kind = from.getKind();
    switch (kind) {
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) from;
      SimpleTable table = catalog.get(from.toString().toLowerCase());
      return LogicalTableScan.create(cluster, table, ImmutableList.of());
      default:
        throw new RuntimeException(kind + " table found");
    }
  }
}
