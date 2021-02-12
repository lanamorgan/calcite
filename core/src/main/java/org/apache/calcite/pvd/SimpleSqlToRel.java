package org.apache.calcite.pvd;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelAnyType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeList;
import org.apache.calcite.rex.RexSimpleField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.SqlAny;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlParam;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.Pair;


import com.google.common.collect.ImmutableList;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SimpleSqlToRel{

  protected final RelOptCluster cluster;
  private final RexBuilder rexBuilder;
  private final RelOptPlanner planner;
  private final Map<String, SimpleTable> catalog;
  private RelDataTypeFactory typeFactory;
  private List <RexNode> aggs;
  private final SqlNodeToRexConverter exprConverter;


  public SimpleSqlToRel(RexBuilder rexBuilder,
      RelOptPlanner planner,
      Map<String, SimpleTable> catalog)
  {
    this.planner = planner;
    this.rexBuilder= rexBuilder;
    this.cluster = RelOptCluster.create(planner, rexBuilder);
    this.catalog = catalog;
    this.typeFactory = typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.aggs = new ArrayList<RexNode>();
    this.exprConverter = new SqlNodeToRexConverterImpl(StandardConvertletTable.INSTANCE);
  }

  /*
   * This makes a really stupid relational plan where joins/scans are at the bottom,
   * then filters, then projections, then grouping/aggs/group filters, then sort.
   * Using SqlToRelConverter requires typechecking and ordinal fields that are
   * both incompatible with and unnecessary for the coarse planning PVD does.
   */

  public RelNode convertQuery(SqlNode root){
    if (!(root instanceof SqlSelect)){
      throw new RuntimeException("Not implemented");
    }
    SqlSelect select = (SqlSelect) root;
    RelNode relroot = convertFrom(select.getFrom());
    RelNode where = convertWhere(select.getWhere(), relroot);
    relroot = where == null ? relroot : where;
    relroot = convertSelectList(select.getSelectList(), relroot);
    RelNode groupby = convertGroupBy(select.getGroup(), relroot);
    relroot = groupby == null ? relroot : groupby;
    return relroot;
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
      projectTypes.add(new RelDataTypeFieldImpl("ANY", projectTypes.size(), outputType));
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
      SqlBasicCall call = (SqlBasicCall) item;
      SqlOperator op = call.getOperator();
      RelDataType opType = getSimpleCallType(op);
      if (opType == null){
        throw new RuntimeException(op.getName() + " currently unsupported.");
      }
      SqlNode[] operands = call.getOperands();
      List <RexNode> opExps = new ArrayList<RexNode>();
      List <RelDataTypeField> opTypes = new ArrayList<RelDataTypeField>();
      for(SqlNode operand: operands){
        convertItem(operand, inputType, opExps, opTypes);
      }
      RexNode rexCall = rexBuilder.makeCall(opType, op, opExps);
      if (op.isAggregator()){
        aggs.add(rexCall);
      }
      projectList.add(rexCall);
      projectTypes.add(new RelDataTypeFieldImpl("", projectTypes.size(), opType));
      break;
    default:
      throw new RuntimeException(kind + " found in project list: " + item.getClass());
    }
  }

  private RelDataType getSimpleCallType(SqlOperator op){
    // Look, this isn't the most above aboard operation. For calls (e.g. count())
    // where we just assume the type, skip validation and pop that type in there!
    // For funcs like SUM where type depends on operands, use most forgiving.
    String name = op.getName();
    switch (name) {
    case "COUNT":
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case "SUM":
    case "AVERAGE":
      return typeFactory.createSqlType(SqlTypeName.FLOAT);
    default:
      System.out.println("Not found: " + name);
      return null;
    }
  }

  public RelNode convertFrom(SqlNode from){
    SqlKind kind = from.getKind();
    switch (kind) {
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) from;
      SimpleTable table = catalog.get(from.toString().toLowerCase());
      return LogicalTableScan.create(cluster, table, ImmutableList.of());
    case JOIN:
      SqlJoin join = (SqlJoin) from;
      RelNode left = convertFrom(join.getLeft());
      RelNode right = convertFrom(join.getRight());
      List<RelHint> hints = new ArrayList<RelHint>();
      SqlNode expr = join.getCondition();
      System.out.println(expr.toString());
      RexNode condition = convertExpression(join.getCondition(), null);
      Set<CorrelationId> variables = new HashSet<CorrelationId>();
      // int joinType = (int) join.getJoinType().getValue();
      JoinRelType joinType = JoinRelType.INNER;
      return LogicalJoin.create(left, right, hints, condition, variables, joinType);
      default:
        throw new RuntimeException(kind + " table found:" + from.getClass());
    }
  }

  public RelNode convertGroupBy(SqlNodeList groupby, RelNode input){
    if (groupby == null) {
      return null;
    }
    List<RexNode> groupList = new ArrayList<RexNode>();
    List <RelDataTypeField> groupTypes = new ArrayList<RelDataTypeField>();
    RelDataType inputType = input.getRowType();
    for (SqlNode groupItem: groupby){
      convertItem(groupItem, inputType, groupList, groupTypes);
    }
    RelDataType outputType =
        new RelRecordType(StructKind.PEEK_FIELDS, groupTypes, true);
    return null;
  }

  public RelNode convertWhere(SqlNode where, RelNode input){
    if (where == null){
      return null;
    }
    RexNode condition = convertExpression(where, input.getRowType());
    return LogicalFilter.create(input, condition);
  }

  public RexNode convertExpression(SqlNode expr, RelDataType input){
    SqlKind kind = expr.getKind();
    switch(kind){
      case AND:
        return makeBinaryCall(SqlStdOperatorTable.AND, expr, input);
      case OR:
        return makeBinaryCall(SqlStdOperatorTable.OR, expr, input);
      case NOT_EQUALS:
        return makeBinaryCall(SqlStdOperatorTable.NOT_EQUALS, expr, input);
      case LESS_THAN:
        return makeBinaryCall(SqlStdOperatorTable.LESS_THAN, expr, input);
      case GREATER_THAN:
        return makeBinaryCall(SqlStdOperatorTable.GREATER_THAN, expr, input);
      case GREATER_THAN_OR_EQUAL:
        return makeBinaryCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            expr, input);
      case LESS_THAN_OR_EQUAL:
        return makeBinaryCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, expr, input);
      case EQUALS:
        return makeBinaryCall(SqlStdOperatorTable.EQUALS, expr, input);
      case IDENTIFIER:
        SqlIdentifier id = (SqlIdentifier) expr;
        String name = id.toString().toLowerCase();
        RelDataTypeField itemType = input.getField(name, false, false);
        RexNode rexItem = new RexSimpleField(name, 0, itemType.getType());
        return rexItem;
    case PARAM:
      SqlParam param = (SqlParam) expr;
      SqlTypeName typeName = param.getType();
      String pretty = "param: " + typeName.getName();
      return rexBuilder.makeLiteral(pretty, typeFactory.createSqlType(typeName), false);
    case LITERAL:
      SqlLiteral lit = (SqlLiteral) expr;
      RelDataType litType = lit.createSqlType(typeFactory);
      return rexBuilder.makeLiteral(lit.getValue(), litType, false);
      default:
        throw new RuntimeException(kind + " found: " + expr.getClass());
    }
  }

  private RexNode makeBinaryCall(SqlOperator op, SqlNode expr, RelDataType input){
    SqlBasicCall call = (SqlBasicCall) expr;
    SqlNode[] operands = call.getOperands();
    RexNode op1 = convertExpression(operands[0], input);
    RexNode op2 = convertExpression(operands[1], input);
    return rexBuilder.makeCall(op, op1, op2);
  }
}
