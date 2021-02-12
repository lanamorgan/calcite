package org.apache.calcite.pvd;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SimpleTable implements RelOptTable, Table{
  final String id;
  final Map<String, RelDataType> schema;
  private int rowCount = 0;
  private RelDataType rowType;

  public SimpleTable(String id, Map<String, RelDataType> schema){
    this.id =id;
    this.schema = schema;
    setRowType();
  }

  public SimpleTable(String id, Map<String, RelDataType> schema, int rowCount){
    this.id =id;
    this.schema = schema;
    this.rowCount = rowCount;
    setRowType();
  }

  public static SimpleTable createFromJson(File filename){
    JSONParser jsonParser = new JSONParser();
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    String tableName = "";
    Map<String, RelDataType> tableSchema = new HashMap <String, RelDataType>();
    try (FileReader reader = new FileReader(filename))
    {
      JSONObject table = (JSONObject) jsonParser.parse(reader);
      JSONObject jsonSchema = (JSONObject) table.get("schema");
      tableName = (String) table.get("tableName");
      for (Object field: jsonSchema.keySet()){
        String key = (String) field;
        RelDataType sqlType = getSqlDataType(typeFactory, jsonSchema.get(key));
        tableSchema.put(key, sqlType);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return new SimpleTable(tableName, tableSchema);
  }

  private static RelDataType getSqlDataType(RelDataTypeFactory factory, Object typ){
    String typeName = (String) typ;
    switch(typeName){
    case "INTEGER":
      return factory.createSqlType(SqlTypeName.INTEGER);
    default:
      return factory.createSqlType(SqlTypeName.VARCHAR);
    }
  }


  @Override public List<String> getQualifiedName(){
    return new ArrayList<String>(Arrays.asList(id));
  }

  /**
   * Returns an estimate of the number of rows in the table.
   */
  @Override public double getRowCount(){
    return rowCount;
  }

  public RelDataType getRowType(){
    return rowType;
  }

  public String getName() { return id; }

  public void setRowType(){
    List<RelDataTypeField> fieldList = new ArrayList<RelDataTypeField>();
    int i = 0;
    for(String key: schema.keySet()){
        fieldList.add(new RelDataTypeFieldImpl(key,i, schema.get(key)));
        i++;
    }
    rowType = new RelRecordType(StructKind.PEEK_FIELDS, fieldList, true);
  }


  /**
   * Returns the {@link RelOptSchema} this table belongs to.
   */
  @Override public @Nullable RelOptSchema getRelOptSchema(){
    return null;
  }

  /**
   * Converts this table into a {@link RelNode relational expression}.
   *
   * <p>The {@link org.apache.calcite.plan.RelOptPlanner planner} calls this
   * method to convert a table into an initial relational expression,
   * generally something abstract, such as a
   * {@link org.apache.calcite.rel.logical.LogicalTableScan},
   * then optimizes this expression by
   * applying {@link org.apache.calcite.plan.RelOptRule rules} to transform it
   * into more efficient access methods for this table.</p>
   */
  @Override public RelNode toRel(ToRelContext context){
    return null;
  }

  /**
   * Returns a description of the physical ordering (or orderings) of the rows
   * returned from this table.
   *
   * @see RelMetadataQuery#collations(RelNode)
   */
  @Override public @Nullable List<RelCollation> getCollationList(){
    return null;
  }

  /**
   * Returns a description of the physical distribution of the rows
   * in this table.
   *
   * @see RelMetadataQuery#distribution(RelNode)
   */
  @Override public @Nullable RelDistribution getDistribution(){
    return null;
  }

  /**
   * Returns whether the given columns are a key or a superset of a unique key
   * of this table.
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   */
  @Override public boolean isKey(ImmutableBitSet columns){
    return false;
  }

  /**
   * Returns a list of unique keys, empty list if no key exist,
   * the result should be consistent with {@code isKey}.
   */
  @Override @Nullable public List<ImmutableBitSet> getKeys(){
    return null;
  }

  /**
   * Returns the referential constraints existing for this table. These constraints
   * are represented over other tables using {@link RelReferentialConstraint} nodes.
   */
  @Override @Nullable public List<RelReferentialConstraint> getReferentialConstraints(){
    return null;
  }

  /**
   * Generates code for this table.
   *
   * @param clazz The desired collection class; for example {@code Queryable}.
   *
   * @return the code for the table, or null if code generation is not supported
   */
  @Override @Nullable public Expression getExpression(Class clazz){
    return null;
  }

  /** Returns a table with the given extra fields.
   *
   * <p>The extended table includes the fields of this base table plus the
   * extended fields that do not have the same name as a field in the base
   * table.
   */
  @Override public RelOptTable extend(List<RelDataTypeField> extendedFields){
    return null;
  }

  /** Returns a list describing how each column is populated. The list has the
   *  same number of entries as there are fields, and is immutable. */
  @Override public List<ColumnStrategy> getColumnStrategies(){
    return null;
  }

  @Override public <C extends Object> @Nullable C unwrap(Class<C> aClass){
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory){
    List<String> names = new ArrayList<String>();
    List<RelDataType> types = new ArrayList<RelDataType>();
    for(String key: schema.keySet()){
      names.add(key);
      types.add(schema.get(key));
    }
    return typeFactory.createStructType(StructKind.PEEK_FIELDS, types, names);
  }

  /** Returns a provider of statistics about this table. */
  public Statistic getStatistic(){
    return Statistics.UNKNOWN;
  }

  /** Type of table. */
  public Schema.TableType getJdbcTableType(){
    return null;
  }

  /**
   * Determines whether the given {@code column} has been rolled up.
   * */
  public boolean isRolledUp(String column){
    return false;
  }


  public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
      @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config){
    return false;
  }

}
