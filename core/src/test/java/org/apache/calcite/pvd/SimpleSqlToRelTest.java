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
package org.apache.calcite.test.pvd;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.pvd.SimpleTable;
import org.apache.calcite.pvd.SimpleSqlToRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.MockRelOptPlanner;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.HashMap;
import java.util.Map;



import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.sql2rel.SqlToRelConverter}.
 */
class SimpleSqlToRelTest {
  /**
   * Sets the SQL statement for a test.
   */
  public final Sql sql(String sql) {
    return new Sql(sql);
  }

  @Test
  void testSimpleAny() {
    final String sql = "select DANY{'empid', DANY{'rand'}} from emp";
    String expected = "SELECT DANY { 'empid', DANY { 'rand' } }\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test
  void testNestedAny(){
    final String sql = "select dany{'empid', dany{'deptno'}} from emp";
    String expected = "SELECT DANY { 'empid', DANY { 'deptno' } }\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test
  void testSimpleProjectAndScan(){
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Map<String, RelDataType> schema = new HashMap<String, RelDataType>();
    schema.put("ename", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    schema.put("eptno", typeFactory.createSqlType(SqlTypeName.INTEGER));
    schema.put("fleet", typeFactory.createSqlType(SqlTypeName.INTEGER));
    SimpleTable personTable = new SimpleTable("person", schema);
    Map<String, SimpleTable> catalog = new HashMap<String, SimpleTable>();
    catalog.put("person", personTable);
    RelOptPlanner planner = new MockRelOptPlanner(Contexts.EMPTY_CONTEXT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    SimpleSqlToRel converter =
        new SimpleSqlToRel(rexBuilder, planner, catalog);
    // TODO: weird bug where dany{<field_that_starts_with_d>} doesn't parse
    // but dany{<field_not_start_with_d, d_field>} does
    final String sqlStr = "select dany{ename, dany{eptno}}, fleet from person";
    final Sql sql = sql(sqlStr);
    final SqlNode parseTree = sql.parseTree();
    RelNode output = converter.convertQuery(parseTree);
    System.out.println(RelOptUtil.toString(output));
  }

  private static final ThreadLocal<boolean[]> LINUXIFY =
      ThreadLocal.withInitial(() -> new boolean[] {true});

  private static final SqlWriterConfig SQL_WRITER_CONFIG =
      SqlPrettyWriter.config()
          .withAlwaysUseParentheses(true)
          .withUpdateSetListNewline(false)
          .withFromFolding(SqlWriterConfig.LineFolding.TALL)
          .withIndentation(0);

  /**
   * Allows fluent testing.
   */
  public class Sql {
    private final String sql;
    private final SqlParser parser;

    Sql(String sql) {
      this.sql = sql;
      Quoting quoting = Quoting.DOUBLE_QUOTE;
      Casing unquotedCasing = Casing.TO_UPPER;
      Casing quotedCasing = Casing.UNCHANGED;
      final SqlParser.Config configBuilder =
          SqlParser.config().withParserFactory(SqlParserImpl.FACTORY)
              .withQuoting(quoting)
              .withUnquotedCasing(unquotedCasing)
              .withQuotedCasing(quotedCasing)
              .withConformance(SqlConformanceEnum.DEFAULT);
      UnaryOperator<SqlParser.Config> transform = UnaryOperator.identity();
      final SqlParser.Config config = transform.apply(configBuilder);
      parser = SqlParser.create(sql, config);
    }

    public SqlNode parseTree() {
      final SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing SQL: " + sql, e);
      }
      return sqlNode;
    }


    public void checkParseTree(SqlNode sqlNode, String expected){
      final SqlDialect dialect = AnsiSqlDialect.DEFAULT;
      final SqlWriterConfig c2 = SQL_WRITER_CONFIG.withDialect(dialect);
      final String actual = sqlNode.toSqlString(c -> c2).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    public void ok(String expected){
      SqlNode sqlNode = parseTree();
      checkParseTree(sqlNode, expected);
    }

    public void convertToRel(String expected) {

    }

    private String linux(String s) {
      if (LINUXIFY.get()[0]) {
        s = Util.toLinux(s);
      }
      return s;
    }
  }
}
