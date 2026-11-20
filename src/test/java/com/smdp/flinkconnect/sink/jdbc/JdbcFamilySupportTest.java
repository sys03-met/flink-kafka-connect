package com.smdp.flinkconnect.sink.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.DbType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 공식 경로 대상 계획 · 공통 INSERT 문장. */
class JdbcFamilySupportTest {

  private static final ColumnRule NOW_RULE =
      new ColumnRule("INGEST_DT", "sql", null, null, null, null, "NOW()");

  /** 같은 테이블·방식은 (대소문자 달라도) 하나로 합치고, sink: jdbc 는 공식 대상에서 뺀다. */
  @Test
  void plan_mergesSameKey_skipsCustomJdbc() {
    List<JdbcFamilySupport.Target> targets =
        JdbcFamilySupport.plan(
            List.of(
                new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.UPSERT, List.of(), List.of("METER_ID")),
                new DestinationConfig(DbType.POSTGRES, "rd_lp", WriteMode.UPSERT, List.of(), List.of("meter_id")),
                new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.BULK, List.of()),
                new DestinationConfig(
                    DbType.POSTGRES, "OTHER", WriteMode.INSERT, List.of(), List.of(), SinkMode.JDBC)));

    assertEquals(2, targets.size());
  }

  /** 같은 대상인데 SQL 식이 다르면 한 문장으로 못 만드니 시작 전에 실패. */
  @Test
  void plan_conflictingSqlRules_fails() {
    List<DestinationConfig> planned =
        List.of(
            new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.INSERT, List.of(NOW_RULE)),
            new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.INSERT, List.of()));
    assertThrows(IllegalStateException.class, () -> JdbcFamilySupport.plan(planned));
  }

  /** 공식 대상은 op: sql 식을 대소문자 무시로 들고 간다. */
  @Test
  void plan_keepsSqlExpressions() {
    JdbcFamilySupport.Target t =
        JdbcFamilySupport.plan(
                List.of(new DestinationConfig(DbType.MSSQL, "RD_LP", WriteMode.INSERT, List.of(NOW_RULE))))
            .get(0);
    assertEquals("NOW()", t.sqlExprs().get("ingest_dt"));
  }

  @Test
  void insertSql_embedsSqlExpression() {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("METER_ID", "m1");
    fields.put(WriteRowRecord.SQL_PREFIX + "INGEST_DT", "NOW()");
    String sql = JdbcSql.insertSql("RD_LP", WriteRowRecord.of(fields));
    assertEquals("INSERT INTO RD_LP (METER_ID, INGEST_DT) VALUES (?, NOW())", sql);
  }

  /** 공식 경로: 테이블 칸 목록 기준. 식 칸(소문자 이름)도 대소문자 무시로 찾는다. */
  @Test
  void insertSql_columnList_matchesExprCaseInsensitive() {
    String sql =
        JdbcSql.insertSql(
            "rd_lp", List.of("meter_id", "ingest_dt"), JdbcSql.caseInsensitive(Map.of("INGEST_DT", "NOW()")));
    assertTrue(sql.endsWith("VALUES (?, NOW())"));
  }
}
