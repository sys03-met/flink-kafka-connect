package com.smdp.flinkconnect.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 컬럼 규칙 테스트 (YAML {@code columns[].op} → 행 Map → WriteRowRecord).
 *
 * 입구 {@link ColumnOpHandlers#apply} 로만 확인한다 (op 별 내부 메서드는 직접 부르지 않는다).
 * 수식 엔진 {@link ExpressionEvaluator} 와 {@code op: sql} 결과를 읽는 {@link WriteRowRecord} 도 여기서 함께 본다.
 */
class ColumnOpHandlersTest {

  /** rename/drop/const/expression/sql 이 한 번에 YAML 순서대로 적용된다. */
  @Test
  void appliesRenameDropConstExpressionAndSqlMarker() {
    Map<String, Object> input = new HashMap<>();
    input.put("FAP", 1.5);
    input.put("LARAP", 0.5);
    input.put("noise", "x");

    Map<String, Object> out =
        ColumnOpHandlers.apply(
            input,
            List.of(
                new ColumnRule("noise", "drop", null, null, null, null, null),
                new ColumnRule("TOTAL", "expression", null, null, null, null, "FAP+LARAP"),
                new ColumnRule("SOURCE", "const", null, null, null, "flink", null),
                new ColumnRule("INGEST_DT", "sql", null, null, null, null, "NOW()"),
                new ColumnRule("FAP", "rename", "fap_kwh", null, null, null, null)));

    assertFalse(out.containsKey("noise"));
    assertEquals(2.0, ((Number) out.get("TOTAL")).doubleValue(), 0.0001);
    assertEquals("flink", out.get("SOURCE"));
    assertEquals("NOW()", out.get(ColumnOpHandlers.SQL_META_PREFIX + "INGEST_DT"));
    assertTrue(out.containsKey("fap_kwh"));
    assertFalse(out.containsKey("FAP"));
  }

  /** javaFunction: upper, coalesce(값이 비면 value 로 대체). */
  @Test
  void javaFunctionUpperAndCoalesce() {
    Map<String, Object> row = new HashMap<>();
    row.put("METER_ID", "m1");
    row.put("NAME", null);

    Map<String, Object> out =
        ColumnOpHandlers.apply(
            row,
            List.of(
                new ColumnRule("METER_ID", "javaFunction", null, null, null, null, "upper"),
                new ColumnRule("NAME", "javaFunction", null, null, null, "UNKNOWN", "coalesce")));

    assertEquals("M1", out.get("METER_ID"));
    assertEquals("UNKNOWN", out.get("NAME"));
  }

  /** 모르는 op 는 건너뛰지 않고 실패 (칸이 빠진 채 DB에 들어가는 걸 막는다). */
  @Test
  void unknownOp_failsInsteadOfSkipping() {
    List<ColumnRule> rules =
        List.of(new ColumnRule("METER_ID", "uppercase", null, null, null, null, null));

    assertThrows(IllegalArgumentException.class, () -> ColumnOpHandlers.apply(Map.of(), rules));
    assertThrows(IllegalArgumentException.class, () -> ColumnOpHandlers.requireKnownOps(rules));
  }

  /** op: plugin — 짧은 클래스명(기본 패키지 부착) + params. */
  @Test
  void pluginShortClassNameAndParams() {
    Map<String, Object> row = new HashMap<>();
    row.put("FAP", 1000);

    Map<String, Object> out =
        ColumnOpHandlers.apply(
            row,
            List.of(
                new ColumnRule(
                    "FAP_KWH", "plugin", null, null, null, null, "ScalePlugin",
                    Map.of("source", "FAP", "factor", 0.001))));

    assertEquals(1.0, (Double) out.get("FAP_KWH"), 1e-9);
  }

  /** 수식: 산술·괄호·필드 참조. */
  @Test
  void expressionArithmeticWithFieldsAndParens() {
    Map<String, Object> row = Map.of("FAP", 1.5, "LARAP", 0.5, "CT", 2);
    Object result = new ExpressionEvaluator().evaluate("(FAP + LARAP) * CT", row);
    assertEquals(4.0, ((Number) result).doubleValue(), 0.0001);
  }

  /** 수식: 문자열 + 는 이어 붙이기. */
  @Test
  void expressionConcatenatesStringsWithPlus() {
    Map<String, Object> row = Map.of("A", "hv", "B", "lp");
    assertEquals("hv-lp", new ExpressionEvaluator().evaluate("A + '-' + B", row));
  }

  /** WriteRowRecord 는 Map 을 그대로 들고 {@code __sql:} 접두사만 해석한다. */
  @Test
  void writeRowKeepsSqlPrefixAndExposesColumns() {
    Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("METER_ID", "M1");
    raw.put(WriteRowRecord.SQL_PREFIX + "INGEST_DT", "NOW()");

    WriteRowRecord row = WriteRowRecord.of(raw);

    assertEquals("M1", row.value("METER_ID"));
    assertTrue(row.isSql("INGEST_DT"));
    assertEquals("NOW()", row.sqlExpression("INGEST_DT"));
    assertTrue(row.hasSql());
    assertEquals(List.of("METER_ID", "INGEST_DT"), row.columns());
  }
}
