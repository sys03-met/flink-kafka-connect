package com.smdp.flinkconnect.transform;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import com.smdp.flinkconnect.transform.plugin.IColumnPlugin;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * YAML {@code columns[].op} 규칙을 메시지 필드(Map)에 적용한다.
 *
 * 밖에서 쓰는 입구는 두 개뿐이다.
 *
 *   - {@link #apply} — 행 하나에 규칙 목록을 YAML 순서대로 적용한 복사본을 돌려준다.
 *   - {@link #requireKnownOps} — Job 시작 전에 모르는 op가 있으면 실패시킨다.
 *
 * op 이름 목록은 {@link #OPS} 한곳에만 있다. 새 op는 여기 한 줄 + 처리 메서드 하나.
 *
 * {@code op: sql} 이면 Map 키를 {@code __sql:컬럼}({@link #SQL_META_PREFIX})으로 남기고,
 * JDBC 적재 쪽이 그걸 보고 VALUES 자리에 식(예: NOW())을 붙인다.
 *
 * 공유 상태: 수식 평가기·플러그인 로더는 Job 전체에서 하나다. 둘 다 상태가 없거나
 * (평가기) 동시 접근에 안전한 캐시(플러그인 로더 ConcurrentHashMap)라 여러 Task가 같이 써도 된다.
 */
public final class ColumnOpHandlers {

  /**
   * SQL 식 키 접두사 ({@link WriteRowRecord#SQL_PREFIX}와 동일).
   *
   * 예: field {@code INGEST_DT} + op sql → 키 {@code __sql:INGEST_DT}, 값 {@code NOW()}.
   */
  public static final String SQL_META_PREFIX = WriteRowRecord.SQL_PREFIX;

  private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluator();

  /** 플러그인 짧은 이름({@code ScalePlugin})에 붙일 기본 패키지. 플러그인 클래스는 이 패키지 최상위에 둔다. */
  public static final String PLUGIN_PACKAGE = "com.smdp.flinkconnect.transform.plugin.";

  /** 전체 클래스명 → 플러그인. 메시지마다 리플렉션하지 않게 Job 전체에서 하나 (동시 접근 안전). */
  private static final Map<String, IColumnPlugin> PLUGINS = new ConcurrentHashMap<>();

  /** YAML {@code javaFunction} 이름 → 함수. 함수 추가는 {@link #javaFunctions} 에 put. */
  private static final Map<String, BiFunction<Map<String, Object>, ColumnRule, Object>>
      JAVA_FUNCTIONS = javaFunctions();

  /** op 이름(소문자) → 처리. 이 맵이 허용 op 목록의 유일한 원본이다. */
  private static final Map<String, BiConsumer<Map<String, Object>, ColumnRule>> OPS = ops();

  private ColumnOpHandlers() {}

  /**
   * columns 규칙을 YAML 순서대로 적용한다. 입력 Map은 건드리지 않고 복사본을 고친다.
   *
   * @throws IllegalArgumentException 모르는 op. 조용히 건너뛰면 칸이 빠진 채 DB에 들어가서다.
   */
  public static Map<String, Object> apply(Map<String, Object> record, List<ColumnRule> rules) {
    // 원본 payload 보존 — 같은 메시지가 다른 DB로도 가므로 규칙은 복사본에만
    Map<String, Object> row = new LinkedHashMap<>(record != null ? record : Map.of());
    if (rules == null) {
      return row;
    }
    for (ColumnRule rule : rules) {
      // op 없는 줄은 아무 일도 안 하는 줄로 본다 (YAML에 field만 적어 둔 경우)
      if (rule.op() == null) {
        continue;
      }
      opFor(rule.op()).accept(row, rule);
    }
    return row;
  }

  /** 모르는 op가 있으면 실패. Resolver가 Job 시작 전에 설정 전체로 부른다. */
  public static void requireKnownOps(List<ColumnRule> rules) {
    if (rules == null) {
      return;
    }
    for (ColumnRule rule : rules) {
      if (rule.op() != null) {
        opFor(rule.op());
      }
    }
  }

  private static BiConsumer<Map<String, Object>, ColumnRule> opFor(String op) {
    BiConsumer<Map<String, Object>, ColumnRule> fn = OPS.get(op.toLowerCase(Locale.ROOT));
    if (fn == null) {
      throw new IllegalArgumentException("Unknown column op: " + op + " (허용: " + OPS.keySet() + ")");
    }
    return fn;
  }

  /** 허용 op 목록과 처리 메서드를 묶는다. YAML의 op는 대소문자 무시. */
  private static Map<String, BiConsumer<Map<String, Object>, ColumnRule>> ops() {
    Map<String, BiConsumer<Map<String, Object>, ColumnRule>> m = new LinkedHashMap<>();
    m.put("rename", ColumnOpHandlers::rename);
    m.put("drop", (row, rule) -> row.remove(rule.field()));
    m.put("const", (row, rule) -> row.put(rule.field(), rule.value()));
    m.put("type", ColumnOpHandlers::convertType);
    m.put("format", ColumnOpHandlers::format);
    m.put("expression", ColumnOpHandlers::expression);
    m.put("javafunction", ColumnOpHandlers::javaFunction);
    m.put("plugin", ColumnOpHandlers::plugin);
    m.put("sql", (row, rule) -> row.put(SQL_META_PREFIX + rule.field(), rule.expression()));
    return Map.copyOf(m);
  }

  /** field → to 이름 변경. 원본 키 제거. 원본이 없으면 아무 일 없음. */
  private static void rename(Map<String, Object> row, ColumnRule rule) {
    if (rule.to() == null || !row.containsKey(rule.field())) {
      return;
    }
    row.put(rule.to(), row.remove(rule.field()));
  }

  /** 형 변환. 모르는 type은 원본 유지. */
  private static void convertType(Map<String, Object> row, ColumnRule rule) {
    if (!row.containsKey(rule.field()) || rule.type() == null) {
      return;
    }
    Object raw = row.get(rule.field());
    if (raw == null) {
      return;
    }
    String s = String.valueOf(raw);
    row.put(
        rule.field(),
        switch (rule.type().toLowerCase(Locale.ROOT)) {
          case "string" -> s;
          case "long", "bigint" -> Long.valueOf(s);
          case "int", "integer" -> Integer.valueOf(s);
          case "double" -> Double.valueOf(s);
          case "decimal", "number" -> new BigDecimal(s);
          case "boolean" -> Boolean.valueOf(s);
          default -> raw;
        });
  }

  /**
   * 날짜/시간 포맷.
   *
   * String이면 parse → LocalDateTime, Number면 epoch milli → format 문자열.
   */
  private static void format(Map<String, Object> row, ColumnRule rule) {
    if (!row.containsKey(rule.field()) || rule.format() == null) {
      return;
    }
    Object raw = row.get(rule.field());
    if (raw == null) {
      return;
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(rule.format());
    if (raw instanceof String s) {
      row.put(rule.field(), LocalDateTime.parse(s, formatter));
    } else if (raw instanceof Number n) {
      Instant instant = Instant.ofEpochMilli(n.longValue());
      row.put(
          rule.field(), LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).format(formatter));
    }
  }

  /** 같은 row 필드 수식. 엔진: {@link ExpressionEvaluator}. */
  private static void expression(Map<String, Object> row, ColumnRule rule) {
    if (rule.expression() == null || rule.expression().isBlank()) {
      return;
    }
    row.put(rule.field(), EVALUATOR.evaluate(rule.expression(), row));
  }

  /** 등록된 Java 함수 호출. YAML: {@code op: javaFunction}, {@code expression: upper}. */
  private static void javaFunction(Map<String, Object> row, ColumnRule rule) {
    String name = rule.expression();
    if (name == null) {
      return;
    }
    BiFunction<Map<String, Object>, ColumnRule, Object> fn = JAVA_FUNCTIONS.get(name);
    if (fn == null) {
      throw new IllegalArgumentException("Unknown javaFunction: " + name);
    }
    row.put(rule.field(), fn.apply(row, rule));
  }

  /**
   * JAR에 빌드된 {@code IColumnPlugin}을 클래스명으로 호출.
   *
   * YAML: {@code op: plugin}, {@code expression: ScalePlugin} + {@code params}.
   * 짧은 javaFunction 과 달리, 긴 변환 로직을 별도 클래스로 둘 자리.
   */
  private static void plugin(Map<String, Object> row, ColumnRule rule) {
    String className = rule.expression();
    if (className == null || className.isBlank()) {
      throw new IllegalArgumentException("plugin requires expression=class name");
    }
    // 점이 없으면 짧은 이름 → 기본 패키지를 붙인다
    String fqcn = className.indexOf('.') < 0 ? PLUGIN_PACKAGE + className.trim() : className.trim();
    PLUGINS.computeIfAbsent(fqcn, ColumnOpHandlers::loadPlugin).apply(row, rule);
  }

  /** 같은 JAR에서 {@code Class.forName} + 기본 생성자. IColumnPlugin 이 아니면 실패. */
  private static IColumnPlugin loadPlugin(String fqcn) {
    try {
      Class<?> clazz = Class.forName(fqcn);
      if (!IColumnPlugin.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Class does not implement IColumnPlugin: " + fqcn);
      }
      return (IColumnPlugin) clazz.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot load IColumnPlugin: " + fqcn, e);
    }
  }

  /** YAML {@code javaFunction} 이름 레지스트리. */
  private static Map<String, BiFunction<Map<String, Object>, ColumnRule, Object>> javaFunctions() {
    Map<String, BiFunction<Map<String, Object>, ColumnRule, Object>> map = new LinkedHashMap<>();
    map.put("upper", (row, rule) -> str(source(row, rule)).toUpperCase(Locale.ROOT));
    map.put("lower", (row, rule) -> str(source(row, rule)).toLowerCase(Locale.ROOT));
    map.put("trim", (row, rule) -> str(source(row, rule)).trim());
    map.put(
        "length",
        (row, rule) -> {
          Object v = source(row, rule);
          return v == null ? 0 : str(v).length();
        });
    map.put(
        "coalesce",
        (row, rule) -> {
          // 같은 메시지 안에서만 대체. 이전 시각 보간은 상태 연산자가 필요함
          Object primary = source(row, rule);
          if (primary != null && !str(primary).isBlank()) {
            return primary;
          }
          return rule.value();
        });
    return Map.copyOf(map);
  }

  /** 입력 소스: {@code to}가 이미 있으면 그쪽, 없으면 {@code field}. */
  private static Object source(Map<String, Object> row, ColumnRule rule) {
    if (rule.to() != null && row.containsKey(rule.to())) {
      return row.get(rule.to());
    }
    return row.get(rule.field());
  }

  private static String str(Object v) {
    return v == null ? "" : String.valueOf(v);
  }
}
