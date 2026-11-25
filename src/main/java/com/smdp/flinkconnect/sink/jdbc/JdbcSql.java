package com.smdp.flinkconnect.sink.jdbc;

import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * DB 종류와 상관없는 JDBC 도구 모음 (상태 없음).
 *
 *   - SQL 만들기: {@link #insertSql}, {@link #statement} (INSERT/UPSERT 선택)
 *   - 값 넣기: {@link #bindPlaceholders}, {@link #bindColumns}, {@link #toJdbcValue}
 *   - 테이블 칸·PK 읽기: {@link #open}, {@link #columns}, {@link #primaryKeys}, {@link #pick}
 *
 * 호출자: {@link JdbcAttacher}, {@link DynamicSink}, {@link JdbcBulkSink}, 각 방언.
 * DB마다 다른 문장(UPSERT)과 대량 적재(COPY/BulkCopy)는 여기 없고 각 어댑터가 직접 구현한다.
 *
 * 이름 비교는 대소문자를 무시한다. Postgres는 따옴표 없는 이름을 소문자로 저장해서,
 * 메시지 키 {@code METER_ID} 와 DB 컬럼 {@code meter_id} 가 같은 칸이다.
 *
 * 테이블 이름은 YAML 표기 그대로 SQL에 넣는다(따옴표 없음). 그래서 DB가 이름을 접는 규칙을 그대로 따른다.
 */
public final class JdbcSql {

  private JdbcSql() {}

  /**
   * DB 테이블 컬럼 한 칸의 정보. Job 시작 때 {@link #columns} 가 DB에서 읽어 채운다.
   * 공식 JdbcSink 의 바인딩 형식, MSSQL BulkCopy 의 칸 형식에 쓴다.
   *
   * @param name DB에 저장된 컬럼명 (Postgres는 보통 소문자)
   * @param jdbcType {@link java.sql.Types} 값
   * @param precision 자릿수 (DECIMAL·VARCHAR 길이 등). 모르면 0
   * @param scale 소수 자릿수. 모르면 0
   */
  public record Column(String name, int jdbcType, int precision, int scale) implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  // ---------------------------------------------------------------- SQL 만들기

  /**
   * INSERT 문장. {@code sqlExprs} 에 있는 컬럼은 {@code ?} 대신 식 문자열(예: NOW())을 넣는다.
   *
   * @param sqlExprs 컬럼명 → SQL 식. 대소문자 무시 맵이어야 한다({@link #caseInsensitive}).
   */
  public static String insertSql(String table, List<String> columns, Map<String, String> sqlExprs) {
    StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (");
    sb.append(String.join(", ", columns));
    sb.append(") VALUES (");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      String expr = sqlExprs.get(columns.get(i));
      sb.append(expr != null ? expr : "?");
    }
    sb.append(')');
    return sb.toString();
  }

  /** 메시지 한 행 기준 INSERT ({@code sink: dynamic} 경로 — 칸 구성이 행마다 다를 수 있음). */
  public static String insertSql(String table, WriteRowRecord row) {
    return insertSql(table, row.columns(), sqlExpressions(row));
  }

  /**
   * INSERT 또는 UPSERT 문장과, {@code ?}에 넣을 칸 이름.
   *
   * 공식 경로와 {@code sink: dynamic}이 같은 갈림을 타게 한다.
   * 식 맵은 여기서 대소문자를 무시하게 맞춘다. MSSQL MERGE 가 {@code containsKey} 로 식을 찾는다.
   * 어느 칸을 문장에 넣을지는 호출자가 정한다. 없는 칸을 NULL로 채울지 뺄지는 여기 없다.
   */
  public record Statement(String sql, List<String> placeholders) {}

  public static Statement statement(
      IJdbcDialect dialect,
      String table,
      WriteMode mode,
      List<String> columns,
      Map<String, String> sqlExprs,
      List<String> primaryKeys
  ) {
    Map<String, String> exprs = caseInsensitive(sqlExprs);
    String sql =
        mode == WriteMode.UPSERT
            ? dialect.upsertSql(table, columns, exprs, primaryKeys)
            : insertSql(table, columns, exprs);
    List<String> placeholders = new ArrayList<>();
    for (String column : columns) {
      if (!exprs.containsKey(column)) {
        placeholders.add(column);
      }
    }
    return new Statement(sql, List.copyOf(placeholders));
  }

  /** 행에 들어 있는 {@code op: sql} 컬럼 → 식. 대소문자 무시 맵. */
  public static Map<String, String> sqlExpressions(WriteRowRecord row) {
    Map<String, String> out = caseInsensitive(Map.of());
    for (String col : row.columns()) {
      if (row.isSql(col)) {
        out.put(col, row.sqlExpression(col));
      }
    }
    return out;
  }

  /** 대소문자 무시 복사본. TreeMap 비교자는 직렬화 가능해서 Sink 필드로 실어도 된다. */
  public static <V> Map<String, V> caseInsensitive(Map<String, V> source) {
    Map<String, V> out = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    out.putAll(source);
    return out;
  }

  // ---------------------------------------------------------------- 값 넣기

  /**
   * {@code sink: dynamic} 경로: 행의 일반 컬럼 값만 {@code ?} 자리에 순서대로 넣는다 ({@code op:sql} 제외).
   * 대상 컬럼 형식을 모르므로 드라이버가 Java 형으로 추론하게 둔다.
   */
  public static void bindPlaceholders(PreparedStatement ps, WriteRowRecord row) throws SQLException {
    int i = 1;
    for (String col : row.columns()) {
      if (!row.isSql(col)) {
        ps.setObject(i++, toJdbcValue(row.value(col)));
      }
    }
  }

  /**
   * 공식 경로: 테이블 컬럼 형식을 알고 있으므로 형식을 지정해 넣는다.
   * 메시지에 없는 칸은 NULL. (문장이 고정이라 칸을 빼고 DB DEFAULT를 타게 할 수 없다)
   */
  public static void bindColumns(
      PreparedStatement ps, List<Column> columns, Map<String, Object> rowCi
  ) throws SQLException {
    for (int i = 0; i < columns.size(); i++) {
      Column col = columns.get(i);
      Object v = toJdbcValue(rowCi.get(col.name()));
      if (v == null) {
        ps.setNull(i + 1, col.jdbcType());
      } else {
        ps.setObject(i + 1, v, col.jdbcType());
      }
    }
  }

  /** 행 → 대소문자 무시 맵 ({@code __sql:} 같은 내부 키 제외). 행마다 한 번 만든다. */
  public static Map<String, Object> rowCaseInsensitive(WriteRowRecord row) {
    Map<String, Object> out = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (String col : row.columns()) {
      if (!row.isSql(col)) {
        out.put(col, row.value(col));
      }
    }
    return out;
  }

  /**
   * java.time 값을 JDBC가 확실히 아는 형으로 바꾼다.
   * 컬럼 규칙 {@code op: format} 은 LocalDateTime 을 만드는데, 드라이버·대량 적재 API마다 지원이 갈린다.
   */
  public static Object toJdbcValue(Object v) {
    if (v instanceof LocalDateTime t) {
      return Timestamp.valueOf(t);
    }
    if (v instanceof LocalDate d) {
      return Date.valueOf(d);
    }
    return v;
  }

  // ---------------------------------------------------------------- 테이블 칸·PK 읽기

  /** endpoint 로 새 연결. 호출자가 닫는다. */
  public static Connection open(JdbcEndpointRecord endpoint, String driver) throws Exception {
    Class.forName(driver);
    return DriverManager.getConnection(endpoint.jdbcUrl(), endpoint.user(), endpoint.password());
  }

  /**
   * 테이블의 넣을 수 있는 컬럼 목록 (자동 증가 칸 제외, 테이블 순서).
   *
   * 행을 읽지 않는 {@code SELECT * ... WHERE 1=0} 의 결과 정보로 알아낸다.
   * DB가 이름을 직접 해석하므로 대소문자·스키마 표기 문제가 없다.
   * shortcut: 계산 컬럼(GENERATED)은 걸러내지 못한다. 그런 테이블이 생기면 YAML로 제외 목록을 받는다.
   */
  public static List<Column> columns(Connection c, String table) throws SQLException {
    List<Column> out = new ArrayList<>();
    try (java.sql.Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT * FROM " + table + " WHERE 1=0")) {
      ResultSetMetaData md = rs.getMetaData();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        // IDENTITY/serial 칸에 NULL을 넣으면 실패하므로 처음부터 넣지 않는다
        if (md.isAutoIncrement(i)) {
          continue;
        }
        out.add(new Column(md.getColumnName(i), md.getColumnType(i), md.getPrecision(i), md.getScale(i)));
      }
    }
    if (out.isEmpty()) {
      throw new IllegalStateException("넣을 수 있는 컬럼이 없습니다: " + table);
    }
    return List.copyOf(out);
  }

  /**
   * 테이블 PK 컬럼명 (키 순서). 없으면 빈 목록.
   *
   * {@code schema.table} 표기면 스키마를 나눠 찾는다. 이름은 표기 → 소문자 → 대문자 순으로 시도
   * (Postgres는 소문자, MSSQL은 보통 대소문자 무시라 첫 시도에 맞는다).
   */
  public static List<String> primaryKeys(Connection c, String table) throws SQLException {
    String schema = null;
    String name = table;
    int dot = table.lastIndexOf('.');
    if (dot > 0) {
      schema = table.substring(0, dot);
      name = table.substring(dot + 1);
    }
    DatabaseMetaData md = c.getMetaData();
    Set<String> tried = new LinkedHashSet<>(
        List.of(name, name.toLowerCase(Locale.ROOT), name.toUpperCase(Locale.ROOT)));
    for (String candidate : tried) {
      // KEY_SEQ 순서대로 정렬해야 복합 키 순서가 맞다
      Map<Short, String> bySeq = new TreeMap<>();
      try (ResultSet rs = md.getPrimaryKeys(c.getCatalog(), schema, candidate)) {
        while (rs.next()) {
          bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
        }
      }
      if (!bySeq.isEmpty()) {
        return List.copyOf(bySeq.values());
      }
    }
    return List.of();
  }

  /**
   * 행이 가진 칸 이름 순서대로 테이블 컬럼 정보를 고른다 ({@code sink: dynamic} 대량 적재용).
   *
   * @throws IllegalStateException 테이블에 없는 칸이 메시지에 있을 때. 조용히 버리지 않는다.
   */
  public static List<Column> pick(String table, List<Column> tableColumns, List<String> rowColumns) {
    Map<String, Column> byName = caseInsensitive(Map.of());
    for (Column col : tableColumns) {
      byName.put(col.name(), col);
    }
    List<Column> out = new ArrayList<>(rowColumns.size());
    for (String name : rowColumns) {
      Column col = byName.get(name);
      if (col == null) {
        throw new IllegalStateException("테이블 " + table + " 에 컬럼 " + name + " 이 없습니다.");
      }
      out.add(col);
    }
    return out;
  }
}
