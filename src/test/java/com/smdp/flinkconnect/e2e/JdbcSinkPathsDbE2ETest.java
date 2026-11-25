package com.smdp.flinkconnect.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Postgres / MSSQL 실제 DB로 네 가지 적재 길을 확인한다 (Kafka·Doris 없이).
 *
 *   - 공식 JdbcSink UPSERT (키는 테이블 PK에서 읽음, op: sql 식 포함)
 *   - 공식 경로 BULK (Postgres COPY / MSSQL BulkCopy)
 *   - {@code sink: jdbc} UPSERT / BULK (우리 JDBC Sink)
 *
 * 전제
 *
 *   - Postgres: {@code .tools/.test/flink-e2e} 의 postgres (localhost:5432, rd/rd/rd)
 *   - MSSQL: 1433 은 다른 프로젝트일 수 있어 기본 14330. {@code E2E_MSSQL_PORT}, {@code E2E_MSSQL_PASSWORD} 로 바꾼다
 *
 * 포트가 닫혀 있으면 건너뛴다. 테스트 전용 테이블 E2E_* 만 지우고 다시 만든다.
 */
class JdbcSinkPathsDbE2ETest {

  @Test
  void postgres_allPaths() throws Exception {
    assumeTrue(portOpen(5432), "Postgres 5432 down");
    run(
        SystemType.POSTGRES,
        new JdbcEndpointRecord("jdbc:postgresql://localhost:5432/rd", "rd", "rd"),
        "DOUBLE PRECISION",
        "TIMESTAMP",
        "NOW()");
  }

  @Test
  void mssql_allPaths() throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("E2E_MSSQL_PORT", "14330"));
    assumeTrue(portOpen(port), "MSSQL " + port + " down");
    String password = System.getenv().getOrDefault("E2E_MSSQL_PASSWORD", "Verify#Pass123");
    run(
        SystemType.MSSQL,
        new JdbcEndpointRecord(
            "jdbc:sqlserver://localhost:" + port
                + ";databaseName=master;encrypt=true;trustServerCertificate=true",
            "sa",
            password),
        "FLOAT",
        "DATETIME2",
        "SYSDATETIME()");
  }

  /** 테이블 준비 → 레코드 적재 → 결과 확인. DB마다 다른 건 칸 형식과 "지금 시각" 식뿐. */
  private static void run(
      SystemType db, JdbcEndpointRecord ep, String doubleType, String tsType, String nowExpr
  ) throws Exception {
    try (Connection c = DriverManager.getConnection(ep.jdbcUrl(), ep.user(), ep.password());
        Statement st = c.createStatement()) {
      for (String t : List.of("E2E_OFF_UP", "E2E_OFF_BULK", "E2E_JDBC_UP", "E2E_JDBC_BULK")) {
        st.execute("DROP TABLE IF EXISTS " + t);
      }
      st.execute("CREATE TABLE E2E_OFF_UP (ID VARCHAR(32) PRIMARY KEY, V " + doubleType
          + ", NOTE VARCHAR(64), INGEST_DT " + tsType + ")");
      st.execute("CREATE TABLE E2E_OFF_BULK (ID VARCHAR(32), V " + doubleType + ", NOTE VARCHAR(64))");
      st.execute("CREATE TABLE E2E_JDBC_UP (ID VARCHAR(32) PRIMARY KEY, V " + doubleType
          + ", NOTE VARCHAR(64))");
      st.execute("CREATE TABLE E2E_JDBC_BULK (ID VARCHAR(32), V " + doubleType + ", NOTE VARCHAR(64))");
    }

    List<WriteRecord> records = new ArrayList<>();
    // 같은 키 두 번 → 두 번째 값으로 갱신돼야 한다. primaryKeys 는 비워 테이블 PK 조회를 탄다
    records.add(rec(db, "E2E_OFF_UP", WriteMode.UPSERT, SinkMode.OFFICIAL, row("a", 1.0, "first", nowExpr)));
    records.add(rec(db, "E2E_OFF_UP", WriteMode.UPSERT, SinkMode.OFFICIAL, row("a", 2.0, "second", nowExpr)));
    records.add(rec(db, "E2E_OFF_UP", WriteMode.UPSERT, SinkMode.OFFICIAL, row("b", 3.0, "b", nowExpr)));
    // 탭이 든 값(COPY 이스케이프), Integer 값(BulkCopy 형 맞춤)
    records.add(rec(db, "E2E_OFF_BULK", WriteMode.BULK, SinkMode.OFFICIAL, row("1", 5, "x\ty", null)));
    records.add(rec(db, "E2E_OFF_BULK", WriteMode.BULK, SinkMode.OFFICIAL, row("2", 6.5, null, null)));
    records.add(rec(db, "E2E_OFF_BULK", WriteMode.BULK, SinkMode.OFFICIAL, row("3", 7.0, "z", null)));
    records.add(rec(db, "E2E_JDBC_UP", WriteMode.UPSERT, SinkMode.JDBC, row("a", 1.0, "first", null)));
    records.add(rec(db, "E2E_JDBC_UP", WriteMode.UPSERT, SinkMode.JDBC, row("a", 2.0, "second", null)));
    records.add(rec(db, "E2E_JDBC_BULK", WriteMode.BULK, SinkMode.JDBC, row("1", 1.0, "p", null)));
    records.add(rec(db, "E2E_JDBC_BULK", WriteMode.BULK, SinkMode.JDBC, row("2", 2, "q\tr", null)));

    FlinkLocalWrites.writeAll(records, Map.of(db, ep), 2);

    try (Connection c = DriverManager.getConnection(ep.jdbcUrl(), ep.user(), ep.password())) {
      assertEquals(2, count(c, "E2E_OFF_UP"));
      assertEquals(2.0, number(c, "SELECT V FROM E2E_OFF_UP WHERE ID = 'a'"));
      assertNotNull(value(c, "SELECT INGEST_DT FROM E2E_OFF_UP WHERE ID = 'a'"));
      assertEquals(3, count(c, "E2E_OFF_BULK"));
      assertEquals("x\ty", value(c, "SELECT NOTE FROM E2E_OFF_BULK WHERE ID = '1'"));
      assertEquals(1, count(c, "E2E_JDBC_UP"));
      assertEquals(2.0, number(c, "SELECT V FROM E2E_JDBC_UP WHERE ID = 'a'"));
      assertEquals(2, count(c, "E2E_JDBC_BULK"));
    }
  }

  private static WriteRecord rec(SystemType db, String table, WriteMode mode, SinkMode sink, WriteRowRecord row) {
    return new WriteRecord(new DestinationConfig(db, table, mode, List.of(), List.of(), sink), row);
  }

  /** nowExpr 가 있으면 INGEST_DT 를 op: sql 칸으로 넣는다 (expand 가 남기는 모양 그대로). */
  private static WriteRowRecord row(String id, Object v, String note, String nowExpr) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID", id);
    m.put("V", v);
    m.put("NOTE", note);
    if (nowExpr != null) {
      m.put(WriteRowRecord.SQL_PREFIX + "INGEST_DT", nowExpr);
    }
    return WriteRowRecord.of(m);
  }

  private static int count(Connection c, String table) throws Exception {
    return ((Number) value(c, "SELECT COUNT(*) FROM " + table)).intValue();
  }

  private static double number(Connection c, String sql) throws Exception {
    return ((Number) value(c, sql)).doubleValue();
  }

  private static Object value(Connection c, String sql) throws Exception {
    try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      return rs.next() ? rs.getObject(1) : null;
    }
  }

  private static boolean portOpen(int port) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("localhost", port), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
