package com.smdp.flinkconnect.e2e;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.job.PipelineJob;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * E2E 공용 도구.
 *
 *   - {@link #writeAll}: WriteRecord 를 로컬 Flink + 실제 Sink 로 적재 (Kafka 없이)
 *   - {@link Doris}: 실제 Doris(all-in-one) 테이블 만들기·세기
 *
 * 대량 건수는 fromData(그래프에 직렬화) 대신 SourceFunction 이터레이터로 흘린다.
 */
final class FlinkLocalWrites {

  private FlinkLocalWrites() {}

  /** WriteRecord 목록을 Job 과 같은 Sink 입구({@link PipelineJob#attachSinks})로 적재하고 끝날 때까지 기다린다. */
  static void writeAll(
      List<WriteRecord> requests, Map<SystemType, JdbcEndpointRecord> endpoints, int batchSize
  ) throws Exception {
    if (requests == null || requests.isEmpty()) {
      return;
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<WriteRecord> stream =
        env.addSource(new IterSource(requests), TypeInformation.of(WriteRecord.class))
            .name("e2e-requests");
    // E2E는 YAML 없이 WriteRecord만 있으므로, 그래프 재료(계획 Destination)를 여기서 만든다
    PipelineJob.attachSinks(
        stream,
        endpoints,
        batchSize,
        1000L,
        plannedFrom(requests),
        new PipelineConfig(null, null, null, Map.of()));
    env.execute("flink-connect-e2e-write");
  }

  /**
   * WriteRecord 의 갈 곳을 (DB, 테이블, 방식, sink) 마다 하나로 모은다.
   * expand 가 컬럼 규칙을 비워 두므로, 행의 {@code __sql:} 칸을 {@code op: sql} 규칙으로 되돌린다
   * (공식 JdbcSink 는 SQL 식을 문장에 미리 넣어야 해서 필요).
   */
  private static List<DestinationConfig> plannedFrom(List<WriteRecord> requests) {
    Map<String, DestinationConfig> byKey = new LinkedHashMap<>();
    for (WriteRecord r : requests) {
      DestinationConfig d = r.destination();
      String key = d.system() + "|" + d.target().toUpperCase() + "|" + d.writeMode() + "|" + d.sink();
      byKey.computeIfAbsent(
          key,
          k ->
              new DestinationConfig(
                  d.system(), d.target(), d.writeMode(), sqlRules(r.row()), d.primaryKeys(), d.sink()));
    }
    return new ArrayList<>(byKey.values());
  }

  /** 행의 {@code __sql:} 칸 → {@code op: sql} 컬럼 규칙. */
  private static List<ColumnRule> sqlRules(WriteRowRecord row) {
    List<ColumnRule> out = new ArrayList<>();
    for (String col : row.columns()) {
      if (row.isSql(col)) {
        out.add(new ColumnRule(col, "sql", null, null, null, null, row.sqlExpression(col)));
      }
    }
    return out;
  }

  /** 테스트용 Source. 리스트를 순서대로 emit. */
  @SuppressWarnings("deprecation")
  static final class IterSource implements SourceFunction<WriteRecord> {
    private static final long serialVersionUID = 1L;
    private final List<WriteRecord> requests;
    private volatile boolean running = true;

    IterSource(List<WriteRecord> requests) {
      this.requests = requests;
    }

    /** Task가 돌 때 호출되는 본문. ctx.collect 할 때마다 다운스트림(Sink)으로 한 건. */
    @Override
    public void run(SourceContext<WriteRecord> ctx) {
      Iterator<WriteRecord> it = requests.iterator();
      while (running && it.hasNext()) {
        ctx.collect(it.next());
      }
    }

    /** Flink가 Job 취소 시 호출. run 루프를 끊는다. */
    @Override
    public void cancel() {
      running = false;
    }
  }

  /**
   * 실제 Doris(all-in-one) JDBC 헬퍼.
   * root 비밀번호 없음. 테이블은 UNIQUE KEY + DISTRIBUTED BY HASH.
   */
  static final class Doris {

    static final String JDBC = "jdbc:mysql://localhost:9030/rd";
    static final String USER = "root";
    static final String PASS = "";

    private Doris() {}

    static Connection connect() throws Exception {
      return DriverManager.getConnection(JDBC, USER, PASS);
    }

    /** DB·RD_LP 없으면 생성 (idempotent). */
    static void ensureRdLp() throws Exception {
      ensureDatabase();
      try (Connection c = connect();
          Statement st = c.createStatement()) {
        st.execute(
            """
            CREATE TABLE IF NOT EXISTS RD_LP (
              METER_ID VARCHAR(64),
              FAP DOUBLE,
              LARAP DOUBLE,
              SERVER_DT VARCHAR(32),
              TOTAL_POWER DOUBLE
            ) UNIQUE KEY(METER_ID)
            DISTRIBUTED BY HASH(METER_ID) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            """);
      }
    }

    /** 패턴 토픽용 검침 테이블 (컬럼 최소). */
    static void ensureRdTable(String table) throws Exception {
      ensureDatabase();
      try (Connection c = connect();
          Statement st = c.createStatement()) {
        st.execute(
            "CREATE TABLE IF NOT EXISTS "
                + table
                + " ("
                + "METER_ID VARCHAR(64), FAP DOUBLE, LARAP DOUBLE"
                + ") UNIQUE KEY(METER_ID) "
                + "DISTRIBUTED BY HASH(METER_ID) BUCKETS 1 "
                + "PROPERTIES (\"replication_num\" = \"1\")");
      }
    }

    /** rd DB 는 테이블보다 먼저 있어야 해서 DB 없이 접속해 만든다. */
    private static void ensureDatabase() throws Exception {
      try (Connection c = DriverManager.getConnection("jdbc:mysql://localhost:9030/", USER, PASS);
          Statement st = c.createStatement()) {
        st.execute("CREATE DATABASE IF NOT EXISTS rd");
      }
    }

    static void truncate(String table) throws Exception {
      try (Connection c = connect();
          Statement st = c.createStatement()) {
        st.execute("TRUNCATE TABLE " + table);
      }
    }

    static int count(String table) throws Exception {
      try (Connection c = connect();
          ResultSet rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
        rs.next();
        return rs.getInt(1);
      }
    }

    static int countWhere(String table, String meterId) throws Exception {
      try (Connection c = connect();
          var ps = c.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE METER_ID = ?")) {
        ps.setString(1, meterId);
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          return rs.getInt(1);
        }
      }
    }
  }
}
