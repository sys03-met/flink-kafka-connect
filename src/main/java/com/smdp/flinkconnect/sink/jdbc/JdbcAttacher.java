package com.smdp.flinkconnect.sink.jdbc;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.sink.jdbc.JdbcSql.Column;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Postgres/MSSQL 쓰기 갈래를 스트림에 붙인다 ({@code PostgresSinkAdapter} / {@code MssqlSinkAdapter} 가 호출).
 *
 * 그래프 모양
 *
 * <pre>
 * writes → [이 DB만] → [라우터] ─ 대상1(INSERT/UPSERT) → 공식 JdbcSink
 *                              ├ 대상2(BULK)          → JdbcBulkSink (COPY / BulkCopy)
 *                              └ sink: jdbc 레코드    → DynamicTableJdbcSink
 * </pre>
 *
 * 대상 = (테이블, 넣는 방식) 하나. 공식 JdbcSink 는 대상마다 SQL 한 문장을 미리 정해야 해서,
 * 그래프를 그릴 때 DB에 접속해 테이블 칸·PK를 읽는다.
 * 그래서 설정과 테이블이 안 맞으면(키 없음, 없는 칸) 메시지가 오기 전에 Job 시작이 실패한다.
 */
public final class JdbcAttacher {

  private JdbcAttacher() {}

  /**
   * JDBC 계열 DB 갈래 부착.
   *
   * @param writes 전체 WriteRecord 스트림
   * @param db 이 호출이 담당하는 DB (POSTGRES 또는 MSSQL)
   * @param endpoint 해당 DB JDBC 접속
   * @param batch Sink 배치 크기
   * @param batchIntervalMs 건수가 안 차도 이 밀리초가 지나면 버퍼를 내보낸다
   * @param planned 이 DB로 가는 Destination 전부 (구독 토픽을 미리 resolve 한 것)
   * @param dialect DB마다 다른 SQL·대량 적재 (어댑터 자신)
   */
  public static void attach(
      DataStream<WriteRecord> writes,
      SystemType db,
      JdbcEndpointRecord endpoint,
      int batch,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      IJdbcDialect dialect
  ) {
    // 이 DB로 갈 레코드가 없는 설정이면 갈래를 만들지 않는다
    if (planned.isEmpty()) {
      return;
    }
    boolean customJdbc = planned.stream().anyMatch(d -> d.sink() == SinkMode.JDBC);
    List<Target> targets = loadTableInfo(endpoint, dialect, plan(planned));
    String dbName = db.name().toLowerCase(Locale.ROOT);

    // 대상마다 옆 출구(OutputTag) 하나. 레코드도 같은 keyOf 로 출구를 찾는다
    Map<String, OutputTag<WriteRecord>> tags = new HashMap<>();
    for (Target t : targets) {
      String key = keyOf(t.table(), t.writeMode());
      tags.put(key, new OutputTag<>(key, TypeInformation.of(WriteRecord.class)));
    }

    SingleOutputStreamOperator<WriteRecord> routed =
        writes
            .filter((FilterFunction<WriteRecord>) r -> r.destination().system() == db)
            .name("to-" + dbName)
            .process(new TargetRouter(tags, customJdbc))
            .name("route-" + dbName);

    JdbcConnectionOptions connection =
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(endpoint.jdbcUrl())
            .withDriverName(dialect.driver())
            .withUsername(endpoint.user())
            .withPassword(endpoint.password())
            .build();
    JdbcExecutionOptions execution =
        JdbcExecutionOptions.builder()
            .withBatchSize(batch)
            .withBatchIntervalMs(batchIntervalMs)
            .build();

    for (Target t : targets) {
      String key = keyOf(t.table(), t.writeMode());
      String name = dbName + "-" + key.toLowerCase(Locale.ROOT).replace('|', '-');
      DataStream<WriteRecord> side = routed.getSideOutput(tags.get(key));
      // BULK 는 공식 JdbcSink 에 COPY/BulkCopy 통로가 없어서 같은 DB 전용 API로 직접 넣는다
      if (t.writeMode() == WriteMode.BULK) {
        side.addSink(
                new JdbcBulkSink(endpoint, dialect, batch, batchIntervalMs, t.table(), t.columns()))
            .name("bulk-" + name);
        continue;
      }
      List<String> names = t.columns().stream().map(Column::name).toList();
      String sql =
          t.writeMode() == WriteMode.UPSERT
              ? dialect.upsertSql(t.table(), names, t.sqlExprs(), t.primaryKeys())
              : JdbcSql.insertSql(t.table(), names, t.sqlExprs());
      // ? 자리 = SQL 식이 없는 칸만 (식 칸은 문장에 이미 들어 있다)
      List<Column> bind =
          t.columns().stream().filter(c -> !t.sqlExprs().containsKey(c.name())).toList();
      side.sinkTo(
              JdbcSink.<WriteRecord>builder()
                  .withQueryStatement(sql, new TargetStatementBuilder(bind))
                  .withExecutionOptions(execution)
                  .buildAtLeastOnce(connection))
          .name("official-jdbc-" + name);
    }

    if (customJdbc) {
      routed.addSink(new DynamicTableJdbcSink(endpoint, dialect, batch, batchIntervalMs))
          .name("custom-jdbc-" + dbName);
    }
  }

  /**
   * 공식 경로 대상 하나. 처음엔 칸이 비어 있고 {@link #loadTableInfo} 가 DB에서 읽어 채운다.
   *
   * @param sqlExprs {@code op: sql} 칸 → 식 (대소문자 무시)
   */
  record Target(
      String table,
      WriteMode writeMode,
      List<String> primaryKeys,
      Map<String, String> sqlExprs,
      List<Column> columns
  ) {}

  /** 라우팅 키. 레코드와 대상이 같은 규칙으로 만들어야 같은 출구로 간다. 테이블은 대소문자 무시. */
  static String keyOf(String table, WriteMode mode) {
    return table.toUpperCase(Locale.ROOT) + "|" + mode.name();
  }

  /**
   * 공식 경로 대상 목록 ({@code sink: jdbc} 는 제외). 칸 정보는 아직 비어 있다.
   *
   * 같은 키가 여러 토픽에서 나오면 하나로 합친다. 대상마다 SQL 한 문장이라,
   * primaryKeys 나 SQL 식이 서로 다르면 합칠 수 없어서 실패시킨다.
   */
  static List<Target> plan(List<DestinationConfig> planned) {
    Map<String, Target> byKey = new LinkedHashMap<>();
    for (DestinationConfig d : planned) {
      if (d.sink() != SinkMode.OFFICIAL) {
        continue;
      }
      Target next = new Target(d.target(), d.writeMode(), d.primaryKeys(), sqlExprs(d.columns()), List.of());
      String key = keyOf(d.target(), d.writeMode());
      Target prev = byKey.putIfAbsent(key, next);
      if (prev != null && !sameStatement(prev, next)) {
        throw new IllegalStateException(
            "같은 테이블·같은 넣는 방식인데 primaryKeys 또는 op: sql 이 서로 다릅니다 ("
                + key
                + "). 공식 Sink는 대상마다 SQL 한 문장이라 맞춰 주세요.");
      }
    }
    return new ArrayList<>(byKey.values());
  }

  /** 컬럼 규칙 중 {@code op: sql} 만 골라 컬럼 → 식. */
  private static Map<String, String> sqlExprs(List<ColumnRule> rules) {
    Map<String, String> out = JdbcSql.caseInsensitive(Map.of());
    for (ColumnRule r : rules) {
      if ("sql".equalsIgnoreCase(r.op())) {
        out.put(r.field(), r.expression());
      }
    }
    return out;
  }

  /** 두 대상이 같은 SQL 문장이 되는지 (키·식 비교는 대소문자 무시). */
  private static boolean sameStatement(Target a, Target b) {
    return upper(a.primaryKeys()).equals(upper(b.primaryKeys())) && a.sqlExprs().equals(b.sqlExprs());
  }

  private static List<String> upper(List<String> names) {
    return names.stream().map(n -> n.toUpperCase(Locale.ROOT)).toList();
  }

  /**
   * 대상마다 테이블 칸·PK를 DB에서 읽어 채우고, 설정과 맞는지 검사한다.
   *
   * 실패 조건: UPSERT 인데 키가 없음, 키나 {@code op: sql} 칸이 테이블에 없음.
   * 조용히 넘어가면 잘못된 SQL이 메시지마다 실패하므로 시작 때 막는다.
   */
  private static List<Target> loadTableInfo(
      JdbcEndpointRecord endpoint, IJdbcDialect dialect, List<Target> planned
  ) {
    if (planned.isEmpty()) {
      return List.of();
    }
    List<Target> out = new ArrayList<>(planned.size());
    try (Connection c = JdbcSql.open(endpoint, dialect.driver())) {
      for (Target t : planned) {
        List<Column> cols = JdbcSql.columns(c, t.table());
        List<String> pks =
            t.primaryKeys().isEmpty() ? JdbcSql.primaryKeys(c, t.table()) : t.primaryKeys();
        if (t.writeMode() == WriteMode.UPSERT && pks.isEmpty()) {
          throw new IllegalStateException(
              "UPSERT 키가 없습니다: " + t.table() + " (YAML primaryKeys 또는 테이블 PK 필요)");
        }
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        cols.forEach(col -> names.add(col.name()));
        List<String> needed = new ArrayList<>(t.sqlExprs().keySet());
        if (t.writeMode() == WriteMode.UPSERT) {
          needed.addAll(pks);
        }
        for (String need : needed) {
          if (!names.contains(need)) {
            throw new IllegalStateException("테이블 " + t.table() + " 에 컬럼 " + need + " 이 없습니다.");
          }
        }
        out.add(new Target(t.table(), t.writeMode(), pks, t.sqlExprs(), cols));
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("테이블 정보를 읽지 못했습니다: " + endpoint.jdbcUrl(), e);
    }
    return out;
  }

  /**
   * 레코드를 대상별 옆 출구로 보낸다. {@code sink: jdbc} 레코드는 본 출구(우리 Sink).
   *
   * 대상이 없는 공식 레코드는 계획(구독 토픽 resolve)과 실행이 어긋났다는 뜻이라 버리지 않고 실패시킨다.
   */
  static final class TargetRouter extends ProcessFunction<WriteRecord, WriteRecord> {

    private static final long serialVersionUID = 1L;

    private final Map<String, OutputTag<WriteRecord>> tags;
    private final boolean acceptCustomJdbc;

    TargetRouter(Map<String, OutputTag<WriteRecord>> tags, boolean acceptCustomJdbc) {
      this.tags = new HashMap<>(tags);
      this.acceptCustomJdbc = acceptCustomJdbc;
    }

    @Override
    public void processElement(WriteRecord r, Context ctx, Collector<WriteRecord> out) {
      if (r.destination().sink() == SinkMode.JDBC) {
        if (!acceptCustomJdbc) {
          throw new IllegalStateException("sink: jdbc 레코드인데 우리 JDBC Sink 가 없습니다: " + r.destination());
        }
        out.collect(r);
        return;
      }
      String key = keyOf(r.destination().target(), r.destination().writeMode());
      OutputTag<WriteRecord> tag = tags.get(key);
      if (tag == null) {
        throw new IllegalStateException("공식 Sink 대상이 그래프에 없습니다: " + key);
      }
      ctx.output(tag, r);
    }
  }

  /**
   * 공식 JdbcSink 가 행마다 부르는 값 채우기. 테이블 칸 순서대로, 형식을 지정해 넣는다.
   * 메시지에 없는 칸은 NULL (문장이 고정이라 칸을 빼서 DB DEFAULT 를 타게 할 수 없다).
   */
  static final class TargetStatementBuilder implements JdbcStatementBuilder<WriteRecord> {

    private static final long serialVersionUID = 1L;

    private final List<Column> bindColumns;

    TargetStatementBuilder(List<Column> bindColumns) {
      this.bindColumns = new ArrayList<>(bindColumns);
    }

    @Override
    public void accept(java.sql.PreparedStatement ps, WriteRecord r) throws java.sql.SQLException {
      JdbcSql.bindColumns(ps, bindColumns, JdbcSql.rowCaseInsensitive(r.row()));
    }
  }
}
