package com.smdp.flinkconnect.sink.postgres;

import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.sink.ISinkAdapter;
import com.smdp.flinkconnect.sink.jdbc.IJdbcDialect;
import com.smdp.flinkconnect.sink.jdbc.JdbcFamilySupport;
import com.smdp.flinkconnect.sink.jdbc.JdbcSql;
import com.smdp.flinkconnect.sink.jdbc.JdbcSql.Column;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.postgresql.PGConnection;

/**
 * Postgres용 {@link ISinkAdapter} + {@link IJdbcDialect}.
 *
 * 연결
 * {@link SystemType#POSTGRES} → {@code newSink()} 가 이 클래스를 만든다.
 *
 * 하는 일
 *
 *   - 배선은 {@link JdbcFamilySupport} 에 맡긴다 (공식 JdbcSink 갈래 + {@code sink: jdbc} 갈래)
 *   - UPSERT 문장: {@code INSERT ... ON CONFLICT (키) DO UPDATE}
 *   - BULK: {@code COPY ... FROM STDIN} (Postgres 전용 고속 적재 통로)
 */
public final class PostgresSinkAdapter implements ISinkAdapter, IJdbcDialect {

  private static final long serialVersionUID = 1L;

  /** 공식 Sink 연결 옵션·우리 Sink open 시 쓰는 드라이버. */
  private static final String DRIVER = "org.postgresql.Driver";

  /** {@link ISinkAdapter} — Postgres 갈래를 그래프에 붙인다. 본문은 JdbcFamilySupport. config는 Kafka용이라 안 쓴다. */
  @Override
  public void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      List<DestinationConfig> planned,
      PipelineConfig config
  ) {
    JdbcFamilySupport.attach(writes, SystemType.POSTGRES, endpoint, batchSize, planned, this);
  }

  @Override
  public String driver() {
    return DRIVER;
  }

  /**
   * {@code INSERT ... ON CONFLICT (키) DO UPDATE SET 칸 = EXCLUDED.칸}.
   * 모든 칸이 키면 고칠 칸이 없으므로 {@code DO NOTHING} (DO UPDATE SET 이 비면 문법 오류).
   */
  @Override
  public String upsertSql(
      String table, List<String> columns, Map<String, String> sqlExprs, List<String> primaryKeys
  ) {
    Set<String> keys =
        primaryKeys.stream().map(k -> k.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    String updates =
        columns.stream()
            .filter(c -> !keys.contains(c.toLowerCase(Locale.ROOT)))
            .map(c -> c + " = EXCLUDED." + c)
            .collect(Collectors.joining(", "));
    String action = updates.isEmpty() ? "DO NOTHING" : "DO UPDATE SET " + updates;
    return JdbcSql.insertSql(table, columns, sqlExprs)
        + " ON CONFLICT ("
        + String.join(", ", primaryKeys)
        + ") "
        + action;
  }

  /**
   * COPY 텍스트 형식으로 한 번에 보낸다. 칸 구분 탭, 줄 구분 \n, NULL 은 {@code \N}.
   * 값 안의 \ 탭 줄바꿈은 COPY 규칙대로 이스케이프해야 칸이 밀리지 않는다.
   */
  @Override
  public void bulkLoad(
      Connection c, String table, List<Column> columns, List<Object[]> rows
  ) throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    String colList = columns.stream().map(Column::name).collect(Collectors.joining(", "));
    String sql = "COPY " + table + " (" + colList + ") FROM STDIN WITH (FORMAT text, NULL '\\N')";
    StringBuilder payload = new StringBuilder();
    for (Object[] row : rows) {
      for (int i = 0; i < row.length; i++) {
        if (i > 0) {
          payload.append('\t');
        }
        payload.append(copyField(row[i]));
      }
      payload.append('\n');
    }
    try {
      c.unwrap(PGConnection.class).getCopyAPI().copyIn(sql, new StringReader(payload.toString()));
    } catch (java.io.IOException e) {
      throw new SQLException("COPY 실패: " + table, e);
    }
  }

  /** COPY 텍스트 한 칸. */
  static String copyField(Object v) {
    if (v == null) {
      return "\\N";
    }
    return String.valueOf(v)
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r");
  }
}
