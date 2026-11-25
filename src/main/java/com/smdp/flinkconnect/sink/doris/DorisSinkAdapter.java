package com.smdp.flinkconnect.sink.doris;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import com.smdp.flinkconnect.sink.ISinkAdapter;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Doris용 {@link ISinkAdapter}. Doris 에 관한 것은 전부 이 파일에 있다.
 *
 *   - Doris 행만 filter 후 공식 {@link DorisSink} (Stream Load) 하나 부착
 *   - 접속 정보에서 DB명·FE/BE 주소 해석
 *   - 행 → Stream Load JSON 변환
 *
 * 레코드마다 테이블명이 있어 Sink 하나로 다중 테이블 ({@code setTableIdentifier} 는 자리 채우기용).
 *
 * writeMode 는 Doris 에서 따로 갈래를 나누지 않는다. Stream Load 하나가 세 가지를 다 맡는다.
 *
 *   - INSERT / BULK: Stream Load 자체가 묶음 적재
 *   - UPSERT: 테이블이 Unique Key 모델이면 같은 키가 덮어써진다 (테이블 설계로 정함)
 *
 * {@code sink: jdbc} 와 {@code op: sql} 은 설정 읽기에서 이미 거절하므로 여기 오지 않는다.
 */
public final class DorisSinkAdapter implements ISinkAdapter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Stream Load는 값만 보낸다. JDBC 갈래와 SQL 식은 시작 전에 거절한다.
   * 로더가 이 메서드를 부르므로 Doris 분기를 로더에 두지 않는다.
   */
  @Override
  public void check(DestinationConfig dest, List<String> subscribe, boolean allowJdbc) {
    if (dest.sink() == SinkMode.JDBC) {
      throw new IllegalArgumentException("sink: jdbc 는 Doris에 쓸 수 없습니다 (공식 Stream Load만).");
    }
    if (dest.columns().stream().anyMatch(c -> c.op() != null && c.op().equalsIgnoreCase("sql"))) {
      throw new IllegalStateException("op: sql 컬럼은 Doris 에 쓸 수 없습니다.");
    }
  }

  /** {@link ISinkAdapter} — Doris 행 filter 후 공식 Stream Load Sink 하나 부착. config는 Kafka용이라 안 쓴다. */
  @Override
  public void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      PipelineConfig config
  ) {
    writes
        .filter((FilterFunction<WriteRecord>) r -> r.destination().system() == SystemType.DORIS)
        .name("to-doris")
        .sinkTo(create(endpoint, batchSize, batchIntervalMs))
        .name("doris-official-sink");
  }

  /**
   * Stream Load 배치 Sink.
   *
   * @param batch Job batchSize. 커넥터 하한 때문에 flush rows는 max(10000, batch)
   * @param intervalMs 건수가 안 차도 이 밀리초가 지나면 Stream Load로 보낸다
   */
  static DorisSink<WriteRecord> create(JdbcEndpointRecord endpoint, int batch, long intervalMs) {
    Properties streamLoad = new Properties();
    streamLoad.setProperty("format", "json");
    streamLoad.setProperty("read_json_by_line", "true");

    String database = databaseName(endpoint.jdbcUrl());
    DorisOptions options =
        DorisOptions.builder()
            .setFenodes(feNodes(endpoint.streamLoadUrl()))
            .setBenodes(beNodes(endpoint.streamLoadUrl()))
            .setJdbcUrl(endpoint.jdbcUrl())
            .setUsername(endpoint.user())
            .setPassword(endpoint.password() == null ? "" : endpoint.password())
            // 실제 라우팅은 Serializer의 DorisRecord.table
            .setTableIdentifier(database + ".RD_LP")
            .setAutoRedirect(false)
            .build();

    DorisExecutionOptions exec =
        DorisExecutionOptions.builder()
            .setLabelPrefix("flink-connect")
            .disable2PC()
            .setBatchMode(true)
            // 행 기준은 커넥터가 10000 미만을 받지 않는다. 시간만 YAML 간격을 따른다.
            .setBufferFlushMaxRows(Math.max(10_000, batch))
            .setBufferFlushIntervalMs(Math.toIntExact(intervalMs))
            .setStreamLoadProp(streamLoad)
            .build();

    return DorisSink.<WriteRecord>builder()
        .setDorisOptions(options)
        .setDorisExecutionOptions(exec)
        .setSerializer(new WriteRecordDorisSerializer(database))
        .build();
  }

  /**
   * Stream Load JSON 맵. {@code op:sql} 컬럼은 DB 식이라 넣지 않는다.
   */
  static Map<String, Object> jsonFields(WriteRowRecord row) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (String col : row.columns()) {
      if (!row.isSql(col)) {
        out.put(col, row.value(col));
      }
    }
    return out;
  }

  /**
   * JDBC URL 경로의 DB명. {@code jdbc:mysql://host:9030/rd} → {@code rd}.
   * Doris tableIdentifier·DorisRecord DB에 쓴다.
   */
  static String databaseName(String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      return "";
    }
    try {
      // sqlserver 식 ;databaseName=ami 표기도 받아 준다
      int dbNameIdx = jdbcUrl.toLowerCase(Locale.ROOT).indexOf("databasename=");
      if (dbNameIdx >= 0) {
        String rest = jdbcUrl.substring(dbNameIdx + "databasename=".length());
        int end = rest.indexOf(';');
        return end < 0 ? rest : rest.substring(0, end);
      }
      String path = URI.create(jdbcUrl.replaceFirst("^jdbc:[a-z]+://", "http://")).getPath();
      if (path == null || path.isBlank() || "/".equals(path)) {
        return "";
      }
      String db = path.substring(1);
      int slash = db.indexOf('/');
      return slash < 0 ? db : db.substring(0, slash);
    } catch (Exception e) {
      return "";
    }
  }

  /** Doris FE HTTP {@code host:8030}. streamLoadUrl 호스트 기준, 8040(BE)이면 8030으로 보정. */
  static String feNodes(String streamLoadUrl) {
    URI uri = httpUri(streamLoadUrl);
    if (uri == null) {
      return "127.0.0.1:8030";
    }
    int port = uri.getPort() > 0 ? uri.getPort() : 8030;
    return uri.getHost() + ":" + (port == 8040 ? 8030 : port);
  }

  /**
   * Doris BE HTTP {@code host:8040}.
   * Docker Desktop에선 FE 307 리다이렉트가 깨져 BE로 직행한다.
   */
  static String beNodes(String streamLoadUrl) {
    URI uri = httpUri(streamLoadUrl);
    if (uri == null) {
      return "127.0.0.1:8040";
    }
    return uri.getHost() + ":" + (uri.getPort() > 0 ? uri.getPort() : 8030);
  }

  /** {@code {table}} 자리를 채워 URI로. 호스트가 없거나 깨졌으면 null. */
  private static URI httpUri(String url) {
    if (url == null || url.isBlank()) {
      return null;
    }
    try {
      URI uri = URI.create(url.replace("{table}", "t"));
      return uri.getHost() == null ? null : uri;
    } catch (Exception e) {
      return null;
    }
  }

  /** WriteRecord → Doris JSON 라인 + 테이블명. 테이블이 레코드에 있어 Sink 하나로 다중 테이블. */
  static final class WriteRecordDorisSerializer implements DorisRecordSerializer<WriteRecord>, Serializable {
    private static final long serialVersionUID = 1L;
    /** JDBC URL에서 뽑은 DB명. DorisRecord 첫 인자. */
    private final String database;

    WriteRecordDorisSerializer(String database) {
      this.database = database;
    }

    /** Doris 커넥터가 Stream Load 직전에 호출. */
    @Override
    public DorisRecord serialize(WriteRecord req) throws IOException {
      return DorisRecord.of(
          database, req.destination().target(), MAPPER.writeValueAsBytes(jsonFields(req.row())));
    }
  }
}
