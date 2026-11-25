package com.smdp.flinkconnect.sink.jdbc;

import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import com.smdp.flinkconnect.sink.jdbc.JdbcSql.Column;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * {@code sink: jdbc} 레코드를 넣는 우리 JDBC Sink.
 *
 * 누가 new 하나
 * {@link JdbcFamilySupport} — 이 DB에 {@code sink: jdbc} 설정이 있을 때만.
 *
 * 공식 JdbcSink 와 다른 점: SQL을 미리 정하지 않고 메시지가 가진 칸으로 그때그때 만든다.
 * 그래서 Sink 하나가 여러 테이블·칸 구성을 받고, 메시지에 없는 칸은 SQL에서 빠져 DB DEFAULT 를 탄다.
 *
 * WriteMode
 *
 *   - INSERT — PreparedStatement 배치
 *   - UPSERT — 어댑터 문장 (Postgres ON CONFLICT / MSSQL MERGE). 키는 YAML, 없으면 테이블 PK
 *   - BULK — 어댑터 대량 적재 (Postgres COPY / MSSQL BulkCopy). {@code op: sql} 은 거절
 */
public final class DynamicTableJdbcSink extends RichSinkFunction<WriteRecord>
    implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;

  private final JdbcEndpointRecord endpoint;
  /** DB마다 다른 드라이버·UPSERT·대량 적재 (어댑터). */
  private final IJdbcDialect dialect;
  /** executeBatch / 대량 적재 flush 행 수. */
  private final int batchSize;

  private transient Connection connection;
  /** key=table|upsert|칸서명 → 재사용 PreparedStatement. */
  private transient Map<String, PreparedStatement> statements;
  /** statements 키별 현재 배치에 쌓인 행 수. */
  private transient Map<String, Integer> pending;
  /** BULK 버퍼. key=table|칸목록. */
  private transient Map<String, BulkBatch> bulkBatches;
  /** 테이블 → DB에서 읽은 칸·PK. 테이블당 한 번만 조회. */
  private transient Map<String, List<Column>> tableColumns;
  private transient Map<String, List<String>> tableKeys;

  public DynamicTableJdbcSink(JdbcEndpointRecord endpoint, IJdbcDialect dialect, int batchSize) {
    this.endpoint = endpoint;
    this.dialect = dialect;
    this.batchSize = batchSize;
  }

  /** Task 시작 시 한 번. 접속과 버퍼를 연다 (transient 필드는 직렬화로 안 넘어옴). */
  @Override
  public void open(OpenContext openContext) throws Exception {
    connection = JdbcSql.open(endpoint, dialect.driver());
    statements = new HashMap<>();
    pending = new HashMap<>();
    bulkBatches = new HashMap<>();
    tableColumns = new HashMap<>();
    tableKeys = new HashMap<>();
  }

  /** WriteRecord 한 건마다. WriteMode 보고 대량 버퍼 또는 문장 배치에 쌓는다. */
  @Override
  public void invoke(WriteRecord value, SinkFunction.Context context) throws Exception {
    WriteMode mode = value.destination().writeMode();
    if (mode == WriteMode.BULK) {
      appendBulk(value);
      return;
    }
    appendStatement(value, mode == WriteMode.UPSERT);
  }

  /** INSERT 또는 UPSERT 배치. */
  private void appendStatement(WriteRecord value, boolean upsert) throws Exception {
    String table = value.destination().target();
    WriteRowRecord row = value.row();
    // 칸 구성이 바뀌면 SQL도 달라지므로 서명으로 PreparedStatement 를 나눈다
    String stmtKey = table + "|" + upsert + "|" + String.join(",", row.columns());
    PreparedStatement ps = statements.get(stmtKey);
    if (ps == null) {
      String sql =
          upsert
              ? dialect.upsertSql(table, row.columns(), JdbcSql.sqlExpressions(row), keysOf(value))
              : JdbcSql.insertSql(table, row);
      ps = connection.prepareStatement(sql);
      statements.put(stmtKey, ps);
    }
    JdbcSql.bindPlaceholders(ps, row);
    ps.addBatch();
    int n = pending.merge(stmtKey, 1, Integer::sum);
    if (n >= batchSize) {
      ps.executeBatch();
      pending.put(stmtKey, 0);
    }
  }

  /** UPSERT 키: YAML primaryKeys, 비면 테이블 PK. 둘 다 없으면 어디를 갱신할지 몰라 실패. */
  private List<String> keysOf(WriteRecord value) throws Exception {
    List<String> yaml = value.destination().primaryKeys();
    if (!yaml.isEmpty()) {
      return yaml;
    }
    String table = value.destination().target();
    List<String> keys = tableKeys.get(table);
    if (keys == null) {
      keys = JdbcSql.primaryKeys(connection, table);
      tableKeys.put(table, keys);
    }
    if (keys.isEmpty()) {
      throw new IllegalStateException("UPSERT 키가 없습니다: " + table + " (YAML primaryKeys 또는 테이블 PK 필요)");
    }
    return keys;
  }

  /** 대량 버퍼에 행을 쌓고 batchSize 가 되면 어댑터 대량 적재로 내보낸다. */
  private void appendBulk(WriteRecord value) throws Exception {
    WriteRowRecord row = value.row();
    String table = value.destination().target();
    // 대량 통로는 값만 보낸다. NOW() 같은 식은 넣을 자리가 없어 막는다
    if (row.hasSql()) {
      throw new IllegalStateException("BULK 에는 op: sql 컬럼을 쓸 수 없습니다: " + table);
    }
    List<String> rowCols = row.columns();
    String key = table + "|" + String.join(",", rowCols);
    BulkBatch batch = bulkBatches.get(key);
    if (batch == null) {
      // BulkCopy 는 칸 형식이 필요해서 테이블 칸 정보에서 메시지 칸만 골라 둔다
      List<Column> all = tableColumns.get(table);
      if (all == null) {
        all = JdbcSql.columns(connection, table);
        tableColumns.put(table, all);
      }
      batch = new BulkBatch(table, JdbcSql.pick(table, all, rowCols));
      bulkBatches.put(key, batch);
    }
    Object[] values = new Object[rowCols.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = JdbcSql.toJdbcValue(row.value(rowCols.get(i)));
    }
    batch.rows.add(values);
    if (batch.rows.size() >= batchSize) {
      flushBulk(batch);
    }
  }

  private void flushBulk(BulkBatch batch) throws Exception {
    if (batch.rows.isEmpty()) {
      return;
    }
    dialect.bulkLoad(connection, batch.table, batch.columns, batch.rows);
    batch.rows.clear();
  }

  /** 쌓인 대량 버퍼·문장 배치를 전부 DB로. */
  private void flushAll() throws Exception {
    if (bulkBatches != null) {
      for (BulkBatch batch : bulkBatches.values()) {
        flushBulk(batch);
      }
    }
    if (statements != null) {
      for (Map.Entry<String, PreparedStatement> e : statements.entrySet()) {
        if (pending.getOrDefault(e.getKey(), 0) > 0) {
          e.getValue().executeBatch();
          pending.put(e.getKey(), 0);
        }
      }
    }
  }

  /** 체크포인트 직전: 버퍼를 비워 "체크포인트 완료 = DB 반영" 을 맞춘다. */
  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    flushAll();
  }

  /** 버퍼를 상태로 저장하지 않는다 (snapshot 때 비우므로 복구할 것이 없다). */
  @Override
  public void initializeState(FunctionInitializationContext context) {}

  /** Task 종료·취소 시. 남은 행을 내보내고 접속을 닫는다. 안 하면 마지막 행이 DB에 안 간다. */
  @Override
  public void close() throws Exception {
    try {
      flushAll();
    } finally {
      if (statements != null) {
        for (PreparedStatement ps : statements.values()) {
          ps.close();
        }
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  /** 한 테이블·칸 구성의 대량 대기 행. */
  private static final class BulkBatch {
    final String table;
    final List<Column> columns;
    final List<Object[]> rows = new ArrayList<>();

    BulkBatch(String table, List<Column> columns) {
      this.table = table;
      this.columns = columns;
    }
  }
}
