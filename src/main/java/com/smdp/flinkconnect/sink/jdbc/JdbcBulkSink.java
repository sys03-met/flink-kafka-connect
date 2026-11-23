package com.smdp.flinkconnect.sink.jdbc;

import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 공식 경로의 BULK 대상 하나(테이블 하나)를 대량 적재 통로로 넣는 Sink.
 *
 * 공식 JdbcSink 는 INSERT 문장 배치만 지원하고 Postgres COPY / MSSQL BulkCopy 가 없다.
 * 그래서 BULK 만 이 Sink 가 맡고, 실제 통로는 어댑터({@link IJdbcDialect#bulkLoad})가 DB별로 구현한다.
 *
 * 칸은 Job 시작 때 테이블에서 읽은 목록 고정. 메시지에 없는 칸은 NULL.
 * 체크포인트 때와 종료 때 남은 행을 내보낸다 (안 그러면 마지막 묶음이 DB에 안 간다).
 */
public final class JdbcBulkSink extends RichSinkFunction<WriteRecord> implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;

  private final JdbcEndpointRecord endpoint;
  private final IJdbcDialect dialect;
  private final int batchSize;
  private final String table;
  /** Job 시작 때 테이블에서 읽은 넣을 칸 (순서 = 행 배열 순서). */
  private final List<JdbcSql.Column> columns;

  private transient Connection connection;
  private transient List<Object[]> rows;

  public JdbcBulkSink(
      JdbcEndpointRecord endpoint,
      IJdbcDialect dialect,
      int batchSize,
      String table,
      List<JdbcSql.Column> columns
  ) {
    this.endpoint = endpoint;
    this.dialect = dialect;
    this.batchSize = batchSize;
    this.table = table;
    this.columns = new ArrayList<>(columns);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    connection = JdbcSql.open(endpoint, dialect.driver());
    rows = new ArrayList<>();
  }

  /** 행을 테이블 칸 순서 배열로 바꿔 쌓고, batchSize 가 되면 내보낸다. */
  @Override
  public void invoke(WriteRecord value, SinkFunction.Context context) throws Exception {
    // 대량 통로는 값만 보낸다. NOW() 같은 식을 넣을 자리가 없어 NULL 로 들어가므로 막는다
    if (value.row().hasSql()) {
      throw new IllegalStateException("BULK 에는 op: sql 컬럼을 쓸 수 없습니다: " + table);
    }
    Map<String, Object> rowCi = JdbcSql.rowCaseInsensitive(value.row());
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = JdbcSql.toJdbcValue(rowCi.get(columns.get(i).name()));
    }
    rows.add(values);
    if (rows.size() >= batchSize) {
      flush();
    }
  }

  private void flush() throws Exception {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    dialect.bulkLoad(connection, table, columns, rows);
    rows.clear();
  }

  /** 체크포인트 직전: 버퍼를 비워 "체크포인트 완료 = DB 반영" 을 맞춘다. */
  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    flush();
  }

  /** 버퍼를 상태로 저장하지 않는다 (snapshot 때 비우므로 복구할 것이 없다). */
  @Override
  public void initializeState(FunctionInitializationContext context) {}

  @Override
  public void close() throws Exception {
    try {
      flush();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
