package com.smdp.flinkconnect.sink.mssql;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.sink.ISinkAdapter;
import com.smdp.flinkconnect.sink.jdbc.IJdbcDialect;
import com.smdp.flinkconnect.sink.jdbc.JdbcAttacher;
import com.smdp.flinkconnect.sink.jdbc.JdbcSql.Column;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * MSSQL용 {@link ISinkAdapter} + {@link IJdbcDialect}.
 *
 * 연결
 * {@link SystemType#MSSQL} → {@code newSink()} 가 이 클래스를 만든다.
 *
 * 하는 일
 *
 *   - 배선은 {@link JdbcAttacher} 에 맡긴다 (공식 JdbcSink 갈래 + {@code sink: dynamic} 갈래)
 *   - UPSERT 문장: {@code MERGE ... WITH (HOLDLOCK)} (MSSQL에는 ON CONFLICT 가 없다)
 *   - BULK: {@link SQLServerBulkCopy} (MSSQL 전용 고속 적재 통로, Postgres COPY 대응)
 */
public final class MssqlSinkAdapter implements ISinkAdapter, IJdbcDialect {

  private static final long serialVersionUID = 1L;

  private static final String DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  /** {@link ISinkAdapter} — MSSQL 갈래를 그래프에 붙인다. 본문은 JdbcAttacher. config는 Kafka용이라 안 쓴다. */
  @Override
  public void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      PipelineConfig config
  ) {
    JdbcAttacher.attach(
        writes, SystemType.MSSQL, endpoint, batchSize, batchIntervalMs, planned, this);
  }

  @Override
  public String driver() {
    return DRIVER;
  }

  /**
   * {@code MERGE INTO 표 WITH (HOLDLOCK) AS tgt USING (SELECT ? AS 칸, ...) AS src ON 키 같음
   * WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...;}
   *
   * HOLDLOCK 이 없으면 두 작업이 동시에 "없음"을 보고 둘 다 INSERT 해서 키 중복 오류가 난다.
   * 모든 칸이 키면 고칠 칸이 없으므로 WHEN MATCHED 를 뺀다. MERGE 는 끝에 ; 가 필수.
   */
  @Override
  public String upsertSql(
      String table, List<String> columns, Map<String, String> sqlExprs, List<String> primaryKeys
  ) {
    Set<String> keys =
        primaryKeys.stream().map(k -> k.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    String select =
        columns.stream()
            .map(c -> (sqlExprs.containsKey(c) ? sqlExprs.get(c) : "?") + " AS " + c)
            .collect(Collectors.joining(", "));
    String on =
        primaryKeys.stream().map(k -> "tgt." + k + " = src." + k).collect(Collectors.joining(" AND "));
    String updates =
        columns.stream()
            .filter(c -> !keys.contains(c.toLowerCase(Locale.ROOT)))
            .map(c -> "tgt." + c + " = src." + c)
            .collect(Collectors.joining(", "));
    StringBuilder sb = new StringBuilder("MERGE INTO ").append(table);
    sb.append(" WITH (HOLDLOCK) AS tgt USING (SELECT ").append(select).append(") AS src ON ");
    sb.append(on);
    if (!updates.isEmpty()) {
      sb.append(" WHEN MATCHED THEN UPDATE SET ").append(updates);
    }
    sb.append(" WHEN NOT MATCHED THEN INSERT (").append(String.join(", ", columns));
    sb.append(") VALUES (");
    sb.append(columns.stream().map(c -> "src." + c).collect(Collectors.joining(", ")));
    sb.append(");");
    return sb.toString();
  }

  /**
   * SQLServerBulkCopy 로 행 묶음을 한 번에 보낸다.
   * 칸은 이름으로 짝지어(addColumnMapping) 테이블 칸 순서와 달라도 된다.
   */
  @Override
  public void bulkLoad(
      Connection c, String table, List<Column> columns, List<Object[]> rows
  ) throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    try (SQLServerBulkCopy bulk = new SQLServerBulkCopy(c.unwrap(SQLServerConnection.class))) {
      SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
      options.setBatchSize(rows.size());
      bulk.setBulkCopyOptions(options);
      bulk.setDestinationTableName(table);
      for (int i = 0; i < columns.size(); i++) {
        bulk.addColumnMapping(i + 1, columns.get(i).name());
      }
      bulk.writeToServer(new RowsBulkData(columns, rows));
    }
  }

  /**
   * BulkCopy 는 칸 형식대로 정확한 Java 형을 요구한다 (예: BIGINT 칸에 Integer 를 주면 실패).
   * 메시지 JSON 은 숫자를 Integer/Long/Double 로 섞어 주므로 칸 형식에 맞춰 바꾼다.
   * 바꿀 수 없는 값은 그대로 두고 드라이버 오류로 드러나게 한다.
   */
  static Object coerce(Object v, int jdbcType) {
    if (v == null) {
      return null;
    }
    switch (jdbcType) {
      case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
          Types.LONGNVARCHAR -> {
        return String.valueOf(v);
      }
      case Types.BIGINT -> {
        return number(v).longValue();
      }
      case Types.INTEGER -> {
        return number(v).intValue();
      }
      case Types.SMALLINT, Types.TINYINT -> {
        return number(v).shortValue();
      }
      case Types.DOUBLE, Types.FLOAT -> {
        return number(v).doubleValue();
      }
      case Types.REAL -> {
        return number(v).floatValue();
      }
      case Types.DECIMAL, Types.NUMERIC -> {
        return v instanceof BigDecimal b ? b : new BigDecimal(String.valueOf(v));
      }
      case Types.BIT, Types.BOOLEAN -> {
        return v instanceof Boolean b ? b : Boolean.valueOf(String.valueOf(v));
      }
      case Types.TIMESTAMP -> {
        return v instanceof String s ? Timestamp.valueOf(s) : v;
      }
      case Types.DATE -> {
        return v instanceof String s ? Date.valueOf(s) : v;
      }
      case Types.TIME -> {
        return v instanceof String s ? Time.valueOf(s) : v;
      }
      default -> {
        return v;
      }
    }
  }

  /** 숫자 또는 숫자 문자열 → Number. */
  private static Number number(Object v) {
    return v instanceof Number n ? n : new BigDecimal(String.valueOf(v));
  }

  /** 메모리의 행 묶음을 BulkCopy 가 읽는 모양으로 감싼다. 1번 칸부터 센다. */
  private static final class RowsBulkData implements ISQLServerBulkData {

    private static final long serialVersionUID = 1L;

    private final List<Column> columns;
    private final List<Object[]> rows;
    private int cursor = -1;

    RowsBulkData(List<Column> columns, List<Object[]> rows) {
      this.columns = columns;
      this.rows = rows;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
      Set<Integer> out = new LinkedHashSet<>();
      for (int i = 1; i <= columns.size(); i++) {
        out.add(i);
      }
      return out;
    }

    @Override
    public String getColumnName(int column) {
      return columns.get(column - 1).name();
    }

    @Override
    public int getColumnType(int column) {
      return columns.get(column - 1).jdbcType();
    }

    @Override
    public int getPrecision(int column) {
      return columns.get(column - 1).precision();
    }

    @Override
    public int getScale(int column) {
      return columns.get(column - 1).scale();
    }

    /** 현재 행을 칸 형식에 맞춘 값으로. */
    @Override
    public Object[] getRowData() {
      Object[] src = rows.get(cursor);
      Object[] out = new Object[src.length];
      for (int i = 0; i < src.length; i++) {
        out[i] = coerce(src[i], columns.get(i).jdbcType());
      }
      return out;
    }

    @Override
    public boolean next() {
      cursor++;
      return cursor < rows.size();
    }
  }
}
