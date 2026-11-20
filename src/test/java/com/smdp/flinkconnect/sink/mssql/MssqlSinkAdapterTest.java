package com.smdp.flinkconnect.sink.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.smdp.flinkconnect.sink.jdbc.JdbcSql;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** MSSQL 방언: MERGE 문장, BulkCopy 값 형 맞춤. */
class MssqlSinkAdapterTest {

  private final MssqlSinkAdapter mssql = new MssqlSinkAdapter();

  @Test
  void upsert_isMergeWithHoldlock() {
    String sql =
        mssql.upsertSql(
            "RD_LP",
            List.of("METER_ID", "FAP", "INGEST_DT"),
            JdbcSql.caseInsensitive(Map.of("ingest_dt", "SYSDATETIME()")),
            List.of("METER_ID"));
    assertEquals(
        "MERGE INTO RD_LP WITH (HOLDLOCK) AS tgt USING (SELECT ? AS METER_ID, ? AS FAP,"
            + " SYSDATETIME() AS INGEST_DT) AS src ON tgt.METER_ID = src.METER_ID"
            + " WHEN MATCHED THEN UPDATE SET tgt.FAP = src.FAP, tgt.INGEST_DT = src.INGEST_DT"
            + " WHEN NOT MATCHED THEN INSERT (METER_ID, FAP, INGEST_DT)"
            + " VALUES (src.METER_ID, src.FAP, src.INGEST_DT);",
        sql);
  }

  /** 모든 칸이 키면 고칠 칸이 없어 WHEN MATCHED 를 뺀다. */
  @Test
  void upsert_allKeyColumns_insertOnly() {
    String sql = mssql.upsertSql("T", List.of("A"), Map.of(), List.of("A"));
    assertFalse(sql.contains("WHEN MATCHED"));
  }

  /** JSON 숫자(Integer/Double/문자열)를 칸 형식이 요구하는 Java 형으로. */
  @Test
  void coerce_matchesColumnType() {
    assertEquals(5L, MssqlSinkAdapter.coerce(5, Types.BIGINT));
    assertEquals(1.0d, MssqlSinkAdapter.coerce(1, Types.DOUBLE));
    assertEquals(new BigDecimal("1.50"), MssqlSinkAdapter.coerce("1.50", Types.DECIMAL));
    assertEquals("7", MssqlSinkAdapter.coerce(7, Types.VARCHAR));
    assertEquals(
        Timestamp.valueOf("2026-11-17 15:00:00"),
        MssqlSinkAdapter.coerce("2026-11-17 15:00:00", Types.TIMESTAMP));
  }
}
