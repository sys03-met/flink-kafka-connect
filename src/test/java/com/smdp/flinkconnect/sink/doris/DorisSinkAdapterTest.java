package com.smdp.flinkconnect.sink.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Doris 어댑터: 접속 정보 해석, Stream Load JSON 변환. (DB 없이) */
class DorisSinkAdapterTest {

  /** JDBC URL → DB명, streamLoadUrl(BE 8040) → FE 8030 / BE 8040. */
  @Test
  void parsesDatabaseAndNodes() {
    String streamLoad = "http://localhost:8040/api/rd/{table}/_stream_load";
    assertEquals("rd", DorisSinkAdapter.databaseName("jdbc:mysql://localhost:9030/rd"));
    assertEquals("localhost:8030", DorisSinkAdapter.feNodes(streamLoad));
    assertEquals("localhost:8040", DorisSinkAdapter.beNodes(streamLoad));
  }

  /** streamLoadUrl 이 없으면 127.0.0.1 기본. */
  @Test
  void defaultsToLocalhostWithoutStreamLoadUrl() {
    assertEquals("127.0.0.1:8030", DorisSinkAdapter.feNodes(null));
    assertEquals("127.0.0.1:8040", DorisSinkAdapter.beNodes(null));
  }

  /** {@code op: sql} 칸(DB 식)은 JSON 에 넣지 않는다. */
  @Test
  void jsonFieldsSkipSqlColumns() {
    Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("METER_ID", "M1");
    raw.put(WriteRowRecord.SQL_PREFIX + "INGEST_DT", "NOW()");

    assertEquals(Map.of("METER_ID", "M1"), DorisSinkAdapter.jsonFields(WriteRowRecord.of(raw)));
  }
}
