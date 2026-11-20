package com.smdp.flinkconnect.sink.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Postgres 방언: ON CONFLICT 문장, COPY 텍스트 칸. */
class PostgresSinkAdapterTest {

  private final PostgresSinkAdapter pg = new PostgresSinkAdapter();

  @Test
  void upsert_updatesNonKeyColumns() {
    String sql = pg.upsertSql("RD_LP", List.of("meter_id", "fap"), Map.of(), List.of("METER_ID"));
    assertEquals(
        "INSERT INTO RD_LP (meter_id, fap) VALUES (?, ?) ON CONFLICT (METER_ID) DO UPDATE SET fap = EXCLUDED.fap",
        sql);
  }

  /** 모든 칸이 키면 SET 이 비어 문법 오류가 나므로 DO NOTHING. */
  @Test
  void upsert_allKeyColumns_doNothing() {
    String sql = pg.upsertSql("T", List.of("a", "b"), Map.of(), List.of("A", "B"));
    assertEquals("INSERT INTO T (a, b) VALUES (?, ?) ON CONFLICT (A, B) DO NOTHING", sql);
  }

  /** NULL 은 \N, 탭·줄바꿈·역슬래시는 이스케이프해야 칸이 밀리지 않는다. */
  @Test
  void copyField_escapes() {
    assertEquals("\\N", PostgresSinkAdapter.copyField(null));
    assertEquals("a\\tb\\nc\\\\d", PostgresSinkAdapter.copyField("a\tb\nc\\d"));
  }
}
