package com.smdp.flinkconnect.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.DbType;
import com.smdp.flinkconnect.config.PipelineModels.DefaultsConfig;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.TopicConfig;
import com.smdp.flinkconnect.config.PipelineModels.TopicPatternConfig;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * DefaultTopicBindingResolver: 토픽 → 갈 곳(테이블명·패턴·다중 destination) + expand.
 *
 * 앞쪽은 손으로 만든 설정, 뒤쪽은 실제 YAML({@code config/example-pipeline.yaml},
 * {@code config/multi-topic-e2e.yaml})로 대표 토픽이 어디로 가는지 본다.
 */
public class TopicBindingResolverTest {

  /** multi-topic-e2e.yaml 의 RD 토픽 (PG+Doris 로 감). 다른 테스트·E2E 도 같은 목록을 쓴다. */
  public static final List<String> RD_TOPICS =
      List.of(
          "hv.j.dproc.rd_lp",
          "hv.j.dproc.rd_mon",
          "hv.j.dproc.rd_day",
          "hv.j.dproc.rd_hour",
          "hv.j.dproc.rd_week");

  /** multi-topic-e2e.yaml 의 METER 토픽 (MSSQL 로 감). */
  public static final List<String> METER_TOPICS =
      List.of(
          "hv.j.dproc.meter_curr_limit",
          "hv.j.dproc.meter_status",
          "hv.j.dproc.meter_alarm",
          "hv.j.dproc.meter_power",
          "hv.j.dproc.meter_event");

  /** defaults 만 있는 설정 (테이블명은 토픽에서 끌어냄). */
  private static PipelineConfig defaultsOnly(DestinationConfig... dests) {
    return new PipelineConfig(
        null, new DefaultsConfig(List.of(dests), "hv\\.j\\.dproc\\.(.+)"), List.of(), Map.of());
  }

  @Test
  void derivesTableFromTopicWhenNotConfigured() {
    PipelineConfig config =
        defaultsOnly(new DestinationConfig(DbType.POSTGRES, null, WriteMode.UPSERT, List.of()));

    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp");

    assertEquals(1, destinations.size());
    assertEquals("RD_LP", destinations.getFirst().table());
    assertEquals(DbType.POSTGRES, destinations.getFirst().db());
    assertEquals(WriteMode.UPSERT, destinations.getFirst().writeMode());
  }

  @Test
  void exactTopicOverridesPatternAndDefaults() {
    PipelineConfig config =
        new PipelineConfig(
            null,
            new DefaultsConfig(
                List.of(new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of())),
                "hv\\.j\\.dproc\\.(.+)"),
            List.of(
                new TopicPatternConfig(
                    "hv.j.dproc.rd_*",
                    null,
                    List.of(
                        new DestinationConfig(DbType.POSTGRES, null, WriteMode.BULK, List.of()),
                        new DestinationConfig(DbType.DORIS, null, WriteMode.BULK, List.of())))),
            Map.of(
                "hv.j.dproc.rd_lp",
                new TopicConfig(
                    "RD_LP",
                    List.of(
                        new DestinationConfig(
                            DbType.POSTGRES,
                            "RD_LP",
                            WriteMode.UPSERT,
                            List.of(
                                new ColumnRule(
                                    "SERVER_DT", "rename", "server_dt", null, null, null, null))),
                        new DestinationConfig(
                            DbType.MSSQL, "RD_LP_SHADOW", WriteMode.INSERT, List.of())))));

    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp");

    assertEquals(2, destinations.size());
    assertEquals(DbType.POSTGRES, destinations.get(0).db());
    assertEquals(WriteMode.UPSERT, destinations.get(0).writeMode());
    assertEquals("RD_LP", destinations.get(0).table());
    assertEquals(1, destinations.get(0).columns().size());
    assertEquals(DbType.MSSQL, destinations.get(1).db());
    assertEquals("RD_LP_SHADOW", destinations.get(1).table());
  }

  @Test
  void patternMatchAppliesWhenNoExactTopic() {
    PipelineConfig config =
        new PipelineConfig(
            null,
            new DefaultsConfig(
                List.of(new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of())),
                "hv\\.j\\.dproc\\.(.+)"),
            List.of(
                new TopicPatternConfig(
                    "hv.j.dproc.meter_*",
                    null,
                    List.of(
                        new DestinationConfig(DbType.MSSQL, null, WriteMode.UPSERT, List.of())))),
            Map.of());

    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.meter_curr_limit");

    assertEquals(1, destinations.size());
    assertEquals(DbType.MSSQL, destinations.getFirst().db());
    assertEquals("METER_CURR_LIMIT", destinations.getFirst().table());
    assertEquals(WriteMode.UPSERT, destinations.getFirst().writeMode());
  }

  @Test
  void laterMatchingPatternWinsOverEarlier() {
    PipelineConfig config =
        new PipelineConfig(
            null,
            new DefaultsConfig(
                List.of(new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of())),
                "hv\\.j\\.dproc\\.(.+)"),
            List.of(
                new TopicPatternConfig(
                    "hv.j.dproc.*",
                    null,
                    List.of(
                        new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of()))),
                new TopicPatternConfig(
                    "hv.j.dproc.rd_*",
                    null,
                    List.of(
                        new DestinationConfig(DbType.DORIS, null, WriteMode.BULK, List.of())))),
            Map.of());

    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_mon");

    assertEquals(DbType.DORIS, destinations.getFirst().db());
    assertEquals(WriteMode.BULK, destinations.getFirst().writeMode());
    assertEquals("RD_MON", destinations.getFirst().table());
  }

  /** 공식 경로가 기본. 테이블명을 토픽에서 끌어내도 된다 (Job이 구독 토픽을 미리 resolve). */
  @Test
  void officialSink_isDefault_andAllowsDerivedTable() {
    PipelineConfig config =
        defaultsOnly(new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of()));

    DestinationConfig d = new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp").getFirst();
    assertEquals(SinkMode.OFFICIAL, d.sink());
    assertEquals("RD_LP", d.table());
  }

  /** 대량 통로·Doris 는 값만 보내서 NOW() 같은 식을 실을 수 없다 → 시작 때 거절. */
  @Test
  void sqlRule_withBulkOrDoris_fails() {
    ColumnRule now = new ColumnRule("INGEST_DT", "sql", null, null, null, null, "NOW()");
    for (DestinationConfig dest :
        List.of(
            new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.BULK, List.of(now)),
            new DestinationConfig(DbType.DORIS, "RD_LP", WriteMode.INSERT, List.of(now)))) {
      PipelineConfig config = defaultsOnly(dest);
      assertThrows(
          IllegalStateException.class,
          () -> new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp"));
    }
  }

  @Test
  void officialSink_withExplicitTable_ok() {
    PipelineConfig config =
        new PipelineConfig(
            null,
            new DefaultsConfig(List.of(), "hv\\.j\\.dproc\\.(.+)"),
            List.of(),
            Map.of(
                "hv.j.dproc.rd_lp",
                new TopicConfig(
                    "RD_LP",
                    List.of(
                        new DestinationConfig(
                            DbType.POSTGRES,
                            "RD_LP",
                            WriteMode.UPSERT,
                            List.of(),
                            List.of("METER_ID"),
                            SinkMode.OFFICIAL)))));

    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp");
    assertEquals(SinkMode.OFFICIAL, destinations.getFirst().sink());
    assertEquals("RD_LP", destinations.getFirst().table());
  }

  @Test
  void resolveCachesBindingsPerTopic() {
    DefaultTopicBindingResolver resolver =
        new DefaultTopicBindingResolver(
            defaultsOnly(new DestinationConfig(DbType.POSTGRES, null, WriteMode.INSERT, List.of())));

    assertSame(resolver.resolve("hv.j.dproc.rd_lp"), resolver.resolve("hv.j.dproc.rd_lp"));
  }

  /** expand: 갈 곳마다 WriteRecord 하나 (같은 메시지 → PG + Doris). */
  @Test
  void expandCreatesOneWriteRecordPerDestination() {
    PipelineConfig config =
        defaultsOnly(
            new DestinationConfig(DbType.POSTGRES, "RD_LP", WriteMode.UPSERT, List.of()),
            new DestinationConfig(DbType.DORIS, "RD_LP", WriteMode.BULK, List.of()));
    Map<String, Object> payload = new HashMap<>();
    payload.put("METER_ID", "M1");

    List<WriteRecord> out =
        new DefaultTopicBindingResolver(config).expand(new TopicRecord("hv.j.dproc.rd_lp", payload));

    assertEquals(2, out.size());
    assertEquals(DbType.POSTGRES, out.get(0).destination().db());
    assertEquals(DbType.DORIS, out.get(1).destination().db());
  }

  /** example-pipeline.yaml: rd_lp → PG UPSERT + Doris BULK, meter_curr_limit → MSSQL UPSERT. */
  @Test
  void examplePipeline_representativeTopics() throws Exception {
    DefaultTopicBindingResolver resolver =
        new DefaultTopicBindingResolver(
            new YamlPipelineConfigLoader().load(Path.of("config/example-pipeline.yaml")));

    List<DestinationConfig> rdLp = resolver.resolve("hv.j.dproc.rd_lp");
    assertEquals(2, rdLp.size());
    assertEquals(DbType.POSTGRES, rdLp.get(0).db());
    assertEquals("RD_LP", rdLp.get(0).table());
    assertEquals(WriteMode.UPSERT, rdLp.get(0).writeMode());
    assertEquals(DbType.DORIS, rdLp.get(1).db());
    assertEquals("RD_LP", rdLp.get(1).table());
    assertEquals(WriteMode.BULK, rdLp.get(1).writeMode());

    List<DestinationConfig> meter = resolver.resolve("hv.j.dproc.meter_curr_limit");
    assertEquals(1, meter.size());
    assertEquals(DbType.MSSQL, meter.get(0).db());
    assertEquals("METER_CURR_LIMIT", meter.get(0).table());
    assertEquals(WriteMode.UPSERT, meter.get(0).writeMode());
  }

  /** multi-topic-e2e.yaml: 토픽 10개가 패턴대로 PG+Doris / MSSQL 로 갈라진다. */
  @Test
  void multiTopicPipeline_tenTopicsRouteToExpectedDatabases() throws Exception {
    DefaultTopicBindingResolver resolver =
        new DefaultTopicBindingResolver(
            new YamlPipelineConfigLoader().load(Path.of("config/multi-topic-e2e.yaml")));

    for (String topic : RD_TOPICS) {
      List<DestinationConfig> destinations = resolver.resolve(topic);
      assertEquals(2, destinations.size(), topic);
      assertEquals(DbType.POSTGRES, destinations.get(0).db(), topic);
      assertEquals(DbType.DORIS, destinations.get(1).db(), topic);
      assertTrue(destinations.get(0).table().startsWith("RD_"), topic);
    }
    for (String topic : METER_TOPICS) {
      List<DestinationConfig> destinations = resolver.resolve(topic);
      assertEquals(1, destinations.size(), topic);
      assertEquals(DbType.MSSQL, destinations.get(0).db(), topic);
      assertTrue(destinations.get(0).table().startsWith("METER_"), topic);
    }
  }
}
