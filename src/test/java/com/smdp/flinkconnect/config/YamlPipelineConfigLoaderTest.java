package com.smdp.flinkconnect.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.KafkaStartingOffset;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.transform.DefaultTopicBindingResolver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** YAML 로더 + Resolver 통합 스모크, sink 선택 규칙, YAML·환경변수 병합(resolve). */
class YamlPipelineConfigLoaderTest {

  @TempDir Path dir;

  /** 예전 YAML은 이름과 DLQ 동작이 그대로이고, 두 저장 방식은 명시적으로 선택한다. */
  @Test
  void jobNameAndDlqAreOptional() throws Exception {
    PipelineConfig defaults = load("{}");
    assertEquals("flink-connect", defaults.jobName());
    assertEquals(KafkaStartingOffset.COMMITTED, defaults.kafka().startingOffset());
    assertNull(defaults.dlq());

    PipelineConfig kafka = load("""
        jobName: rd-heavy
        kafka: {subscribe: ['hv.*']}
        dlq: {type: kafka, topic: dlq.rd-heavy}
        """);
    assertEquals("rd-heavy", kafka.jobName());
    assertEquals("dlq.rd-heavy", kafka.dlq().topic());
    assertNull(load("dlq: {type: kafka}").dlq().topic());

    assertEquals(".error", load("dlq: {type: file}").dlq().directory());
    var file = load("dlq: {type: file, directory: /data/errors, fileName: rejected}").dlq();
    assertEquals("/data/errors", file.directory());
    assertEquals("rejected", file.fileName());
  }

  /** 잘못된 이름과 경로 이탈을 막고, 명시한 DLQ와 입력 glob의 중복을 거절한다. */
  @Test
  void dlqRejectsInvalidSettingsAndSubscribeOverlap() {
    assertThrows(IllegalArgumentException.class, () -> load("dlq: {type: ftp}"));
    assertThrows(IllegalArgumentException.class, () -> load("dlq: {type: kafka, topic: ''}"));
    assertThrows(IllegalArgumentException.class, () -> load("dlq: {type: file, fileName: ../bad}"));
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {subscribe: ['hv.*']}
        dlq: {type: kafka, topic: hv.errors}
        """));
  }

  @Test
  void loadsExampleYamlAndResolvesFanOut() throws Exception {
    Path yaml = Path.of("config/example-pipeline.yaml");
    PipelineConfig config = new YamlPipelineConfigLoader().load(yaml);
    List<DestinationConfig> destinations =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp");

    assertEquals(2, destinations.size());
    assertEquals(SystemType.POSTGRES, destinations.get(0).system());
    assertEquals(WriteMode.UPSERT, destinations.get(0).writeMode());
    assertEquals(SinkMode.OFFICIAL, destinations.get(0).sink());
    assertEquals("RD_LP", destinations.get(0).target());
    assertEquals(List.of("METER_ID"), destinations.get(0).primaryKeys());
    assertEquals(SystemType.DORIS, destinations.get(1).system());
  }

  /** 시작 오프셋은 생략하면 그룹 기록, 오타는 처음부터 읽히지 않게 거절한다. */
  @Test
  void startingOffsetsDefaultsToCommittedAndRejectsUnknown() throws Exception {
    assertEquals(
        KafkaStartingOffset.EARLIEST,
        load("kafka: {startingOffsets: earliest}").kafka().startingOffset());
    assertEquals(
        KafkaStartingOffset.LATEST,
        load("kafka: {startingOffsets: LATEST}").kafka().startingOffset());
    assertThrows(
        IllegalArgumentException.class, () -> load("kafka: {startingOffsets: beginning}"));
  }

  /** sink: jdbc 는 토픽명을 안 적는 곳(defaults / topicPatterns)에서만 된다. */
  @Test
  void jdbcSink_allowedInPatterns_rejectedInTopics() throws Exception {
    PipelineConfig ok = load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        topicPatterns:
          - pattern: t.*
            destinations: [{system: postgres, sink: jdbc}]
        """);
    assertEquals(
        SinkMode.JDBC, new DefaultTopicBindingResolver(ok).resolve("t.a").getFirst().sink());

    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.a]}
        topics:
          t.a:
            destinations: [{system: postgres, sink: jdbc}]
        """));
  }

  /** Doris 는 공식 DorisSink 만 있다. 예전 값 dynamic 은 조용히 바꾸지 않고 실패. */
  @Test
  void jdbcForDoris_andOldDynamic_rejected() {
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        defaults:
          destinations: [{system: doris, sink: jdbc}]
        """));
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        defaults:
          destinations: [{system: postgres, sink: dynamic}]
        """));
  }

  /** Kafka 목적지는 name을 출력 토픽으로 유지하고, 예전 키·구독 겹침·writeMode는 시작 전에 거절한다. */
  @Test
  void kafkaDestinationUsesNameAndRejectsLoop() throws Exception {
    PipelineConfig config = load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: ['hv.j.dproc.rd_*']}
        defaults:
          destinations: [{system: kafka, target: hv.j.out.rd_lp}]
        """);
    DestinationConfig dest =
        new DefaultTopicBindingResolver(config).resolve("hv.j.dproc.rd_lp").getFirst();
    assertEquals(SystemType.KAFKA, dest.system());
    assertEquals("hv.j.out.rd_lp", dest.target());

    assertThrows(IllegalArgumentException.class, () -> load("""
        defaults:
          destinations: [{db: postgres}]
        """));
    assertThrows(IllegalArgumentException.class, () -> load("""
        defaults:
          destinations: [{system: kafka}]
        """));
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {subscribe: ['hv.*']}
        defaults:
          destinations: [{system: kafka, target: hv.out}]
        """));
    assertThrows(IllegalArgumentException.class, () -> load("""
        defaults:
          destinations: [{system: kafka, target: hv.out, writeMode: UPSERT}]
        """));
  }

  /** YAML databases 가 env 보다 우선, YAML 에 없는 DB 만 env 로 채운다. */
  @Test
  void resolve_yamlWinsOverEnvAndFillsMissingDb() {
    PipelineConfig config =
        new PipelineConfig(
            null,
            null,
            null,
            Map.of(),
            Map.of(SystemType.POSTGRES, new JdbcEndpointRecord("jdbc:postgresql://yaml/db", "y", "p")));

    YamlPipelineConfigLoader.Resolved resolved =
        YamlPipelineConfigLoader.resolve(
            config,
            Map.of(
                "PG_JDBC_URL", "jdbc:postgresql://env/db",
                "MSSQL_JDBC_URL", "jdbc:sqlserver://env",
                "MSSQL_USER", "sa",
                "MSSQL_PASSWORD", "x",
                "WRITE_BATCH_SIZE", "50"));

    assertEquals("jdbc:postgresql://yaml/db", resolved.endpoints().get(SystemType.POSTGRES).jdbcUrl());
    assertTrue(resolved.endpoints().containsKey(SystemType.MSSQL));
    assertEquals(50, resolved.writeBatchSize());
    assertEquals(1000L, resolved.writeBatchIntervalMs());
  }

  /** 배치 건수·간격은 YAML이 환경변수보다 우선하고, 생략하면 100건·1000ms다. */
  @Test
  void writeBatchYamlOverridesEnvAndRejectsZero() throws Exception {
    PipelineConfig yaml = load("""
        writeBatchSize: 1000
        writeBatchIntervalMs: 500
        """);
    YamlPipelineConfigLoader.Resolved resolved =
        YamlPipelineConfigLoader.resolve(yaml, Map.of("WRITE_BATCH_SIZE", "50"));
    assertEquals(1000, resolved.writeBatchSize());
    assertEquals(500L, resolved.writeBatchIntervalMs());

    YamlPipelineConfigLoader.Resolved fallback =
        YamlPipelineConfigLoader.resolve(load("{}"), Map.of());
    assertEquals(100, fallback.writeBatchSize());
    assertEquals(1000L, fallback.writeBatchIntervalMs());

    assertThrows(IllegalArgumentException.class, () -> load("writeBatchSize: 0"));
    assertThrows(IllegalArgumentException.class, () -> load("writeBatchIntervalMs: 0"));
  }

  private PipelineConfig load(String yaml) throws Exception {
    Path file = dir.resolve("p.yaml");
    Files.writeString(file, yaml);
    return new YamlPipelineConfigLoader().load(file);
  }
}
