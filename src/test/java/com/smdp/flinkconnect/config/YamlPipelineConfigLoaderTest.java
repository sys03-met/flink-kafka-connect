package com.smdp.flinkconnect.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smdp.flinkconnect.config.PipelineModels.DbType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
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
    assertEquals(DbType.POSTGRES, destinations.get(0).db());
    assertEquals(WriteMode.UPSERT, destinations.get(0).writeMode());
    assertEquals(SinkMode.OFFICIAL, destinations.get(0).sink());
    assertEquals("RD_LP", destinations.get(0).table());
    assertEquals(List.of("METER_ID"), destinations.get(0).primaryKeys());
    assertEquals(DbType.DORIS, destinations.get(1).db());
  }

  /** sink: jdbc 는 토픽명을 안 적는 곳(defaults / topicPatterns)에서만 된다. */
  @Test
  void jdbcSink_allowedInPatterns_rejectedInTopics() throws Exception {
    PipelineConfig ok = load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        topicPatterns:
          - pattern: t.*
            destinations: [{db: postgres, sink: jdbc}]
        """);
    assertEquals(
        SinkMode.JDBC, new DefaultTopicBindingResolver(ok).resolve("t.a").getFirst().sink());

    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.a]}
        topics:
          t.a:
            destinations: [{db: postgres, sink: jdbc}]
        """));
  }

  /** Doris 는 공식 DorisSink 만 있다. 예전 값 dynamic 은 조용히 바꾸지 않고 실패. */
  @Test
  void jdbcForDoris_andOldDynamic_rejected() {
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        defaults:
          destinations: [{db: doris, sink: jdbc}]
        """));
    assertThrows(IllegalArgumentException.class, () -> load("""
        kafka: {bootstrapServers: x, groupId: g, subscribe: [t.*]}
        defaults:
          destinations: [{db: postgres, sink: dynamic}]
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
            Map.of(DbType.POSTGRES, new JdbcEndpointRecord("jdbc:postgresql://yaml/db", "y", "p")));

    YamlPipelineConfigLoader.Resolved resolved =
        YamlPipelineConfigLoader.resolve(
            config,
            Map.of(
                "PG_JDBC_URL", "jdbc:postgresql://env/db",
                "MSSQL_JDBC_URL", "jdbc:sqlserver://env",
                "MSSQL_USER", "sa",
                "MSSQL_PASSWORD", "x",
                "WRITE_BATCH_SIZE", "50"));

    assertEquals("jdbc:postgresql://yaml/db", resolved.endpoints().get(DbType.POSTGRES).jdbcUrl());
    assertTrue(resolved.endpoints().containsKey(DbType.MSSQL));
    assertEquals(50, resolved.writeBatchSize());
  }

  private PipelineConfig load(String yaml) throws Exception {
    Path file = dir.resolve("p.yaml");
    Files.writeString(file, yaml);
    return new YamlPipelineConfigLoader().load(file);
  }
}
