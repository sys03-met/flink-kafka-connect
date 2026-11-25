package com.smdp.flinkconnect.job;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DefaultsConfig;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.DlqConfig;
import com.smdp.flinkconnect.config.PipelineModels.DlqType;
import com.smdp.flinkconnect.config.PipelineModels.KafkaConfig;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.TopicConfig;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.job.PipelineJob.ExpandProcessor;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.transform.TopicBindingResolverTest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kafka 레코드의 정상 적재 변환과 실패 갈래를 실제 브로커 없이 검증한다.
 *
 * Flink 가 하듯 {@code open} 을 먼저 부르고 {@code flatMap} 을 부른다 (클러스터 없이).
 */
class PipelineJobTest {

  /** 오류를 원본 토픽별 파일에 추가하고, 정상 JSON과 tombstone은 파일로 보내지 않는다. */
  @Test
  void routesInvalidJsonToTopicFiles(@TempDir Path temp) throws Exception {
    Path errors = temp.resolve(".error");
    var config = new PipelineConfig(
        null, null, null, null, null, "file-test",
        new DlqConfig(DlqType.FILE, null, errors.toString(), null));
    var decoder = new DeadLetterStream.Decoder();
    decoder.open(null);
    byte[] invalid = {'{', (byte) 0xff};
    byte[] key = {42};
    List<Either<Tuple2<String, String>, TopicRecord>> decoded = new ArrayList<>();
    decoder.deserialize(new ConsumerRecord<>("input", 2, 9L, key, invalid), collecting(decoded));
    decoder.deserialize(new ConsumerRecord<>("input", 2, 10L, null,
        "{\"ok\":true}".getBytes(StandardCharsets.UTF_8)), collecting(decoded));
    decoder.deserialize(new ConsumerRecord<>("input", 2, 11L, null, null), collecting(decoded));
    decoder.deserialize(new ConsumerRecord<>("input", 2, 12L, null, invalid), collecting(decoded));
    decoder.deserialize(new ConsumerRecord<>("other", 0, 3L, null, invalid), collecting(decoded));
    assertEquals(4, decoded.size());

    // 제한 개수에서 취소하지 않고 스트림 끝까지 읽어 파일 Sink 종료도 기다린다.
    var env = StreamExecutionEnvironment.createLocalEnvironment(1);
    var input = env.fromCollection(decoded, decoder.getProducedType());
    try (var rows = DeadLetterStream.attach(input, config).executeAndCollect()) {
      assertEquals(new TopicRecord("input", Map.of("ok", true)), rows.next());
      assertFalse(rows.hasNext());
    }
    List<String> lines = Files.readAllLines(errors.resolve("input.error"));
    assertEquals(2, lines.size());
    assertEquals(1, Files.readAllLines(errors.resolve("other.error")).size());
    JsonNode error = new ObjectMapper().readTree(lines.getFirst());
    assertEquals("input", error.get("topic").asText());
    assertEquals(2, error.get("partition").asInt());
    assertEquals(9L, error.get("offset").asLong());
    assertArrayEquals(key, error.get("keyBase64").binaryValue());
    assertArrayEquals(invalid, error.get("valueBase64").binaryValue());
  }

  /** 명시한 파일 이름 하나로 합치고 이미 있는 .error는 중복으로 붙이지 않는다. */
  @Test
  void explicitFileNameKeepsSingleSuffix(@TempDir Path temp) throws Exception {
    var sink = new DeadLetterStream.TextSink(
        new DlqConfig(DlqType.FILE, null, temp.toString(), "rejected.error"));
    sink.invoke(Tuple2.of("input", "{}"), null);
    sink.invoke(Tuple2.of("other", "{}"), null);
    assertEquals(List.of("{}", "{}"), Files.readAllLines(temp.resolve("rejected.error")));
    assertFalse(Files.exists(temp.resolve("input.error")));
  }

  /** 자동 이름은 원본마다 달라지고, 이미 존재하는 오류 토픽을 다시 읽지 않는다. */
  @Test
  void kafkaDlqUsesSourceSuffixUnlessConfigured() {
    var auto = new DlqConfig(DlqType.KAFKA, null, null, null);
    assertEquals(List.of("input", "other"), DeadLetterStream.inputTopics(
        List.of("input", "input.error", "old.error", "other"), auto));
    assertThrows(IllegalArgumentException.class,
        () -> DeadLetterStream.inputTopics(List.of("a".repeat(244)), auto));
    var serializer = DeadLetterStream.kafkaSerializer(null);
    assertEquals("input.error", serializer.serialize(Tuple2.of("input", "{}"), null, null).topic());
    assertEquals("other.error", serializer.serialize(Tuple2.of("other", "{}"), null, null).topic());
    assertEquals("custom.errors", DeadLetterStream.kafkaSerializer("custom.errors")
        .serialize(Tuple2.of("input", "{}"), null, null).topic());
  }

  /** Kafka DLQ Sink는 설정했을 때만 Job 그래프에 등록한다. */
  @Test
  void kafkaDlqSinkIsOptIn() {
    var kafka = new KafkaConfig("localhost:9092", "test", List.of("input"));
    for (boolean enabled : List.of(false, true)) {
      var config = new PipelineConfig(kafka, null, null, null, null, null,
          enabled ? new DlqConfig(DlqType.KAFKA, null, null, null) : null);
      var env = StreamExecutionEnvironment.createLocalEnvironment(1);
      PipelineJob.assemble(env, config, Set.of("input"), Map.of(), 1);
      assertEquals(enabled, env.getStreamGraph().getStreamNodes().stream()
          .anyMatch(node -> node.getOperatorName().contains("dlq-kafka")));
    }
  }

  /** 갈 곳 2개(PG+Doris) → WriteRecord 2건. */
  @Test
  void expandsToMultipleDestinations() throws Exception {
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
                        new DestinationConfig(SystemType.POSTGRES, "RD_LP", WriteMode.UPSERT, List.of()),
                        new DestinationConfig(SystemType.DORIS, "RD_LP", WriteMode.BULK, List.of())))));

    List<WriteRecord> out = new ArrayList<>();
    opened(config).flatMap(new TopicRecord("hv.j.dproc.rd_lp", rdPayload("M1")), collecting(out));

    assertEquals(2, out.size());
    assertEquals(SystemType.POSTGRES, out.get(0).destination().system());
    assertEquals(SystemType.DORIS, out.get(1).destination().system());
  }

  /** multi-topic-e2e.yaml: 토픽 10개가 한 ExpandProcessor 에서 모두 WriteRecord 로 나온다. */
  @Test
  void tenTopics_expandToAllWriteRecords() throws Exception {
    ExpandProcessor expand = opened(multiTopicConfig());
    List<WriteRecord> out = new ArrayList<>();
    Collector<WriteRecord> collector = collecting(out);

    for (String topic : TopicBindingResolverTest.RD_TOPICS) {
      expand.flatMap(new TopicRecord(topic, rdPayload("m1")), collector);
    }
    for (String topic : TopicBindingResolverTest.METER_TOPICS) {
      expand.flatMap(new TopicRecord(topic, meterPayload("m1")), collector);
    }

    // rd 5 × (PG+Doris) + meter 5 × MSSQL = 15
    assertEquals(15, out.size());
    Set<String> tables =
        out.stream().map(r -> r.destination().target()).collect(Collectors.toCollection(HashSet::new));
    assertTrue(tables.contains("RD_LP"));
    assertTrue(tables.contains("METER_CURR_LIMIT"));
    assertEquals(5, out.stream().filter(r -> r.destination().system() == SystemType.POSTGRES).count());
    assertEquals(5, out.stream().filter(r -> r.destination().system() == SystemType.DORIS).count());
    assertEquals(5, out.stream().filter(r -> r.destination().system() == SystemType.MSSQL).count());
  }

  /** 동시성 스모크: 스레드 10개가 각자 expand 해도 결과가 빠지지 않는다. */
  @Test
  void concurrentExpand_tenTopics_noLostRequests() throws Exception {
    PipelineConfig config = multiTopicConfig();
    ExecutorService pool = Executors.newFixedThreadPool(10);
    Map<String, List<WriteRecord>> byTopic = new ConcurrentHashMap<>();
    List<String> all = new ArrayList<>(TopicBindingResolverTest.RD_TOPICS);
    all.addAll(TopicBindingResolverTest.METER_TOPICS);

    long started = System.nanoTime();
    List<Future<?>> futures = new ArrayList<>();
    for (String topic : all) {
      futures.add(
          pool.submit(
              () -> {
                List<WriteRecord> bag = new ArrayList<>();
                Map<String, Object> payload =
                    topic.contains(".rd_") ? rdPayload(topic) : meterPayload(topic);
                opened(config).flatMap(new TopicRecord(topic, payload), collecting(bag));
                byTopic.put(topic, bag);
              }));
    }
    for (Future<?> f : futures) {
      f.get(30, TimeUnit.SECONDS);
    }
    pool.shutdown();
    long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

    assertEquals(10, byTopic.size());
    assertEquals(15, byTopic.values().stream().mapToInt(List::size).sum());
    // 성능 스모크: 10토픽 동시 expand가 비정상적으로 느리면 실패 (로컬 기준 여유)
    assertTrue(elapsedMs < 5_000, "concurrent expand too slow: " + elapsedMs + "ms");
  }

  private static PipelineConfig multiTopicConfig() throws Exception {
    return new YamlPipelineConfigLoader().load(Path.of("config/multi-topic-e2e.yaml"));
  }

  /** Flink 처럼 open 까지 부른 ExpandProcessor. */
  private static ExpandProcessor opened(PipelineConfig config) {
    ExpandProcessor fn = new ExpandProcessor(config);
    fn.open(DefaultOpenContext.INSTANCE);
    return fn;
  }

  private static Map<String, Object> rdPayload(String id) {
    Map<String, Object> m = new HashMap<>();
    m.put("METER_ID", id);
    m.put("FAP", 1.0);
    m.put("LARAP", 2.0);
    return m;
  }

  private static Map<String, Object> meterPayload(String id) {
    return Map.of("METER_ID", id, "LIMIT_A", 1.0);
  }

  /** Flink 없이도 디코더와 변환기가 내보낸 레코드를 검사한다. */
  private static <T> Collector<T> collecting(List<T> sink) {
    return new Collector<>() {
      /** 출력 순서 그대로 테스트 목록에 담는다. */
      @Override
      public void collect(T record) {
        sink.add(record);
      }

      /** 메모리 목록은 별도 종료 작업이 없다. */
      @Override
      public void close() {}
    };
  }
}
