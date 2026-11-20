package com.smdp.flinkconnect.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.transform.DefaultTopicBindingResolver;
import com.smdp.flinkconnect.transform.TopicBindingResolverTest;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * M3 시임: 토픽 10개에 동시 produce → expand → Writer → DB 조회.
 *
 * <p>성능 관찰: 동시 produce+적재 시간, 토픽당 WriteRecord 수(팬아웃 배수).
 */
@Tag("docker")
class MultiTopicKafkaDbE2ETest {

  private static final Path YAML = Path.of("config/multi-topic-e2e.yaml");
  private static final String BOOTSTRAP = "localhost:19092";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeAll
  static void requireDocker() {
    assumeTrue(portOpen(19092), "Kafka 19092 down");
    assumeTrue(portOpen(5432), "Postgres down");
    assumeTrue(portOpen(9030), "Doris JDBC down");
    assumeTrue(portOpen(8040), "Doris BE down");
    assumeTrue(portOpen(1433), "MSSQL down");
  }

  @Test
  void tenTopics_concurrentProduce_landInAllDatabases() throws Exception {
    List<String> all = new ArrayList<>();
    all.addAll(TopicBindingResolverTest.RD_TOPICS);
    all.addAll(TopicBindingResolverTest.METER_TOPICS);
    assertEquals(10, all.size());

    ensureKafkaTopics(all);
    ensureTables(all);

    String runId = "mt-" + UUID.randomUUID().toString().substring(0, 8);

    long t0 = System.nanoTime();
    // 토픽 10개에 동시에 produce
    ExecutorService pool = Executors.newFixedThreadPool(10);
    List<Future<?>> produces = new ArrayList<>();
    for (String topic : all) {
      produces.add(
          pool.submit(
              () -> {
                try {
                  Map<String, Object> payload =
                      topic.contains(".rd_") ? rdPayload(runId) : meterPayload(runId);
                  produceJson(topic, payload);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }));
    }
    for (Future<?> f : produces) {
      f.get(30, TimeUnit.SECONDS);
    }
    long produceMs = (System.nanoTime() - t0) / 1_000_000L;

    PipelineConfig config = new YamlPipelineConfigLoader().load(YAML);
    YamlPipelineConfigLoader.Resolved settings = YamlPipelineConfigLoader.resolve(config, Map.of());
    DefaultTopicBindingResolver resolver = new DefaultTopicBindingResolver(config);

    long t1 = System.nanoTime();
    int writeRequests = 0;
    List<WriteRecord> allReqs = new ArrayList<>();
    for (String topic : all) {
      Map<String, Object> payload =
          topic.contains(".rd_") ? rdPayload(runId) : meterPayload(runId);
      List<WriteRecord> reqs = resolver.expand(new TopicRecord(topic, payload));
      writeRequests += reqs.size();
      allReqs.addAll(reqs);
    }
    FlinkLocalWrites.writeAll(allReqs, settings.endpoints(), settings.writeBatchSize());
    long writeMs = (System.nanoTime() - t1) / 1_000_000L;

    // rd 5×2 + meter 5×1 = 15 WriteRecord (팬아웃 배수 — 성능 부담 지점)
    assertEquals(15, writeRequests);

    for (String topic : TopicBindingResolverTest.RD_TOPICS) {
      String table = tableFromTopic(topic);
      assertEquals(1, countPg(table, runId), "pg " + table);
      assertEquals(1, countDoris(table, runId), "doris " + table);
    }
    for (String topic : TopicBindingResolverTest.METER_TOPICS) {
      String table = tableFromTopic(topic);
      assertEquals(1, countMssql(table, runId), "mssql " + table);
    }

    assertTrue(produceMs < 15_000, "concurrent produce slow: " + produceMs + "ms");
    assertTrue(writeMs < 30_000, "expand+write slow: " + writeMs + "ms");

    System.out.printf(
        "M3 perf: topics=%d produceMs=%d writeMs=%d writeRequests=%d (fanout hint)%n",
        all.size(), produceMs, writeMs, writeRequests);
  }

  private static String tableFromTopic(String topic) {
    int i = topic.lastIndexOf('.');
    return topic.substring(i + 1).toUpperCase();
  }

  private static void ensureTables(List<String> topics) throws Exception {
    for (String topic : topics) {
      String table = tableFromTopic(topic);
      if (topic.contains(".rd_")) {
        try (Connection c =
                DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
            Statement st = c.createStatement()) {
          st.execute(
              "CREATE TABLE IF NOT EXISTS "
                  + table
                  + " (METER_ID VARCHAR(64) PRIMARY KEY, FAP DOUBLE PRECISION, LARAP DOUBLE PRECISION)");
        }
        FlinkLocalWrites.Doris.ensureRdTable(table);
      } else {
        ensureMssqlDb();
        try (Connection c =
                DriverManager.getConnection(
                    "jdbc:sqlserver://localhost:1433;databaseName=ami;encrypt=false;trustServerCertificate=true",
                    "sa",
                    "Your_strong_Passw0rd");
            Statement st = c.createStatement()) {
          st.execute(
              "IF OBJECT_ID('"
                  + table
                  + "') IS NULL CREATE TABLE "
                  + table
                  + " (METER_ID VARCHAR(64) NOT NULL PRIMARY KEY, LIMIT_A FLOAT NULL)");
        }
      }
    }
  }

  private static void ensureMssqlDb() throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:sqlserver://localhost:1433;encrypt=false;trustServerCertificate=true",
                "sa",
                "Your_strong_Passw0rd");
        Statement st = c.createStatement()) {
      st.execute("IF DB_ID('ami') IS NULL CREATE DATABASE ami");
    }
  }

  private static void ensureKafkaTopics(List<String> topics) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    try (AdminClient admin = AdminClient.create(props)) {
      List<NewTopic> news =
          topics.stream().map(t -> new NewTopic(t, 1, (short) 1)).collect(Collectors.toList());
      try {
        admin.createTopics(news).all().get(30, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // already exists
      }
    }
  }

  private static void produceJson(String topic, Map<String, Object> payload) throws Exception {
    byte[] body = MAPPER.writeValueAsBytes(payload);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, null, body)).get(10, TimeUnit.SECONDS);
    }
  }

  private static Map<String, Object> rdPayload(String meterId) {
    Map<String, Object> m = new HashMap<>();
    m.put("METER_ID", meterId);
    m.put("FAP", 3.0);
    m.put("LARAP", 1.5);
    return m;
  }

  private static Map<String, Object> meterPayload(String meterId) {
    return Map.of("METER_ID", meterId, "LIMIT_A", 9.0);
  }

  private static int countPg(String table, String meterId) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        var ps = c.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE METER_ID = ?")) {
      ps.setString(1, meterId);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static int countDoris(String table, String meterId) throws Exception {
    return FlinkLocalWrites.Doris.countWhere(table, meterId);
  }

  private static int countMssql(String table, String meterId) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:sqlserver://localhost:1433;databaseName=ami;encrypt=false;trustServerCertificate=true",
                "sa",
                "Your_strong_Passw0rd");
        var ps = c.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE METER_ID = ?")) {
      ps.setString(1, meterId);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static boolean portOpen(int port) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("localhost", port), 400);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
