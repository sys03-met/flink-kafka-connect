package com.smdp.flinkconnect.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.transform.DefaultTopicBindingResolver;
import java.util.ArrayList;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
 * S2 시임: expand → 공식 Sink(Flink 로컬) 후 PG / Doris / MSSQL에 행이 생기는지.
 *
 * <p>전제: {@code .tools/.test/flink-e2e/docker-compose.yml} 기동.
 */
@Tag("docker")
class ExpandToDbE2ETest {

  private static final Path YAML = Path.of("config", "e2e-pipeline.yaml");
  private static final String BOOTSTRAP = "localhost:19092";

  @BeforeAll
  static void requireDockerStack() {
    assumeTrue(portOpen("localhost", 19092), "Kafka(Redpanda) 19092 not up");
    assumeTrue(portOpen("localhost", 5432), "Postgres 5432 not up");
    assumeTrue(portOpen("localhost", 9030), "Doris JDBC 9030 not up");
    assumeTrue(portOpen("localhost", 8040), "Doris BE 8040 not up");
    assumeTrue(portOpen("localhost", 1433), "MSSQL 1433 not up");
  }

  @Test
  void representativeTopicsLandInPostgresDorisAndMssql() throws Exception {
    ensureKafkaTopics();
    ensureMssqlSchema();
    FlinkLocalWrites.Doris.ensureRdLp();

    String meterId = "e2e-" + UUID.randomUUID().toString().substring(0, 8);
    Map<String, Object> rdPayload = rdPayload(meterId);
    Map<String, Object> meterPayload = Map.of("METER_ID", meterId, "LIMIT_A", 42.5);

    produceJson("hv.j.dproc.rd_lp", rdPayload);
    produceJson("hv.j.dproc.meter_curr_limit", meterPayload);

    PipelineConfig config = new YamlPipelineConfigLoader().load(YAML);
    YamlPipelineConfigLoader.Resolved settings = YamlPipelineConfigLoader.resolve(config, Map.of());
    DefaultTopicBindingResolver resolver = new DefaultTopicBindingResolver(config);

    List<WriteRecord> all = new ArrayList<>();
    all.addAll(resolver.expand(new TopicRecord("hv.j.dproc.rd_lp", rdPayload)));
    all.addAll(resolver.expand(new TopicRecord("hv.j.dproc.meter_curr_limit", meterPayload)));
    FlinkLocalWrites.writeAll(all, settings.endpoints(), settings.writeBatchSize());

    assertEquals(1, countPostgres(meterId));
    assertEquals(1, countDoris(meterId));
    assertEquals(1, countMssql(meterId));
    assertTrue(postgresTotalPower(meterId) > 0);
  }

  private static Map<String, Object> rdPayload(String meterId) {
    Map<String, Object> m = new HashMap<>();
    m.put("METER_ID", meterId);
    m.put("FAP", 10.0);
    m.put("LARAP", 2.5);
    m.put("SERVER_DT", "2026-11-17 15:00:00");
    return m;
  }

  private static void ensureKafkaTopics() throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    try (AdminClient admin = AdminClient.create(props)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic("hv.j.dproc.rd_lp", 1, (short) 1),
                  new NewTopic("hv.j.dproc.meter_curr_limit", 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    } catch (Exception ignored) {
      // 이미 있으면 무시
    }
  }

  private static void produceJson(String topic, Map<String, Object> payload) throws Exception {
    String json =
        new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(payload);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      producer
          .send(new ProducerRecord<>(topic, null, json.getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static void ensureMssqlSchema() throws Exception {
    String url =
        "jdbc:sqlserver://localhost:1433;encrypt=false;trustServerCertificate=true";
    try (Connection c = DriverManager.getConnection(url, "sa", "Your_strong_Passw0rd");
        Statement st = c.createStatement()) {
      st.execute("IF DB_ID('ami') IS NULL CREATE DATABASE ami");
    }
    String dbUrl =
        "jdbc:sqlserver://localhost:1433;databaseName=ami;encrypt=false;trustServerCertificate=true";
    try (Connection c = DriverManager.getConnection(dbUrl, "sa", "Your_strong_Passw0rd");
        Statement st = c.createStatement()) {
      st.execute(
          """
          IF OBJECT_ID('METER_CURR_LIMIT') IS NULL
          CREATE TABLE METER_CURR_LIMIT (
            METER_ID VARCHAR(64) NOT NULL PRIMARY KEY,
            LIMIT_A FLOAT NULL
          )
          """);
    }
  }

  private static int countPostgres(String meterId) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        var ps = c.prepareStatement("SELECT COUNT(*) FROM RD_LP WHERE METER_ID = ?")) {
      ps.setString(1, meterId.toUpperCase());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static double postgresTotalPower(String meterId) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        var ps =
            c.prepareStatement("SELECT TOTAL_POWER FROM RD_LP WHERE METER_ID = ?")) {
      ps.setString(1, meterId.toUpperCase());
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getDouble(1);
      }
    }
  }

  private static int countDoris(String meterId) throws Exception {
    // doris destination에는 columns 규칙 없음 → METER_ID 대소문자 그대로
    return FlinkLocalWrites.Doris.countWhere("RD_LP", meterId);
  }

  private static int countMssql(String meterId) throws Exception {
    String url =
        "jdbc:sqlserver://localhost:1433;databaseName=ami;encrypt=false;trustServerCertificate=true";
    try (Connection c = DriverManager.getConnection(url, "sa", "Your_strong_Passw0rd");
        var ps = c.prepareStatement("SELECT COUNT(*) FROM METER_CURR_LIMIT WHERE METER_ID = ?")) {
      ps.setString(1, meterId);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static boolean portOpen(String host, int port) {
    try (Socket ignored = new Socket()) {
      ignored.connect(new java.net.InetSocketAddress(host, port), 400);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
