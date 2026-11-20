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
import java.net.InetSocketAddress;
import java.net.Socket;
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
import org.junit.jupiter.api.Timeout;

/**
 * 검침 10만: Kafka produce + expand → 로컬 Flink + 공식 Sink(PG JDBC / DorisSink).
 */
@Tag("docker")
@Tag("load")
class RdLpLoad100kE2ETest {

  private static final int N = 100_000;
  private static final String TOPIC = "hv.j.dproc.rd_lp";
  private static final String TABLE = "RD_LP";
  private static final Path YAML = Path.of("config/doris-streamload-e2e.yaml");
  private static final String BOOTSTRAP = "localhost:19092";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeAll
  static void requireDocker() {
    assumeTrue(portOpen(19092), "Kafka down");
    assumeTrue(portOpen(5432), "Postgres down");
    assumeTrue(portOpen(9030), "Doris JDBC down");
    assumeTrue(portOpen(8040), "Doris BE Stream Load down");
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  void load100kMeteringRecords_postgresAndDorisStreamLoad() throws Exception {
    ensureTopicAndTables();
    truncateTables();

    PipelineConfig config = new YamlPipelineConfigLoader().load(YAML);
    YamlPipelineConfigLoader.Resolved settings = YamlPipelineConfigLoader.resolve(config, Map.of());
    assertTrue(
        settings.endpoints().values().stream()
            .anyMatch(e -> e.streamLoadUrl() != null && !e.streamLoadUrl().isBlank()),
        "streamLoadUrl must be set");

    DefaultTopicBindingResolver resolver = new DefaultTopicBindingResolver(config);

    long tProduce = System.nanoTime();
    produce100k();
    long produceMs = (System.nanoTime() - tProduce) / 1_000_000L;

    List<WriteRecord> all = new ArrayList<>(N * 2);
    for (int i = 0; i < N; i++) {
      all.addAll(resolver.expand(new TopicRecord(TOPIC, rdPayload(i))));
    }

    long tWrite = System.nanoTime();
    FlinkLocalWrites.writeAll(all, settings.endpoints(), 5_000);
    long writeMs = (System.nanoTime() - tWrite) / 1_000_000L;

    assertEquals(N, countPg());
    assertEquals(N, FlinkLocalWrites.Doris.count(TABLE));

    System.out.printf(
        "RD_LP 100k official sinks: produceMs=%d writeMs=%d pg=%d doris=%d%n",
        produceMs, writeMs, countPg(), FlinkLocalWrites.Doris.count(TABLE));

    assertTrue(produceMs < 180_000, "produce too slow: " + produceMs + "ms");
    assertTrue(writeMs < 900_000, "write too slow: " + writeMs + "ms");
  }

  private static void produce100k() throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(64 * 1024));
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < N; i++) {
        producer.send(new ProducerRecord<>(TOPIC, null, MAPPER.writeValueAsBytes(rdPayload(i))));
        if (i % 10_000 == 0) {
          producer.flush();
        }
      }
      producer.flush();
    }
  }

  private static Map<String, Object> rdPayload(int i) {
    Map<String, Object> m = new HashMap<>();
    m.put("METER_ID", String.format("M%06d", i));
    m.put("FAP", 10.0 + (i % 100) * 0.1);
    m.put("LARAP", 1.0 + (i % 50) * 0.01);
    return m;
  }

  private static void ensureTopicAndTables() throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    try (AdminClient admin = AdminClient.create(props)) {
      try {
        admin
            .createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1)))
            .all()
            .get(30, TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }
    }
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE IF NOT EXISTS "
              + TABLE
              + " (METER_ID VARCHAR(64) PRIMARY KEY, FAP DOUBLE PRECISION, LARAP DOUBLE PRECISION,"
              + " SERVER_DT VARCHAR(32), TOTAL_POWER DOUBLE PRECISION)");
    }
    FlinkLocalWrites.Doris.ensureRdLp();
  }

  private static void truncateTables() throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE " + TABLE);
    }
    FlinkLocalWrites.Doris.truncate(TABLE);
  }

  private static int countPg() throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/rd", "rd", "rd");
        ResultSet rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
      rs.next();
      return rs.getInt(1);
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
