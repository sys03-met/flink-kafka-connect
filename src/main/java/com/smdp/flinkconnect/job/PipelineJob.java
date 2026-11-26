package com.smdp.flinkconnect.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.KafkaConfig;
import com.smdp.flinkconnect.config.PipelineModels.KafkaStartingOffset;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.subscribe.TopicSubscriptions;
import com.smdp.flinkconnect.transform.DefaultTopicBindingResolver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Job 엔트리포인트 + Kafka 입력 스트림 조립. 처리 순서가 이 파일 위에서 아래로 그대로 읽힌다.
 *
 * <pre>
 * Kafka → {@link KafkaDecoder}(바이트→JSON) → {@link ExpandProcessor}(갈 곳마다 1→N) → {@link #attachSinks}(시스템별 Sink)
 * </pre>
 *
 * YAML 로드 → 위 그래프 조립 → execute. execute 이전에는 데이터를 읽지 않는다.
 * 안쪽 클래스는 Flink 가 워커로 직렬화해 보내므로 반드시 {@code static}.
 *
 * Usage: {@code PipelineJob config/example-pipeline.yaml}
 */
public final class PipelineJob {

  private static final Logger log = LoggerFactory.getLogger(PipelineJob.class);

  /** Job 진입점은 인스턴스를 만들지 않는다. */
  private PipelineJob() {}

  /** YAML → 환경변수 JDBC/배치 병합 → 그래프 조립 → 실행. */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: PipelineJob <pipeline.yaml>");
      System.exit(1);
    }
    PipelineConfig config = new YamlPipelineConfigLoader().load(Path.of(args[0]));
    YamlPipelineConfigLoader.Resolved settings = YamlPipelineConfigLoader.resolve(config, System.getenv());

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 10초마다 처리 지점을 남긴다. Kafka 그룹 오프셋도 이 성공 때 기록되므로
    // 재시작이 토픽 처음부터 다시 읽지 않는다. Sink는 at-least-once라 모드도 맞춘다.
    env.enableCheckpointing(10_000L);
    env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
    assemble(
        env,
        config,
        TopicSubscriptions.listKafkaTopics(config.kafka().bootstrapServers()),
        settings.endpoints(),
        settings.writeBatchSize(),
        settings.writeBatchIntervalMs());
    // 이 호출 이후에야 Source가 메시지를 읽기 시작
    env.execute(config.jobName());
  }

  /**
   * Kafka Source → expand → Sink 통로만 붙인다.
   *
   * @param brokerTopics 브로커에 있는 토픽 이름 (구독 glob 을 펼칠 재료)
   * @return expand 이후 WriteRecord 스트림 (Sink는 이미 연결됨)
   */
  static DataStream<WriteRecord> assemble(
      StreamExecutionEnvironment env,
      PipelineConfig config,
      Set<String> brokerTopics,
      Map<SystemType, JdbcEndpointRecord> endpoints,
      int batchSize,
      long batchIntervalMs
  ) {
    KafkaConfig kafka = config.kafka();
    // glob을 펼친 뒤 자동 DLQ가 입력에 섞이지 않게 한다 (아직 읽기 아님).
    List<String> topics = DeadLetterStream.inputTopics(
        TopicSubscriptions.expand(kafka.subscribe(), brokerTopics), config.dlq());
    if (topics.isEmpty()) {
      throw new IllegalStateException(
          "구독 토픽이 비었습니다. kafka.subscribe 패턴과 브로커 토픽을 확인하세요.");
    }

    // DLQ를 쓰지 않으면 기존 입력 그래프를 유지하고 오류 갈래도 만들지 않는다.
    DataStream<TopicRecord> records =
        config.dlq() == null
            ? readKafka(env, kafka, topics, new KafkaDecoder())
            : DeadLetterStream.attach(
                readKafka(env, kafka, topics, new DeadLetterStream.Decoder()), config);

    // 토픽→갈 곳·컬럼 변환 (1메시지 → N WriteRecord)
    DataStream<WriteRecord> writes = records.flatMap(new ExpandProcessor(config)).name("expand");
    // expand가 키를 풀므로, 나눌 칸이 있을 때만 그 다음에 건다. 없으면 지금처럼 순서 없이 흐른다.
    if (config.keyBy() != null) {
      writes = writes.keyBy(new RowFieldKey(config.keyBy()));
    }

    // 그래프는 메시지 전에 다 그려야 해서, 구독 토픽 전부의 갈 곳을 미리 푼다.
    // 설정 오류(모르는 op, BULK+sql 등)도 여기서 Job 시작 전에 걸린다.
    DefaultTopicBindingResolver resolver = new DefaultTopicBindingResolver(config);
    List<DestinationConfig> planned = new ArrayList<>();
    for (String topic : topics) {
      planned.addAll(resolver.resolve(topic));
    }
    attachSinks(writes, endpoints, batchSize, batchIntervalMs, planned, config);
    return writes;
  }

  /** DLQ 유무와 무관하게 같은 구독·오프셋 정책으로 Kafka Source 하나를 붙인다. */
  private static <T> DataStream<T> readKafka(
      StreamExecutionEnvironment env,
      KafkaConfig kafka,
      List<String> topics,
      KafkaRecordDeserializationSchema<T> decoder
  ) {
    KafkaSource<T> source =
        KafkaSource.<T>builder()
            .setBootstrapServers(kafka.bootstrapServers())
            .setTopics(topics)
            .setGroupId(kafka.groupId())
            .setStartingOffsets(startingOffsets(kafka.startingOffset()))
            .setDeserializer(decoder)
            .build();
    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
  }

  /**
   * 체크포인트가 없을 때만 쓰는 시작 위치.
   * committed는 그룹 기록이 없으면 맨 끝이라, 새 그룹이 보관분을 전부 넣지 않는다.
   */
  private static OffsetsInitializer startingOffsets(KafkaStartingOffset mode) {
    return switch (mode) {
      case COMMITTED -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
      case LATEST -> OffsetsInitializer.latest();
      case EARLIEST -> OffsetsInitializer.earliest();
    };
  }

  /**
   * Kafka 메시지(바이트) → {@link TopicRecord} (토픽명 + JSON 맵). KafkaSource 의 역직렬화기.
   *
   * 전제: 페이로드는 JSON Object만 (dproc = j). DLQ가 없으면 깨진 JSON은 위치만 기록하고 건너뛴다.
   */
  public static final class KafkaDecoder implements KafkaRecordDeserializationSchema<TopicRecord> {

    private static final long serialVersionUID = 1L;

    /** 직렬화에 안 맞고 무거워서 워커에서 {@link #open} 이 만든다. */
    private transient ObjectMapper mapper;

    /** Flink 생명주기: Source reader 시작 시 한 번 (deserialize 보다 먼저). */
    @Override
    public void open(DeserializationSchema.InitializationContext context) {
      // 첫 객체 뒤에 잘못된 데이터가 붙은 경우도 깨진 JSON으로 분류한다.
      mapper = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    /** Kafka 레코드마다 Flink가 호출. 성공하면 TopicRecord 한 건을 다음 단계로. */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TopicRecord> out) {
      if (record.value() == null) {
        return;
      }
      TopicRecord decoded;
      try {
        decoded = decode(record);
      } catch (IOException ex) {
        // DLQ를 껐을 때 원문이 로그에 저장되지 않도록 위치만 남긴다.
        log.warn("잘못된 JSON 건너뜀: topic={} partition={} offset={}",
            record.topic(), record.partition(), record.offset());
        return;
      }
      out.collect(decoded);
    }

    /** DLQ 사용 여부와 상관없이 같은 기준으로 JSON 객체를 해석한다. */
    TopicRecord decode(ConsumerRecord<byte[], byte[]> record) throws IOException {
      Map<String, Object> payload =
          mapper.readValue(record.value(), new TypeReference<Map<String, Object>>() {});
      // Kafka tombstone과 달리 바이트로 들어온 JSON null은 객체가 아니다.
      if (payload == null) {
        throw new IOException("JSON 객체가 아닙니다.");
      }
      return new TopicRecord(record.topic(), payload);
    }

    /** Flink 타입 시스템용. 그래프 조립 때 한 번 불린다. */
    @Override
    public TypeInformation<TopicRecord> getProducedType() {
      return TypeInformation.of(TopicRecord.class);
    }
  }

  /**
   * YAML {@code keyBy} 칸 값으로 나눈다. 같은 값은 같은 작업으로 가고, 없는 칸은 빈 문자열로 모은다.
   */
  static final class RowFieldKey implements KeySelector<WriteRecord, String> {

    private static final long serialVersionUID = 1L;

    private final String field;

    RowFieldKey(String field) {
      this.field = field;
    }

    @Override
    public String getKey(WriteRecord value) {
      return value.row().partitionKey(field);
    }
  }

  /**
   * Kafka 메시지 1건 → DB 쓰기 요청 0~N건. 갈 곳 결정은 {@link DefaultTopicBindingResolver#expand} 에 맡긴다.
   *
   * 메서드 이름이 flatMap 인 이유: Flink 가 정해 둔 이름이라 바꿀 수 없다.
   * map = 1건 → 정확히 1건, flatMap = 1건 → 0~N건(여러 개를 펼쳐서 flat 하게 내보냄).
   * 한 메시지가 Postgres+Doris 처럼 여러 DB로 가야 해서 flatMap 을 쓴다.
   */
  public static final class ExpandProcessor extends RichFlatMapFunction<TopicRecord, WriteRecord> {

    private static final long serialVersionUID = 1L;

    /** Task에 실어 보낼 YAML 설정 (직렬화됨). */
    private final PipelineConfig config;

    /** 워커에서 {@link #open} 이 만든다 (transient: 직렬화 제외). */
    private transient DefaultTopicBindingResolver resolver;

    public ExpandProcessor(PipelineConfig config) {
      this.config = config;
    }

    /** Flink 생명주기: Task(병렬 슬롯)가 시작될 때 한 번. flatMap 보다 항상 먼저. */
    @Override
    public void open(OpenContext openContext) {
      resolver = new DefaultTopicBindingResolver(config);
    }

    /** 레코드마다 호출. out.collect 한 번 = Sink로 한 건. */
    @Override
    public void flatMap(TopicRecord value, Collector<WriteRecord> out) {
      List<WriteRecord> requests = resolver.expand(value);
      log.debug("expand topic={} destinations={}", value.topic(), requests.size());
      for (WriteRecord req : requests) {
        out.collect(req);
      }
    }
  }

  /**
   * 쓰기 요청 스트림에 시스템별 Sink를 붙인다 (Sink 입구는 여기 하나. E2E 테스트도 이걸 부른다).
   *
   * 메시지마다 도는 코드가 아니라, Job 시작 때 그래프를 한 번 그리는 코드다.
   * 어댑터 짝은 {@link SystemType#newSink()} 에 있으므로 여기 switch는 없다.
   *
   * @param endpoints JDBC 시스템 접속. 키에 없는 JDBC 시스템은 붙이지 않는다
   * @param planned 구독 토픽 전부를 resolve 한 Destination 목록 (그래프를 그릴 재료)
   * @param config Kafka Sink가 브로커와 구독을 볼 때 쓴다
   */
  public static void attachSinks(
      DataStream<WriteRecord> writes,
      Map<SystemType, JdbcEndpointRecord> endpoints,
      int batchSize,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      PipelineConfig config
  ) {
    int batch = Math.max(1, batchSize);
    // 0이면 타이머가 쉬지 않고 같은 버퍼를 계속 내보낸다. YAML 검증을 한 번 더 막는다.
    long interval = Math.max(1L, batchIntervalMs);
    for (SystemType system : SystemType.values()) {
      List<DestinationConfig> mine = planned.stream().filter(d -> d.system() == system).toList();
      if (mine.isEmpty()) {
        continue;
      }
      JdbcEndpointRecord endpoint = endpoints.get(system);
      // JDBC 시스템만 접속이 필요하다. Kafka는 브로커 주소를 config에서 본다.
      if (system.jdbc() && endpoint == null) {
        log.warn("{} 접속 정보가 없어 이 시스템으로 가는 행은 적재하지 않습니다.", system);
        continue;
      }
      system.newSink().attach(writes, endpoint, batch, interval, mine, config);
    }
  }

}
