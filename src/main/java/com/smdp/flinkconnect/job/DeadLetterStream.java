package com.smdp.flinkconnect.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.DlqConfig;
import com.smdp.flinkconnect.config.PipelineModels.DlqType;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Flink 입력용 선택 DLQ.
 *
 * - Kafka JSON을 정상 레코드와 역직렬화 실패로 나눈다.
 * - 실패 원문을 같은 Kafka 클러스터의 별도 토픽 또는 워커 디렉터리에 기록한다.
 *
 * YAML 사용 예: {@code dlq: {type: file}}. DB 쓰기 실패는 이 갈래에서 처리하지 않는다.
 */
final class DeadLetterStream {

  private static final String SUFFIX = ".error";
  // 오류 JSON을 다시 파싱하지 않고 저장 이름을 정하도록 원본 토픽을 함께 보낸다.
  private static final TypeInformation<Tuple2<String, String>> ERROR_TYPE =
      Types.TUPLE(Types.STRING, Types.STRING);
  private static final OutputTag<Tuple2<String, String>> ERRORS =
      new OutputTag<>("invalid-kafka-json", ERROR_TYPE);

  /** DLQ 갈래는 Job 그래프를 조립할 때만 호출한다. */
  private DeadLetterStream() {}

  /** Kafka 자동 이름 모드에서는 기존 DLQ를 입력으로 읽지 않고 새 이름 길이도 미리 검사한다. */
  static List<String> inputTopics(List<String> topics, DlqConfig dlq) {
    if (dlq == null || dlq.type() != DlqType.KAFKA || dlq.topic() != null) {
      return topics;
    }
    // 원본 토픽이 제거된 뒤에도 남은 DLQ가 glob에 다시 잡히지 않게 접미사를 예약한다.
    List<String> inputs = topics.stream().filter(topic -> !topic.endsWith(SUFFIX)).toList();
    for (String topic : inputs) {
      if (topic.length() + SUFFIX.length() > 249) {
        throw new IllegalArgumentException(
            "자동 DLQ 토픽명이 Kafka 249자 제한을 넘습니다. dlq.topic을 지정하세요: " + topic);
      }
    }
    return inputs;
  }

  /** 실패만 설정된 저장소로 보내고, 정상 레코드는 기존 적재 단계에 넘긴다. */
  static DataStream<TopicRecord> attach(
      DataStream<Either<Tuple2<String, String>, TopicRecord>> input, PipelineConfig config
  ) {
    SingleOutputStreamOperator<TopicRecord> valid = input.process(new Split()).name("route-dlq");
    DataStream<Tuple2<String, String>> errors = valid.getSideOutput(ERRORS);
    // 실패 저장 자체가 실패하면 건너뛰지 않고 Flink에 예외를 전달한다.
    switch (config.dlq().type()) {
      case KAFKA -> errors.sinkTo(
          KafkaSink.<Tuple2<String, String>>builder()
              .setBootstrapServers(config.kafka().bootstrapServers())
              // 체크포인트를 켠 환경에서 승인되지 않은 레코드를 남긴 채 진행하지 않는다.
              .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
              .setRecordSerializer(kafkaSerializer(config.dlq().topic()))
              .build()).name("dlq-kafka");
      // 동일 경로에 여러 subtask가 동시에 쓰지 않게 파일 출력만 Job당 한 writer로 제한한다.
      case FILE -> errors.addSink(new TextSink(config.dlq())).name("dlq-file").setParallelism(1);
    }
    return valid;
  }

  /** 이름이 없으면 각 실패 레코드의 원본 토픽에 접미사를 붙여 동일한 JSON 형식으로 발행한다. */
  static KafkaRecordSerializationSchema<Tuple2<String, String>> kafkaSerializer(String topic) {
    return (error, context, timestamp) -> new ProducerRecord<>(
        topic == null ? error.f0 + SUFFIX : topic, error.f1.getBytes(StandardCharsets.UTF_8));
  }

  /** 정상 JSON은 오른쪽, 재처리용 오류 JSON은 왼쪽에 담아 Source에서 전달한다. */
  static final class Decoder
      implements KafkaRecordDeserializationSchema<Either<Tuple2<String, String>, TopicRecord>> {
    private final PipelineJob.KafkaDecoder json = new PipelineJob.KafkaDecoder();
    private transient ObjectMapper mapper;

    /** Kafka reader마다 기존 JSON 파서와 오류 직렬화기를 한 번 준비한다. */
    @Override
    public void open(DeserializationSchema.InitializationContext context) {
      json.open(context);
      mapper = new ObjectMapper();
    }

    /** tombstone은 무시하고 실패 원문은 인코딩 손실 없이 보존한다. */
    @Override
    public void deserialize(
        ConsumerRecord<byte[], byte[]> record,
        Collector<Either<Tuple2<String, String>, TopicRecord>> out
    ) throws IOException {
      if (record.value() == null) {
        return;
      }
      TopicRecord decoded;
      try {
        decoded = json.decode(record);
      } catch (IOException ex) {
        // JSON 문자열 변환으로 잘못된 UTF-8이 유실되지 않게 byte[]를 Base64로 기록한다.
        String line = mapper.writeValueAsString(new Failure(
            record.topic(), record.partition(), record.offset(), "kafka-json",
            "INVALID_JSON_OBJECT", record.key(), record.value()));
        out.collect(Either.Left(Tuple2.of(record.topic(), line)));
        return;
      }
      out.collect(Either.Right(decoded));
    }

    /** 정상과 실패 양쪽을 Flink가 각각 직렬화할 수 있게 타입을 선언한다. */
    @Override
    public TypeInformation<Either<Tuple2<String, String>, TopicRecord>> getProducedType() {
      return new EitherTypeInfo<>(ERROR_TYPE, json.getProducedType());
    }
  }

  /** Kafka와 텍스트 파일이 공유하는 오류 한 줄의 형식. */
  record Failure(
      String topic, int partition, long offset, String stage, String error,
      byte[] keyBase64, byte[] valueBase64
  ) {}

  /** 실패 레코드를 정상 DB 처리 경로에서 분리한다. */
  private static final class Split
      extends ProcessFunction<Either<Tuple2<String, String>, TopicRecord>, TopicRecord> {
    /** 한 레코드는 정상 출력과 DLQ 중 한쪽으로만 보낸다. */
    @Override
    public void processElement(
        Either<Tuple2<String, String>, TopicRecord> item, Context context, Collector<TopicRecord> out
    ) {
      if (item.isLeft()) {
        context.output(ERRORS, item.left());
      } else {
        out.collect(item.right());
      }
    }
  }

  /** TaskManager 디렉터리에서 토픽별 또는 지정된 UTF-8 텍스트 파일에 이어 쓴다. */
  static final class TextSink extends RichSinkFunction<Tuple2<String, String>> {
    private final String directory;
    private final String fileName;
    private transient Path folder;

    /** 경로는 제출 머신이 아니라 실제로 쓰는 TaskManager에서 해석한다. */
    TextSink(DlqConfig dlq) {
      directory = dlq.directory();
      String configured = dlq.fileName();
      fileName = configured == null || configured.endsWith(SUFFIX) ? configured : configured + SUFFIX;
    }

    /** 행이 들어온 토픽의 파일만 만들거나 열어 한 줄씩 추가한다. */
    @Override
    public void invoke(Tuple2<String, String> error, Context context) throws IOException {
      if (folder == null) {
        folder = Files.createDirectories(Path.of(directory));
      }
      // shortcut: 로컬 파일이라 체크포인트에 묶이지 않는다. 중앙 보관은 Kafka 모드를 쓴다.
      Files.writeString(folder.resolve(fileName == null ? error.f0 + SUFFIX : fileName),
          error.f1 + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }
}
