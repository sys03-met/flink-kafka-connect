package com.smdp.flinkconnect.sink.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import com.smdp.flinkconnect.sink.ISinkAdapter;
import com.smdp.flinkconnect.subscribe.TopicSubscriptions;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka용 {@link ISinkAdapter}. 컬럼 변환이 끝난 행을 같은 브로커의 다른 토픽에 JSON으로 넣는다.
 *
 * 연결
 * {@link SystemType#KAFKA} → {@code newSink()} 가 이 클래스를 만든다.
 *
 * 하는 일
 *
 *   - {@code target}은 출력 토픽. 비우거나 테이블명으로 유도하지 않는다
 *   - {@code writeMode}, {@code sink: jdbc}, {@code op: sql} 은 토픽에 넣을 자리가 없어 거절
 *   - 출력 토픽이 {@code kafka.subscribe}에 다시 걸리면 시작 전에 거절 (자기가 쓴 메시지를 다시 읽음)
 *   - 체크포인트와 맞춰 at-least-once. 재시작 때 같은 JSON이 두 번 나갈 수 있다
 */
public final class KafkaSinkAdapter implements ISinkAdapter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** 테이블명으로 바꾸지 않는다. 출력 토픽은 YAML target 그대로다. */
  @Override
  public String resolveName(String explicit, String derived) {
    if (explicit == null || explicit.isBlank()) {
      throw new IllegalArgumentException("system: kafka 는 target(출력 토픽)이 필요합니다.");
    }
    return explicit;
  }

  /** 토픽으로 보낼 수 없는 설정과, 입력으로 다시 들어오는 출력 토픽을 막는다. */
  @Override
  public void check(DestinationConfig dest, List<String> subscribe, boolean allowJdbc) {
    if (dest.sink() == SinkMode.JDBC) {
      throw new IllegalArgumentException("system: kafka 에는 sink: jdbc 를 쓸 수 없습니다.");
    }
    if (dest.writeMode() != null && dest.writeMode() != WriteMode.INSERT) {
      throw new IllegalArgumentException("system: kafka 에는 writeMode를 쓸 수 없습니다.");
    }
    if (dest.columns().stream().anyMatch(c -> c.op() != null && c.op().equalsIgnoreCase("sql"))) {
      throw new IllegalArgumentException("system: kafka 에는 op: sql 을 쓸 수 없습니다.");
    }
    String target = dest.target();
    if (target == null || target.isBlank()) {
      throw new IllegalArgumentException("system: kafka 는 target(출력 토픽)이 필요합니다.");
    }
    if (!target.matches("[a-zA-Z0-9._-]{1,249}") || target.equals(".") || target.equals("..")) {
      throw new IllegalArgumentException("kafka target이 유효한 토픽명이 아닙니다: " + target);
    }
    // 구독 glob에 출력 토픽이 걸리면 방금 쓴 JSON을 다음 입력으로 다시 읽는다.
    if (subscribe != null && TopicSubscriptions.expand(subscribe, Set.of(target)).contains(target)) {
      throw new IllegalArgumentException("kafka 출력 토픽이 kafka.subscribe와 겹칩니다: " + target);
    }
  }

  /** Kafka 행만 골라 같은 브로커로 보낸다. endpoint는 쓰지 않는다. */
  @Override
  public void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      PipelineConfig config
  ) {
    // 프로듀서가 자체 배치한다. DB용 건수·간격으로 행을 쥐고 있으면 토픽 지연만 는다.
    if (batchSize < 1 || batchIntervalMs < 1) {
      throw new IllegalArgumentException("배치 건수와 간격은 1 이상이어야 합니다.");
    }
    if (config == null) {
      throw new IllegalStateException("Kafka Sink에는 파이프라인 설정이 필요합니다.");
    }
    for (DestinationConfig dest : planned) {
      check(dest, config.kafka().subscribe(), true);
    }
    writes
        .filter((FilterFunction<WriteRecord>) r -> r.destination().system() == SystemType.KAFKA)
        .name("to-kafka")
        .sinkTo(
            KafkaSink.<WriteRecord>builder()
                .setBootstrapServers(config.kafka().bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer((req, context, timestamp) -> {
                  try {
                    return toRecord(req);
                  } catch (IOException ex) {
                    // 직렬화 스키마는 검사 예외를 넘기지 못한다. 깨진 행은 건너뛰지 않고 Job을 실패시킨다.
                    throw new IllegalStateException("Kafka JSON 직렬화 실패: " + req.destination().target(), ex);
                  }
                })
                .build())
        .name("kafka-sink");
  }

  /** 행 → 출력 토픽의 JSON 레코드. 키는 입력 디코더가 버리므로 싣지 않는다. */
  static ProducerRecord<byte[], byte[]> toRecord(WriteRecord req) throws IOException {
    return new ProducerRecord<>(req.destination().target(), MAPPER.writeValueAsBytes(jsonFields(req.row())));
  }

  /** {@code op: sql} 칸은 DB 식이라 토픽 JSON에 넣지 않는다. */
  private static Map<String, Object> jsonFields(WriteRowRecord row) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (String col : row.columns()) {
      if (!row.isSql(col)) {
        out.put(col, row.value(col));
      }
    }
    return out;
  }
}
