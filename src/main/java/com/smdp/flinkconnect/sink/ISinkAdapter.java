package com.smdp.flinkconnect.sink;

import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 시스템별 Sink 부착 약속.
 *
 * 구현은 {@code sink/postgres|mssql|doris|kafka/*SinkAdapter}.
 * 어떤 시스템에 어떤 구현을 쓸지는 {@link com.smdp.flinkconnect.config.PipelineModels.SystemType} 상수에 적는다.
 *
 * 새 시스템은 어댑터를 만들고 상수 한 줄을 추가하면 된다.
 * 이름 채우기와 거절 규칙도 여기 기본 구현을 덮어 쓴다. 로더와 Resolver는 시스템을 구분하지 않는다.
 */
public interface ISinkAdapter {

  /**
   * 이 시스템으로 가는 WriteRecord에 Sink를 붙인다.
   *
   * @param writes 전체 스트림 (구현체가 자기 시스템만 고른다)
   * @param endpoint JDBC 접속. {@code jdbc()} 가 아닌 시스템(Kafka)은 null
   * @param batchSize 한 번에 모을 행 수. 차지 않아도 batchIntervalMs가 지나면 보낸다
   * @param batchIntervalMs 건수가 안 차도 버퍼를 내보내는 간격(밀리초)
   * @param planned 이 시스템으로 가는 Destination 전부
   * @param config 같은 Job의 Kafka 브로커·구독. Kafka Sink가 출력에 쓴다
   */
  void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      long batchIntervalMs,
      List<DestinationConfig> planned,
      PipelineConfig config
  );

  /**
   * 목적지에 target이 없을 때 쓸 값.
   *
   * 기본은 YAML target이 있으면 그대로, 없으면 토픽에서 유도한 테이블명.
   * Kafka처럼 유도하면 안 되는 시스템은 덮어 쓴다.
   */
  default String resolveName(String explicit, String derived) {
    return explicit != null && !explicit.isBlank() ? explicit : derived;
  }

  /**
   * 이 시스템이 받을 수 없는 목적지 설정을 거절한다. 로드와 resolve 때 호출된다.
   *
   * @param subscribe 입력 구독. 출력 토픽이 여기 다시 걸리면 자기가 쓴 메시지를 다시 읽는다
   * @param allowJdbc 토픽명을 직접 적은 항목은 false. 기본 구현은 쓰지 않고, 로더가 먼저 거절한다
   */
  default void check(DestinationConfig dest, List<String> subscribe, boolean allowJdbc) {}
}
