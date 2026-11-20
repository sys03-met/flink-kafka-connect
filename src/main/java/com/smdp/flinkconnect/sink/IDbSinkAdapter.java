package com.smdp.flinkconnect.sink;

import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * DB별 Sink 부착 약속 (인터페이스 — 파일명 {@code I*}).
 *
 * 구현
 * {@code sink/postgres|mssql|doris/*SinkAdapter}. 어떤 DB에 어떤 구현을 쓸지는 {@code PipelineJob.adapterFor} 가 정한다.
 *
 * 기본은 각 DB의 Flink 공식 커넥터로 넣는다 (Postgres/MSSQL = 공식 JdbcSink, Doris = 공식 DorisSink).
 * {@code sink: jdbc} 로 지정된 레코드만 우리 JDBC Sink 로 넣는다.
 */
public interface IDbSinkAdapter {

  /**
   * 이 DB로 가는 WriteRecord 에 Sink를 붙인다.
   *
   * @param writes 전체 스트림 (구현체가 db filter)
   * @param endpoint 이 DB JDBC/Doris 접속 ({@link JdbcEndpointRecord})
   * @param batchSize 배치 크기
   * @param planned 이 DB로 가는 Destination 전부 (구독 토픽을 미리 resolve 한 것). 그래프를 그릴 재료
   */
  void attach(
      DataStream<WriteRecord> writes,
      JdbcEndpointRecord endpoint,
      int batchSize,
      List<DestinationConfig> planned
  );
}
