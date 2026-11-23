package com.smdp.flinkconnect.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 파이프라인 설정 객체들 (YAML 에 적는 값 전부).
 *
 * YAML에 대응하는 설정 객체들을 한 파일에 모아 두었다.
 * 로더는 {@link YamlPipelineConfigLoader}.
 *
 * 병합 우선순위: defaults < topicPatterns(순서) < topics(정확 일치).
 *
 * Flink 연산자 스냅샷용으로 설정 트리는 {@link Serializable}.
 * 설정 코드는 적재(sink) 코드를 모른다. DB 이름 → 어댑터 짝짓기는 {@code PipelineJob} 한 곳.
 */
public final class PipelineModels {

  private PipelineModels() {}

  /**
   * 지원 DB 종류 (YAML {@code db: postgres}). 이름만 있고 구현은 모른다.
   *
   * DB 추가: 상수 하나 + {@code sink/<이름>/…SinkAdapter} + {@code PipelineJob.adapterFor} switch 한 줄
   * (switch 를 빠뜨리면 컴파일 오류로 걸린다).
   */
  public enum DbType {
    POSTGRES,
    MSSQL,
    DORIS
  }

  /**
   * DB 접속 한 줄 (YAML {@code databases.postgres|mssql|doris} 또는 환경변수 병합 결과).
   *
   * @param streamLoadUrl Doris 전용. {@code http://host:8040/api/.../{table}/_stream_load}.
   *     해석은 {@code DorisSinkAdapter} 가 한다. null 이면 127.0.0.1 기본
   */
  public record JdbcEndpointRecord(String jdbcUrl, String user, String password, String streamLoadUrl)
      implements Serializable {
    private static final long serialVersionUID = 1L;

    /** streamLoadUrl 없는 Postgres/MSSQL용. */
    public JdbcEndpointRecord(String jdbcUrl, String user, String password) {
      this(jdbcUrl, user, password, null);
    }
  }

  /**
   * DB에 넣는 방식. DB마다 실제 동작은 각 어댑터가 정한다.
   *
   *   - INSERT — 그냥 넣기
   *   - UPSERT — 키가 있으면 갱신. Postgres는 ON CONFLICT, MSSQL은 MERGE, Doris는 Unique Key 테이블의 Stream Load
   *   - BULK — 대량. Postgres는 COPY, MSSQL은 SQLServerBulkCopy, Doris는 Stream Load
   */
  public enum WriteMode {
    INSERT,
    UPSERT,
    BULK
  }

  /**
   * 어느 코드로 DB에 넣을지 (YAML {@code sink:}).
   *
   *   - {@link #OFFICIAL} (기본) — Flink 공식 커넥터. Postgres/MSSQL은 공식 JdbcSink, Doris는 공식 DorisSink.
   *     공식 JdbcSink는 SQL 한 문장을 미리 정해야 해서, Job 시작 때 대상 테이블 컬럼을 DB에서 읽는다.
   *   - {@link #JDBC} — 우리가 만든 JDBC 코드({@code DynamicTableJdbcSink}). 메시지마다 칸 구성이 달라도 된다.
   *     토픽명을 직접 적은 {@code topics:} 항목에서는 못 쓰고, {@code defaults}/{@code topicPatterns}에서만 쓴다.
   *     Doris에는 쓸 수 없다.
   */
  public enum SinkMode {
    OFFICIAL,
    JDBC
  }

  /**
   * YAML column 규칙 한 줄.
   *
   * {@code field}: 주 연산/결과 컬럼. {@code params}: 플러그인 자유 인자 맵.
   * op별: rename→to, plugin→expression(클래스명)+params.
   */
  public record ColumnRule(
      String field,
      String op,
      String to,
      String type,
      String format,
      String value,
      String expression,
      Map<String, Object> params
  ) implements Serializable {
    private static final long serialVersionUID = 1L;

    public ColumnRule {
      // Flink Kryo: Map.of() 불변맵은 역직렬화 시 put 실패
      params = params == null ? new java.util.LinkedHashMap<>() : new java.util.LinkedHashMap<>(params);
    }

    /** params 없는 기존 호출용. */
    public ColumnRule(
        String field,
        String op,
        String to,
        String type,
        String format,
        String value,
        String expression
    ) {
      this(field, op, to, type, format, value, expression, Map.of());
    }

    /** 플러그인 파라미터 조회. 없으면 null. */
    public Object param(String key) {
      return params.get(key);
    }

    public String paramString(String key, String fallback) {
      Object v = params.get(key);
      return v == null ? fallback : String.valueOf(v);
    }

    public double paramDouble(String key, double fallback) {
      Object v = params.get(key);
      if (v == null) {
        return fallback;
      }
      if (v instanceof Number n) {
        return n.doubleValue();
      }
      return Double.parseDouble(String.valueOf(v));
    }
  }

  /**
   * 복원 상태가 없을 때 Kafka를 어디서 읽기 시작할지 (YAML {@code kafka.startingOffsets}).
   *
   * 체크포인트가 있으면 그 지점이 이 값보다 우선한다.
   */
  public enum KafkaStartingOffset {
    /** 그룹에 기록된 오프셋. 기록이 없으면 맨 끝. 하루치 재적재를 피하려는 기본값. */
    COMMITTED,
    /** 기록과 상관없이 맨 끝. 밀린 데이터를 버리고 지금만 받을 때. */
    LATEST,
    /** 보관 중인 맨 앞. 과거 재적재용. 대량 토픽은 적재 부하가 그대로 돌아온다. */
    EARLIEST
  }

  public record KafkaConfig(
      String bootstrapServers,
      String groupId,
      List<String> subscribe,
      KafkaStartingOffset startingOffset
  ) implements Serializable {
    private static final long serialVersionUID = 1L;

    public KafkaConfig {
      if (subscribe == null) {
        subscribe = List.of();
      }
      if (startingOffset == null) {
        startingOffset = KafkaStartingOffset.COMMITTED;
      }
    }

    /** 시작 위치를 생략하는 기존 호출. 기본은 그룹 오프셋이다. */
    public KafkaConfig(String bootstrapServers, String groupId, List<String> subscribe) {
      this(bootstrapServers, groupId, subscribe, KafkaStartingOffset.COMMITTED);
    }
  }

  /**
   * 갈 곳 한 줄 (Destination).
   *
   * 두 군데서 쓴다.
   *
   *   - YAML 원본: table/writeMode 가 비어 있을 수 있다
   *   - Resolver 가 풀어낸 값: table/writeMode 가 채워져 {@code WriteRecord} 에 실려 Sink 로 간다
   *     (이때 columns 는 이미 행에 반영돼 비운다)
   *
   * 리스트는 가변 {@link ArrayList} 복사본. Flink Kryo 는 불변 리스트를 역직렬화하다 깨진다.
   *
   * @param primaryKeys UPSERT용 키. 비면 Job 시작/첫 적재 때 DB 테이블의 PK를 읽어 쓴다. 그래도 없으면 실패.
   * @param sink 어느 코드로 넣을지. null이면 {@link SinkMode#OFFICIAL}.
   */
  public record DestinationConfig(
      DbType db,
      String table,
      WriteMode writeMode,
      List<ColumnRule> columns,
      List<String> primaryKeys,
      SinkMode sink
  ) implements Serializable {
    private static final long serialVersionUID = 1L;

    public DestinationConfig {
      columns = columns == null ? new ArrayList<>() : new ArrayList<>(columns);
      primaryKeys = primaryKeys == null ? new ArrayList<>() : new ArrayList<>(primaryKeys);
      if (sink == null) {
        sink = SinkMode.OFFICIAL;
      }
    }

    /** PK 없음, sink 기본 OFFICIAL. */
    public DestinationConfig(DbType db, String table, WriteMode writeMode, List<ColumnRule> columns) {
      this(db, table, writeMode, columns, List.of(), SinkMode.OFFICIAL);
    }

    /** sink 기본 OFFICIAL. */
    public DestinationConfig(
        DbType db, String table, WriteMode writeMode, List<ColumnRule> columns, List<String> primaryKeys
    ) {
      this(db, table, writeMode, columns, primaryKeys, SinkMode.OFFICIAL);
    }
  }

  public record DefaultsConfig(List<DestinationConfig> destinations, String tableFromTopic)
      implements Serializable {
    private static final long serialVersionUID = 1L;

    public DefaultsConfig {
      if (destinations == null) {
        destinations = List.of();
      }
    }
  }

  public record TopicPatternConfig(
      String pattern, String table, List<DestinationConfig> destinations)
      implements Serializable {
    private static final long serialVersionUID = 1L;

    public TopicPatternConfig {
      if (destinations == null) {
        destinations = List.of();
      }
    }
  }

  public record TopicConfig(String table, List<DestinationConfig> destinations)
      implements Serializable {
    private static final long serialVersionUID = 1L;

    public TopicConfig {
      if (destinations == null) {
        destinations = List.of();
      }
    }
  }

  /** 실패 원문 저장 대상. 설정을 생략하면 DLQ 자체를 붙이지 않는다. */
  public record DlqConfig(DlqType type, String topic, String directory, String fileName)
      implements Serializable {}

  /** YAML에서 선택하는 두 가지 실패 원문 저장 방식. */
  public enum DlqType {
    KAFKA,
    FILE
  }

  /** 한 번 제출하는 Flink Job 하나의 이름과 입력·출력 설정. */
  public record PipelineConfig(
      KafkaConfig kafka,
      DefaultsConfig defaults,
      List<TopicPatternConfig> topicPatterns,
      Map<String, TopicConfig> topics,
      Map<DbType, JdbcEndpointRecord> databases,
      String jobName,
      DlqConfig dlq
  ) implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 이전 YAML과 호출자도 기존 이름과 기본값으로 실행되게 한다. */
    public PipelineConfig {
      jobName = jobName == null || jobName.isBlank() ? "flink-connect" : jobName.trim();
      if (kafka == null) {
        kafka = new KafkaConfig("localhost:9092", "flink-connect", List.of());
      }
      if (defaults == null) {
        defaults = new DefaultsConfig(List.of(), "hv\\.j\\.dproc\\.(.+)");
      }
      if (topicPatterns == null) {
        topicPatterns = List.of();
      }
      if (topics == null) {
        topics = Map.of();
      }
      if (databases == null) {
        databases = Map.of();
      }
    }

    /** DB 정보를 넘기던 기존 호출은 이름과 DLQ 기본값을 유지한다. */
    public PipelineConfig(
        KafkaConfig kafka,
        DefaultsConfig defaults,
        List<TopicPatternConfig> topicPatterns,
        Map<String, TopicConfig> topics,
        Map<DbType, JdbcEndpointRecord> databases
    ) {
      this(kafka, defaults, topicPatterns, topics, databases, null, null);
    }

    /** DB 정보도 생략하는 기존 호출용. */
    public PipelineConfig(
        KafkaConfig kafka,
        DefaultsConfig defaults,
        List<TopicPatternConfig> topicPatterns,
        Map<String, TopicConfig> topics
    ) {
      this(kafka, defaults, topicPatterns, topics, Map.of());
    }
  }
}
