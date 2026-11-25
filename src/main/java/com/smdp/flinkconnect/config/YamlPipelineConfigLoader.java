package com.smdp.flinkconnect.config;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DefaultsConfig;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.DlqConfig;
import com.smdp.flinkconnect.config.PipelineModels.DlqType;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.KafkaConfig;
import com.smdp.flinkconnect.config.PipelineModels.KafkaStartingOffset;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.SinkMode;
import com.smdp.flinkconnect.config.PipelineModels.TopicConfig;
import com.smdp.flinkconnect.config.PipelineModels.TopicPatternConfig;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.subscribe.TopicSubscriptions;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.yaml.snakeyaml.Yaml;

/**
 * 파이프라인 YAML → {@link PipelineConfig} 로더.
 *
 * 용도: Job 기동 시 설정 파일을 DTO로 읽어 Resolver / Fan-out / Sink가 쓰게 한다.
 *
 * 하는 일:
 *
 *   - SnakeYAML로 Map 파싱 (스키마 검증기 없음 — 필드 누락은 기본값)
 *   - jobName / kafka / dlq / defaults / topicPatterns / topics / databases 섹션 조립
 *   - Destination·ColumnRule·primaryKeys 하위 파싱
   *   - {@code sink: jdbc} 를 토픽명 명시 항목에 쓰면 로드 때 바로 실패
   *   - 시스템별 거절(Doris JDBC, Kafka 토픽명 등)은 각 Sink의 check가 한다
 *   - 모르는 column op는 Resolver 생성 때 실패 (op 목록은 ColumnOpHandlers 한곳)
 *   - {@link #resolve}: YAML + 환경변수 → 최종 접속 정보·배치 크기
 *
 *
 * 전제: DTO는 {@link PipelineModels}.
 * Usage:
 * {@code PipelineConfig c = new YamlPipelineConfigLoader().load(path);}
 * {@code Resolved s = YamlPipelineConfigLoader.resolve(c, System.getenv());}
 */
public final class YamlPipelineConfigLoader {

  /** {@link #resolve} 결과. Job 이 Sink 를 붙일 때 쓴다. */
  public record Resolved(Map<SystemType, JdbcEndpointRecord> endpoints, int writeBatchSize) {
    public Resolved {
      endpoints = endpoints != null ? Map.copyOf(endpoints) : Map.of();
    }
  }

  /**
   * YAML과 환경변수를 합친 최종 설정.
   *
   *   - {@code databases:}에 있는 DB는 YAML 값 유지 (env로 덮지 않음)
   *   - 없는 DB만 {@code PG_*}/{@code MSSQL_*}/{@code DORIS_*}로 채움 (URL 없으면 그 DB Sink 안 붙임)
   *   - {@code WRITE_BATCH_SIZE} 기본 100
   */
  public static Resolved resolve(PipelineConfig config, Map<String, String> env) {
    Map<String, String> e = env != null ? env : Map.of();
    Map<SystemType, JdbcEndpointRecord> endpoints = new EnumMap<>(SystemType.class);
    // YAML databases 가 우선 — 이미 있으면 env로 덮지 않음
    endpoints.putAll(config.databases());
    // 환경변수 키는 시스템 상수에 있다. Kafka처럼 키가 없으면 건너뛴다.
    for (SystemType system : SystemType.values()) {
      if (system.jdbc()) {
        putIfAbsent(endpoints, system, e, system.envUrl(), system.envUser(), system.envPassword());
      }
    }
    String batch = e.get("WRITE_BATCH_SIZE");
    return new Resolved(endpoints, batch == null || batch.isBlank() ? 100 : Integer.parseInt(batch));
  }

  /** YAML에 해당 DB가 없을 때만 env URL로 endpoint 추가. URL 없으면 건너뜀 (부분 배포 허용). */
  private static void putIfAbsent(
      Map<SystemType, JdbcEndpointRecord> map,
      SystemType db,
      Map<String, String> env,
      String urlKey,
      String userKey,
      String passKey
  ) {
    String url = env.get(urlKey);
    if (map.containsKey(db) || url == null || url.isBlank()) {
      return;
    }
    map.put(db, new JdbcEndpointRecord(url, env.getOrDefault(userKey, ""), env.getOrDefault(passKey, "")));
  }

  /**
   * YAML 파일을 {@link PipelineConfig}로 로드한다.
   *
   * 빈/null 문서는 기본 PipelineConfig. 섹션 누락은 빈 목록·기본 문자열로 채운다.
   */
  @SuppressWarnings("unchecked")
  public PipelineConfig load(Path path) throws Exception {
    try (InputStream in = Files.newInputStream(path)) {
      Map<String, Object> root = new Yaml().load(in);
      if (root == null) {
        return new PipelineConfig(null, null, List.of(), Map.of());
      }

      // --- kafka: 브로커·그룹·구독 glob ---
      Map<String, Object> kafkaMap = (Map<String, Object>) root.getOrDefault("kafka", Map.of());
      List<String> subscribe = new ArrayList<>();
      Object sub = kafkaMap.get("subscribe");
      if (sub instanceof List<?> list) {
        for (Object item : list) {
          subscribe.add(String.valueOf(item));
        }
      }
      KafkaConfig kafka =
          new KafkaConfig(
              stringOr(kafkaMap.get("bootstrapServers"), "localhost:9092"),
              stringOr(kafkaMap.get("groupId"), "flink-connect"),
              subscribe,
              parseStartingOffset(kafkaMap.get("startingOffsets")));

      // --- defaults: 전역 destinations + 토픽에서 target을 뽑는 정규식 ---
      Map<String, Object> defaultsMap =
          (Map<String, Object>) root.getOrDefault("defaults", Map.of());
      rejectRetiredTargetKeys(defaultsMap);
      DefaultsConfig defaults =
          new DefaultsConfig(
              parseDestinations(defaultsMap.get("destinations"), true, subscribe),
              // 고압 dproc 관례. Resolver가 캡처 그룹을 대문자 target으로 쓴다.
              stringOr(defaultsMap.get("targetFromTopic"), "hv\\.j\\.dproc\\.(.+)"));

      // --- topicPatterns: 선언 순서 유지 (나중 매칭이 이김) ---
      List<TopicPatternConfig> patterns = new ArrayList<>();
      List<Map<String, Object>> patternList =
          (List<Map<String, Object>>) root.getOrDefault("topicPatterns", List.of());
      for (Map<String, Object> p : patternList) {
        rejectRetiredTargetKeys(p);
        patterns.add(
            new TopicPatternConfig(
                stringOr(p.get("pattern"), null),
                stringOr(p.get("target"), null),
                parseDestinations(p.get("destinations"), true, subscribe)));
      }

      // --- topics: 정확 일치 키가 최종 승자. LinkedHashMap으로 선언 순서 보존 ---
      Map<String, TopicConfig> topics = new LinkedHashMap<>();
      Map<String, Object> topicsMap = (Map<String, Object>) root.getOrDefault("topics", Map.of());
      for (Map.Entry<String, Object> e : topicsMap.entrySet()) {
        Map<String, Object> t = (Map<String, Object>) e.getValue();
        rejectRetiredTargetKeys(t);
        topics.put(
            e.getKey(),
            // 토픽명을 직접 적은 항목은 공식 커넥터만 허용 (sink: jdbc 거절)
            new TopicConfig(
                stringOr(t.get("target"), null), parseDestinations(t.get("destinations"), false, subscribe)));
      }

      // --- databases: YAML JDBC. 없으면 resolve 가 env로 보충 ---
      Map<SystemType, JdbcEndpointRecord> databases = parseDatabases(root.get("databases"));
      return new PipelineConfig(
          kafka, defaults, patterns, topics, databases,
          stringOr(root.get("jobName"), null), parseDlq(root.get("dlq"), subscribe));
    }
  }

  /** 선택 DLQ를 해석하고, 직접 적은 이름이 잘못되거나 입력과 겹치면 제출 전에 막는다. */
  private static DlqConfig parseDlq(Object raw, List<String> subscribe) {
    if (raw == null) {
      return null;
    }
    if (!(raw instanceof Map<?, ?> map)) {
      throw new IllegalArgumentException("dlq는 type이 있는 YAML 맵이어야 합니다.");
    }
    return switch (stringOr(map.get("type"), "").trim().toLowerCase(Locale.ROOT)) {
      case "kafka" -> {
        String topic = stringOr(map.get("topic"), null);
        if (topic != null) {
          topic = topic.trim();
          if (!topic.matches("[a-zA-Z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("dlq.topic이 유효한 Kafka 토픽명이 아닙니다.");
          }
          // 아직 존재하지 않는 DLQ라도 입력 패턴과 겹치면 다음 제출 때 재구독할 수 있다.
          if (TopicSubscriptions.expand(subscribe, Set.of(topic)).contains(topic)) {
            throw new IllegalArgumentException("dlq.topic이 kafka.subscribe와 겹칩니다: " + topic);
          }
        }
        yield new DlqConfig(DlqType.KAFKA, topic, null, null);
      }
      case "file" -> {
        String directory = stringOr(map.get("directory"), ".error").trim();
        if (directory.isEmpty()) {
          throw new IllegalArgumentException("dlq.directory는 빈 경로일 수 없습니다.");
        }
        // 경로 이동은 directory로만 지정하고 fileName은 단일 파일 이름만 받는다.
        String fileName = stringOr(map.get("fileName"), null);
        if (fileName != null
            && (!fileName.matches("[a-zA-Z0-9._-]{1,249}")
                || fileName.equals(".") || fileName.equals(".."))) {
          throw new IllegalArgumentException("dlq.fileName은 디렉터리 없는 파일명이어야 합니다.");
        }
        yield new DlqConfig(DlqType.FILE, null, directory, fileName);
      }
      default -> throw new IllegalArgumentException("dlq.type은 kafka 또는 file이어야 합니다.");
    };
  }

  /**
   * {@code databases:} 맵 → 시스템별 JDBC 엔드포인트.
   *
   * 키는 대소문자 무시({@code postgres} → POSTGRES).
   * Doris Stream Load용 {@code streamLoadUrl}은 선택(없으면 null).
   * Kafka는 브로커 주소를 {@code kafka.bootstrapServers}에서 받으므로 이 맵에 넣지 않는다.
   */
  @SuppressWarnings("unchecked")
  private static Map<SystemType, JdbcEndpointRecord> parseDatabases(Object raw) {
    if (!(raw instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<SystemType, JdbcEndpointRecord> result = new EnumMap<>(SystemType.class);
    for (Map.Entry<?, ?> e : map.entrySet()) {
      SystemType db = SystemType.valueOf(String.valueOf(e.getKey()).toUpperCase(Locale.ROOT));
      if (!db.jdbc()) {
        throw new IllegalArgumentException(
            db + " 는 databases에 적지 않습니다. 접속은 그 시스템의 설정을 씁니다.");
      }
      Map<String, Object> m = (Map<String, Object>) e.getValue();
      result.put(
          db,
          new JdbcEndpointRecord(
              stringOr(m.get("jdbcUrl"), ""),
              stringOr(m.get("user"), ""),
              stringOr(m.get("password"), ""),
              stringOr(m.get("streamLoadUrl"), null)));
    }
    return result;
  }

  /** 예전 table 키를 조용히 버리면 target이 비어 정규식으로 채워진다. 제출 전에 실패시킨다. */
  private static void rejectRetiredTargetKeys(Map<String, Object> map) {
    if (map.containsKey("table") || map.containsKey("tableFromTopic")) {
      throw new IllegalArgumentException(
          "target / targetFromTopic 을 씁니다. table / tableFromTopic 은 더 이상 읽지 않습니다.");
    }
  }

  /**
   * Destination 리스트 파싱 (defaults / patterns / topics 공통).
   *
   * writeMode 기본 INSERT. sink 기본 OFFICIAL(공식 커넥터).
   *
   * @param allowJdbc {@code sink: jdbc} 허용 여부. 토픽명을 직접 적은 {@code topics:}는 false.
   */
  @SuppressWarnings("unchecked")
  private static List<DestinationConfig> parseDestinations(
      Object raw, boolean allowJdbc, List<String> subscribe
  ) {
    if (!(raw instanceof List<?> list)) {
      return List.of();
    }
    List<DestinationConfig> result = new ArrayList<>();
    for (Object item : list) {
      Map<String, Object> m = (Map<String, Object>) item;
      // 예전 키를 조용히 무시하면 postgres로 떨어진다. 제출 전에 이름을 짚어 실패시킨다.
      if (m.containsKey("db") || m.containsKey("table") || m.containsKey("name")) {
        throw new IllegalArgumentException(
            "목적지는 system 과 target 을 씁니다. db / table / name 은 더 이상 읽지 않습니다.");
      }
      SystemType system =
          SystemType.valueOf(stringOr(m.get("system"), "POSTGRES").toUpperCase(Locale.ROOT));
      WriteMode mode =
          WriteMode.valueOf(stringOr(m.get("writeMode"), "INSERT").toUpperCase(Locale.ROOT));
      SinkMode sink = parseSinkMode(stringOr(m.get("sink"), "official"));
      // 우리 JDBC 코드는 토픽명을 안 적은 패턴/기본 항목에서만. 정확 토픽은 공식 커넥터로 고정한다.
      if (sink == SinkMode.JDBC && !allowJdbc) {
        throw new IllegalArgumentException(
            "sink: jdbc 는 defaults / topicPatterns 에서만 쓸 수 있습니다. "
                + "topics: (토픽명 명시) 항목은 공식 Sink만 씁니다. system=" + system);
      }
      DestinationConfig dest =
          new DestinationConfig(
              system,
              stringOr(m.get("target"), null),
              mode,
              parseColumns(m.get("columns")),
              parseStringList(m.get("primaryKeys")),
              sink);
      // 시스템별 거절은 어댑터에 있다. 로더는 종류를 나열하지 않는다.
      system.newSink().check(dest, subscribe, allowJdbc);
      result.add(dest);
    }
    return result;
  }

  /**
   * YAML 문자열 리스트 (주로 {@code primaryKeys: [METER_ID]}).
   *
   * 리스트가 아니면 빈 목록 — null/누락과 동일 취급.
   */
  private static List<String> parseStringList(Object raw) {
    if (!(raw instanceof List<?> list)) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (Object item : list) {
      if (item != null) {
        out.add(String.valueOf(item));
      }
    }
    return List.copyOf(out);
  }

  /**
   * YAML {@code sink:} 값 → {@link SinkMode}.
   *
   * 예전 값 {@code dynamic} 은 {@code jdbc} 로 바뀌었다. 조용히 다른 길로 가지 않게 이름을 짚어 실패시킨다.
   */
  /**
   * YAML {@code kafka.startingOffsets}. 생략·빈 값은 committed.
   * 오타는 earliest로 떨어지면 대량 토픽을 처음부터 읽으므로 바로 실패시킨다.
   */
  private static KafkaStartingOffset parseStartingOffset(Object raw) {
    if (raw == null || String.valueOf(raw).isBlank()) {
      return KafkaStartingOffset.COMMITTED;
    }
    return switch (String.valueOf(raw).trim().toLowerCase(Locale.ROOT)) {
      case "committed" -> KafkaStartingOffset.COMMITTED;
      case "latest" -> KafkaStartingOffset.LATEST;
      case "earliest" -> KafkaStartingOffset.EARLIEST;
      default ->
          throw new IllegalArgumentException(
              "kafka.startingOffsets: " + raw + " 는 모르는 값입니다. committed(기본), latest, earliest 만 씁니다.");
    };
  }

  private static SinkMode parseSinkMode(String raw) {
    String v = raw.trim().toLowerCase(Locale.ROOT);
    return switch (v) {
      case "official" -> SinkMode.OFFICIAL;
      case "jdbc" -> SinkMode.JDBC;
      default ->
          throw new IllegalArgumentException(
              "sink: " + raw + " 는 모르는 값입니다. official(기본) 또는 jdbc 만 씁니다.");
    };
  }

  /**
   * ColumnRule 목록 파싱.
   *
   * {@code field} + 선택 {@code params}. op 이름 검사는 op 목록을 가진
   * {@code ColumnOpHandlers}가 Resolver 생성 때 한다 (Job 시작 전).
   */
  @SuppressWarnings("unchecked")
  private static List<ColumnRule> parseColumns(Object raw) {
    if (!(raw instanceof List<?> list)) {
      return List.of();
    }
    List<ColumnRule> rules = new ArrayList<>();
    for (Object item : list) {
      Map<String, Object> m = (Map<String, Object>) item;
      String op = stringOr(m.get("op"), null);
      rules.add(
          new ColumnRule(
              stringOr(m.get("field"), null),
              op,
              stringOr(m.get("to"), null),
              stringOr(m.get("type"), null),
              stringOr(m.get("format"), null),
              stringOr(m.get("value"), null),
              stringOr(m.get("expression"), null),
              parseParams(m.get("params"))));
    }
    return rules;
  }

  /** YAML params: { key: value, ... } → 플러그인 인자. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> parseParams(Object raw) {
    if (!(raw instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> e : map.entrySet()) {
      if (e.getKey() != null) {
        out.put(String.valueOf(e.getKey()), e.getValue());
      }
    }
    return Map.copyOf(out);
  }

  /** null이면 fallback, 아니면 문자열화. YAML 숫자·불리언도 String으로 넣는다. */
  private static String stringOr(Object value, String fallback) {
    return value == null ? fallback : String.valueOf(value);
  }
}
