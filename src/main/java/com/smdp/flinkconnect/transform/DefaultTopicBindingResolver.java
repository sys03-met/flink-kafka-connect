package com.smdp.flinkconnect.transform;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import com.smdp.flinkconnect.config.PipelineModels.DefaultsConfig;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.PipelineModels.TopicConfig;
import com.smdp.flinkconnect.config.PipelineModels.TopicPatternConfig;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.sink.ISinkAdapter;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import com.smdp.flinkconnect.subscribe.TopicSubscriptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 변환 단계: 토픽 → 갈 곳 + 컬럼 손질.
 *
 *   - {@link #resolve}: defaults → topicPatterns → topics → Destination 목록 (토픽별 캐시)
 *   - {@link #expand}: 갈 곳마다 columns 적용 → {@link WriteRecord} (payload는 매번)
 *
 * 설정 오류(모르는 op, 대량/Doris + SQL 식)는 생성·resolve 때 바로 실패한다.
 * Job은 시작 전에 구독 토픽 전부를 resolve 하므로, 메시지가 오기 전에 걸린다.
 */
public final class DefaultTopicBindingResolver {

  private final PipelineConfig config;
  private final Pattern targetFromTopic;
  /** 토픽명 → 갈 곳. YAML·구독은 Job 수명 동안 안 바뀌므로 첫 resolve 후 재사용. */
  private final Map<String, List<DestinationConfig>> destByTopic = new ConcurrentHashMap<>();

  public DefaultTopicBindingResolver(PipelineConfig config) {
    this.config = config;
    String regex =
        config.defaults() != null && config.defaults().targetFromTopic() != null
            ? config.defaults().targetFromTopic()
            : "hv\\.j\\.dproc\\.(.+)";
    this.targetFromTopic = Pattern.compile(regex);
    requireKnownOps(config);
  }

  /** 토픽명 → 갈 곳 목록 (토픽당 한 번만 계산). table·writeMode 는 항상 채워져 있다. */
  public List<DestinationConfig> resolve(String topic) {
    return destByTopic.computeIfAbsent(topic, this::resolveUncached);
  }

  /** glob·YAML 병합. {@link #resolve} 캐시 미스일 때만. */
  private List<DestinationConfig> resolveUncached(String topic) {
    DefaultsConfig defaults = config.defaults();
    String target = null;
    // 우선순위 바닥: defaults.destinations (이후 패턴·정확 토픽이 덮어씀)
    List<DestinationConfig> destinations =
        defaults != null ? new ArrayList<>(defaults.destinations()) : new ArrayList<>();

    // 패턴은 여러 개 맞을 수 있음 → 나중에 맞은 쪽이 테이블·갈 곳을 덮어씀
    for (TopicPatternConfig patternConfig : config.topicPatterns()) {
      if (TopicSubscriptions.matches(patternConfig.pattern(), topic)) {
        if (patternConfig.target() != null && !patternConfig.target().isBlank()) {
          target = patternConfig.target();
        }
        if (!patternConfig.destinations().isEmpty()) {
          destinations = new ArrayList<>(patternConfig.destinations());
        }
      }
    }

    // 정확 토픽 키가 있으면 패턴보다 항상 우선 (운영이 rd_lp만 다르게 쓰는 경우)
    TopicConfig exact = config.topics().get(topic);
    if (exact != null) {
      if (exact.target() != null && !exact.target().isBlank()) {
        target = exact.target();
      }
      if (!exact.destinations().isEmpty()) {
        destinations = new ArrayList<>(exact.destinations());
      }
    }

    // YAML에 target이 없으면 토픽명에서 만든다
    String resolvedTarget = target != null ? target : deriveTarget(topic);
    List<DestinationConfig> bindings = new ArrayList<>();
    for (DestinationConfig dest : destinations) {
      // target 규칙은 시스템 어댑터에 있다. DB는 비우면 토픽에서 유도하고, Kafka는 target을 요구한다.
      ISinkAdapter sink = dest.system().newSink();
      String explicit = dest.target() != null && !dest.target().isBlank() ? dest.target() : null;
      String resolvedName = sink.resolveName(explicit, resolvedTarget);
      WriteMode mode = dest.writeMode() != null ? dest.writeMode() : WriteMode.INSERT;
      rejectBulkSql(topic, dest.system(), resolvedName, mode, dest.columns());
      DestinationConfig filled =
          new DestinationConfig(
              dest.system(), resolvedName, mode, dest.columns(), dest.primaryKeys(), dest.sink());
      sink.check(filled, List.of(), true);
      bindings.add(filled);
    }
    return List.copyOf(bindings);
  }

  /**
   * 대량 적재는 값만 보내는 통로라 {@code op: sql} 식을 넣을 자리가 없다.
   *
   * Doris·Kafka처럼 시스템 전체가 식을 거부하는 경우는 각 어댑터 {@code check}가 맡는다.
   * 거절하지 않으면 그 칸이 NULL로 들어가거나 JSON에서 빠진다.
   */
  private static void rejectBulkSql(
      String topic, SystemType system, String target, WriteMode mode, List<ColumnRule> columns
  ) {
    boolean hasSql = columns.stream().anyMatch(c -> "sql".equalsIgnoreCase(c.op()));
    if (!hasSql || mode != WriteMode.BULK) {
      return;
    }
    throw new IllegalStateException(
        "op: sql 컬럼은 대량(BULK)에 쓸 수 없습니다 (topic="
            + topic
            + ", system="
            + system
            + ", target="
            + target
            + ")");
  }

  /** 한 TopicRecord → WriteRecord N개 (갈 곳 + columns). */
  public List<WriteRecord> expand(TopicRecord record) {
    // 갈 곳·column 규칙은 캐시, payload 적용만 매 메시지
    List<DestinationConfig> destinations = resolve(record.topic());
    List<WriteRecord> out = new ArrayList<>(destinations.size());
    for (DestinationConfig binding : destinations) {
      // fan-out: 같은 메시지 → DB별 WriteRecord (Postgres+Doris 등)
      WriteRowRecord row = WriteRowRecord.of(ColumnOpHandlers.apply(record.payload(), binding.columns()));
      // Sink·Flink Kryo에는 컬럼 규칙이 필요 없음(이미 row에 반영). record ColumnRule 직렬화 회피.
      DestinationConfig sinkDest =
          new DestinationConfig(
              binding.system(),
              binding.target(),
              binding.writeMode(),
              List.of(),
              binding.primaryKeys(),
              binding.sink());
      out.add(new WriteRecord(sinkDest, row));
    }
    return new ArrayList<>(out);
  }

  /** 설정 안의 모든 컬럼 규칙 op가 아는 이름인지. 생성자에서 한 번. */
  private static void requireKnownOps(PipelineConfig config) {
    if (config.defaults() != null) {
      config.defaults().destinations().forEach(d -> ColumnOpHandlers.requireKnownOps(d.columns()));
    }
    for (TopicPatternConfig p : config.topicPatterns()) {
      p.destinations().forEach(d -> ColumnOpHandlers.requireKnownOps(d.columns()));
    }
    for (TopicConfig t : config.topics().values()) {
      t.destinations().forEach(d -> ColumnOpHandlers.requireKnownOps(d.columns()));
    }
  }

  /** target 미지정 시 토픽 리프(또는 캡처 그룹)를 대문자로. DB는 이 값을 테이블명으로 쓴다. */
  private String deriveTarget(String topic) {
    Matcher matcher = targetFromTopic.matcher(topic);
    if (matcher.matches() && matcher.groupCount() >= 1) {
      return matcher.group(1).toUpperCase(Locale.ROOT);
    }
    int lastDot = topic.lastIndexOf('.');
    String leaf = lastDot >= 0 ? topic.substring(lastDot + 1) : topic;
    return leaf.toUpperCase(Locale.ROOT);
  }
}
