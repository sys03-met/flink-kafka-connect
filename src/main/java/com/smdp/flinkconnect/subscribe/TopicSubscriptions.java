package com.smdp.flinkconnect.subscribe;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;

/**
 * Kafka 구독 토픽 목록과 토픽 glob 규칙.
 *
 * 하는 일:
 *
 *   - {@link #listKafkaTopics}: 브로커에 있는 토픽 이름 조회 (Job 시작 때 한 번)
 *   - {@link #expand}: YAML {@code kafka.subscribe} glob/정확명 → 실제 토픽 목록 (중복 제거·정렬)
 *   - {@link #matches}: glob 한 줄 ↔ 토픽명. Resolver 의 {@code topicPatterns} 매칭도 이걸 쓴다
 *
 * 구독과 라우팅이 같은 glob 규칙을 써야 해서 매칭은 여기 한 곳에만 둔다 (복붙하면 둘이 어긋난다).
 * 테스트는 브로커 없이 {@link #expand} 에 토픽 집합을 직접 넘긴다.
 */
public final class TopicSubscriptions {

  private TopicSubscriptions() {}

  /**
   * Kafka AdminClient 로 브로커 토픽 이름 조회 (내부 토픽 제외).
   *
   * 타임아웃을 짧게 둔 이유: 브로커 다운 시 Job 기동이 오래 멈추지 않게.
   */
  public static Set<String> listKafkaTopics(String bootstrapServers) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    try (AdminClient admin = AdminClient.create(props)) {
      return admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get(15, TimeUnit.SECONDS);
    }
  }

  /**
   * subscribe 패턴 → 실제 토픽 목록 (순수).
   *
   * 정확 문자열(와일드카드 없음)은 브로커에 없어도 유지한다 —
   * 아직 안 만들어진 토픽을 YAML에 적어 두는 운영 관례를 허용하기 위함.
   * glob만 브로커 토픽과 매칭한다.
   */
  public static List<String> expand(List<String> subscribePatterns, Set<String> available) {
    if (subscribePatterns == null || subscribePatterns.isEmpty()) {
      return List.of();
    }
    Set<String> topics = available != null ? available : Set.of();
    // LinkedHashSet: 중복 제거. 순서는 마지막에 정렬로 고정
    Set<String> selected = new LinkedHashSet<>();
    for (String pattern : subscribePatterns) {
      if (pattern == null || pattern.isBlank()) {
        continue;
      }
      // 정확명: 브로커에 없어도 구독 목록에 넣음 (토픽 생성 전 YAML 허용)
      if (!pattern.contains("*") && !pattern.contains("?")) {
        selected.add(pattern);
        continue;
      }
      // glob: 브로커에 실제로 있는 토픽만 고름
      for (String topic : topics) {
        if (matches(pattern, topic)) {
          selected.add(topic);
        }
      }
    }
    List<String> ordered = new ArrayList<>(selected);
    // 구독 목록 순서를 안정화 (재현·디버깅)
    ordered.sort(String::compareTo);
    return ordered;
  }

  /**
   * 토픽 이름용 단순 glob ({@code *} = 아무 문자열, {@code ?} = 글자 하나).
   * 정규식을 YAML에 노출하지 않으려고 문법을 이렇게 제한한다.
   * 예: {@code matches("hv.j.dproc.rd_*", "hv.j.dproc.rd_lp")} → true
   */
  public static boolean matches(String glob, String value) {
    if (glob == null || value == null) {
      return false;
    }
    if (glob.equals(value)) {
      return true;
    }
    // glob → 정규식 한 번 변환 후 전체 일치 (^…$)
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < glob.length(); i++) {
      char c = glob.charAt(i);
      switch (c) {
        case '*' -> regex.append(".*");
        case '?' -> regex.append('.');
        // 토픽의 '.' 등이 정규식 메타로 안 먹히게 이스케이프
        case '.', '(', ')', '[', ']', '{', '}', '+', '^', '$', '|' -> regex.append('\\').append(c);
        default -> regex.append(c);
      }
    }
    regex.append('$');
    return Pattern.compile(regex.toString()).matcher(value).matches();
  }

}
