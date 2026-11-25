package com.smdp.flinkconnect.model;

import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 파이프라인을 흘러가는 데이터 상자들 (값만 있고 실행 로직 없음).
 *
 * <pre>
 * Kafka → TopicRecord → (Resolver.expand) → WriteRecord = DestinationConfig + WriteRowRecord → Sink
 * </pre>
 *
 * Flink 가 연산자 사이로 넘길 때 Kryo 로 직렬화하므로 맵·리스트는 가변 복사본을 둔다.
 */
public final class PipelineRecords {

  private PipelineRecords() {}

  /**
   * Kafka 메시지 하나 (토픽명 + JSON 본문). {@code PipelineJob.KafkaDecoder} 가 만든다.
   *
   * @param topic 예: {@code hv.j.dproc.rd_lp}. 어디로 갈지 찾을 때 씀
   * @param payload JSON 본문. 예: {@code {"METER_ID":"1","FAP":100}}
   */
  public record TopicRecord(String topic, Map<String, Object> payload) implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Sink로 흘러가는 쓰기 한 건. 한 메시지의 갈 곳이 N개면 N건.
   *
   * @param destination 어디·어떻게 (Resolver 가 target/writeMode 를 채운 값)
   * @param row 무엇을
   */
  public record WriteRecord(DestinationConfig destination, WriteRowRecord row) implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  /**
   * DB에 넣을 한 행. 컬럼 규칙을 적용한 Map을 그대로 담는다.
   *
   * {@code op: sql} 이면 키가 {@code __sql:컬럼명}, 값은 {@code NOW()} 같은 식 문자열.
   * Sink 가 INSERT 만들 때 이 접두사만 보면 된다. 별도 셀 타입 없음.
   */
  public record WriteRowRecord(Map<String, Object> fields) implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 컬럼 규칙 {@code op: sql} 이 Map에 남기는 키 접두사. */
    public static final String SQL_PREFIX = "__sql:";

    public WriteRowRecord {
      // Map.copyOf는 null 값을 거부. Kryo(Flink)는 불변 맵에 put 하다 깨지므로 가변 맵 유지.
      fields = fields == null ? new LinkedHashMap<>() : new LinkedHashMap<>(fields);
    }

    /** 손질된 Map을 그대로 감싼다. */
    public static WriteRowRecord of(Map<String, Object> raw) {
      return new WriteRowRecord(raw);
    }

    /** INSERT 컬럼 목록. {@code __sql:INGEST_DT} → {@code INGEST_DT}. 그 밖의 {@code __} 키는 내부용이라 뺀다. */
    public List<String> columns() {
      Set<String> cols = new LinkedHashSet<>();
      for (String key : fields.keySet()) {
        if (key.startsWith(SQL_PREFIX)) {
          cols.add(key.substring(SQL_PREFIX.length()));
        } else if (!key.startsWith("__")) {
          cols.add(key);
        }
      }
      return new ArrayList<>(cols);
    }

    /** {@code op: sql} 칸이 하나라도 있는가 (대량 적재 거절용). */
    public boolean hasSql() {
      return fields.keySet().stream().anyMatch(k -> k.startsWith(SQL_PREFIX));
    }

    /** 이 컬럼이 {@code op: sql} 인가. */
    public boolean isSql(String column) {
      return fields.containsKey(SQL_PREFIX + column);
    }

    /** SQL 식 문자열. 예: {@code NOW()}. */
    public String sqlExpression(String column) {
      Object v = fields.get(SQL_PREFIX + column);
      return v == null ? null : String.valueOf(v);
    }

    /** 보통 값. sql 컬럼이면 null. */
    public Object value(String column) {
      return isSql(column) ? null : fields.get(column);
    }
  }
}
