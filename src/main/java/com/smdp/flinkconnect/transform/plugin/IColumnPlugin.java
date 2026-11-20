package com.smdp.flinkconnect.transform.plugin;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import java.util.Map;

/**
 * 컬럼 변환 플러그인 전략 인터페이스 (같은 JAR에 빌드).
 *
 * YAML {@code op: plugin}, {@code expression}: 짧은 클래스명 또는 전체 패키지 클래스명.
 * {@code field}: 주 연산 컬럼. {@code params}: 추가 인자.
 *
 * 인자 이름은 {@code rule}이 맞다 — 규칙 전체(op/field/params/…)이지 컬럼명 하나가 아니다.
 *
 *
 * - field: FAP_KWH
 *   op: plugin
 *   expression: ScalePlugin
 *   params:
 *     source: FAP
 *     factor: 0.001
 *
 */
public interface IColumnPlugin {

  /**
   * row를 in-place로 손질한다.
   *
   * @param row 변환 중인 맵 (다른 컬럼도 읽기 가능)
   * @param rule 컬럼 규칙 전체 — 주 컬럼은 {@code rule.field()}, 인자는 {@code rule.params()}
   */
  void apply(Map<String, Object> row, ColumnRule rule);
}
