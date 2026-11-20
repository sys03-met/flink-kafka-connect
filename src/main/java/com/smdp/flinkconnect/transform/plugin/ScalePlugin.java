package com.smdp.flinkconnect.transform.plugin;

import com.smdp.flinkconnect.config.PipelineModels.ColumnRule;
import java.util.Map;

/**
 * 예제: {@code params}로 인자를 받는 플러그인.
 *
 * {@code params.source}(기본=field) × {@code params.factor} → {@code field}.
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
public final class ScalePlugin implements IColumnPlugin {

  @Override
  public void apply(Map<String, Object> row, ColumnRule rule) {
    if (rule.field() == null) {
      return;
    }
    String source = rule.paramString("source", rule.field());
    double factor = rule.paramDouble("factor", 1.0);
    Object raw = row.get(source);
    if (!(raw instanceof Number n)) {
      return;
    }
    row.put(rule.field(), n.doubleValue() * factor);
  }
}
