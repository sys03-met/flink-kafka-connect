package com.smdp.flinkconnect.subscribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * TopicSubscriptions: 구독 glob 확장 + glob 매칭 규칙.
 *
 * 브로커 없이 토픽 집합을 {@code expand} 에 바로 넘긴다.
 */
class TopicSubscriptionsTest {

  /** glob 은 브로커 토픽과 매칭, 정확명은 그대로 유지, 결과는 정렬. */
  @Test
  void expandsGlobAndKeepsExactTopics() {
    Set<String> broker =
        Set.of(
            "hv.j.dproc.rd_lp",
            "hv.j.dproc.rd_mon",
            "hv.j.dproc.meter_curr_limit",
            "hv.s.cfep.recv_packet");

    List<String> topics =
        TopicSubscriptions.expand(List.of("hv.j.dproc.rd_*", "hv.j.dproc.meter_curr_limit"), broker);

    assertEquals(List.of("hv.j.dproc.meter_curr_limit", "hv.j.dproc.rd_lp", "hv.j.dproc.rd_mon"), topics);
  }

  /** {@code .} 은 글자 그대로, {@code ?} 는 한 글자. */
  @Test
  void globDotIsLiteralAndQuestionMarkIsOneChar() {
    assertTrue(TopicSubscriptions.matches("hv.j.dproc.rd_*", "hv.j.dproc.rd_lp"));
    assertFalse(TopicSubscriptions.matches("hv.j.dproc.rd_*", "hvXjXdprocXrd_lp"));
    assertTrue(TopicSubscriptions.matches("rd_?p", "rd_lp"));
    assertFalse(TopicSubscriptions.matches("rd_?p", "rd_llp"));
  }
}
