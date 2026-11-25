package com.smdp.flinkconnect.sink.jdbc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 건수가 배치 크기에 못 미쳐도 일정 시간마다 버퍼를 비운다.
 *
 * 공식 JdbcSink·Doris는 커넥터가 이 간격을 갖고, sink: dynamic 과 BULK 는 건수만 보고 있었다.
 * 트래픽이 적으면 마지막 묶음이 체크포인트(10초)까지 쌓이므로, 같은 간격을 여기서 맞춘다.
 * flush 는 Sink의 락 안에서 돌아야 한다. 작업 스레드와 이 타이머가 같은 버퍼를 만지기 때문이다.
 */
final class JdbcIntervalFlush {

  private final ScheduledExecutorService executor;
  /** 타이머 스레드의 실패. 다음 invoke 에서 작업 스레드가 다시 던진다. */
  private volatile Exception error;

  JdbcIntervalFlush(long intervalMs, Runnable flush) {
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "jdbc-batch-flush");
      // Task가 끝나는데 이 스레드가 JVM을 붙잡지 않게 한다.
      thread.setDaemon(true);
      return thread;
    });
    // 내보내기가 간격보다 길면 다음 주기를 겹치지 않고, 끝난 뒤부터 다시 잰다.
    executor.scheduleWithFixedDelay(() -> {
      try {
        flush.run();
      } catch (Exception ex) {
        error = ex;
      }
    }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  /** 타이머가 삼킨 예외를 작업 스레드로 올린다. */
  void throwIfFailed() throws Exception {
    Exception failed = error;
    if (failed != null) {
      throw failed;
    }
  }

  /**
   * 타이머를 멈춘다. 호출하는 쪽이 락을 쥔 채 부르면, flush가 그 락을 기다리는 동안 서로 못 놓는다.
   * 그래서 close 는 락 밖에서 이 메서드를 먼저 부른다.
   */
  void close() {
    executor.shutdownNow();
  }
}
