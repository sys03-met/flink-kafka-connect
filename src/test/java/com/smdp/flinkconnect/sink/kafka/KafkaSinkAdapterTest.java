package com.smdp.flinkconnect.sink.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.smdp.flinkconnect.config.PipelineModels.DestinationConfig;
import com.smdp.flinkconnect.config.PipelineModels.SystemType;
import com.smdp.flinkconnect.config.PipelineModels.WriteMode;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRowRecord;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

/** Kafka Sink가 행을 출력 토픽의 JSON으로 만드는지만 본다. 브로커는 쓰지 않는다. */
class KafkaSinkAdapterTest {

  /** 토픽은 name, 본문은 컬럼 JSON. SQL 식 칸은 DB용이므로 빠진다. */
  @Test
  void toRecordUsesTopicNameAndSkipsSql() throws Exception {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("METER_ID", "1");
    fields.put(WriteRowRecord.SQL_PREFIX + "INGEST_DT", "NOW()");
    WriteRecord req =
        new WriteRecord(
            new DestinationConfig(SystemType.KAFKA, "hv.j.out.rd_lp", WriteMode.INSERT, List.of()),
            WriteRowRecord.of(fields));

    ProducerRecord<byte[], byte[]> record = KafkaSinkAdapter.toRecord(req);
    String json = new String(record.value(), StandardCharsets.UTF_8);

    assertEquals("hv.j.out.rd_lp", record.topic());
    assertEquals("{\"METER_ID\":\"1\"}", json);
    assertFalse(json.contains("INGEST_DT"));
  }
}
