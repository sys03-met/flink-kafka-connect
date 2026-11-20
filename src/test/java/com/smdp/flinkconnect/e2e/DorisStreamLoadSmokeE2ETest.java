package com.smdp.flinkconnect.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.smdp.flinkconnect.config.PipelineModels.DbType;
import com.smdp.flinkconnect.config.PipelineModels.JdbcEndpointRecord;
import com.smdp.flinkconnect.config.PipelineModels.PipelineConfig;
import com.smdp.flinkconnect.config.YamlPipelineConfigLoader;
import com.smdp.flinkconnect.model.PipelineRecords.TopicRecord;
import com.smdp.flinkconnect.model.PipelineRecords.WriteRecord;
import com.smdp.flinkconnect.transform.DefaultTopicBindingResolver;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 공식 DorisSink Stream Load 스모크: 2천 건 → COUNT.
 */
@Tag("docker")
class DorisStreamLoadSmokeE2ETest {

  private static final Path YAML = Path.of("config/doris-streamload-e2e.yaml");
  private static final int N = 2_000;

  @BeforeAll
  static void requireDoris() {
    assumeTrue(portOpen(9030), "Doris JDBC down");
    assumeTrue(portOpen(8040), "Doris BE down");
  }

  @Test
  void streamLoad_bulkInsertsRows() throws Exception {
    FlinkLocalWrites.Doris.ensureRdLp();
    FlinkLocalWrites.Doris.truncate("RD_LP");

    PipelineConfig config = new YamlPipelineConfigLoader().load(YAML);
    YamlPipelineConfigLoader.Resolved settings = YamlPipelineConfigLoader.resolve(config, Map.of());
    assertTrue(
        settings.endpoints().values().stream()
            .anyMatch(e -> e.streamLoadUrl() != null && e.streamLoadUrl().contains(":8040")));

    // Doris만 남긴 엔드포인트 (PG 동시 적재 없이 Sink 경로만 검증)
    Map<DbType, JdbcEndpointRecord> dorisOnlyEp =
        Map.of(DbType.DORIS, settings.endpoints().get(DbType.DORIS));

    DefaultTopicBindingResolver resolver = new DefaultTopicBindingResolver(config);
    List<WriteRecord> dorisOnly = new ArrayList<>();
    for (int i = 0; i < N; i++) {
      Map<String, Object> payload = new HashMap<>();
      payload.put("METER_ID", String.format("SL%06d", i));
      payload.put("FAP", 1.0);
      payload.put("LARAP", 0.5);
      for (WriteRecord req : resolver.expand(new TopicRecord("hv.j.dproc.rd_lp", payload))) {
        if (req.destination().db() == DbType.DORIS) {
          dorisOnly.add(req);
        }
      }
    }

    FlinkLocalWrites.writeAll(dorisOnly, dorisOnlyEp, 500);
    assertEquals(N, FlinkLocalWrites.Doris.count("RD_LP"));
  }

  private static boolean portOpen(int port) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("localhost", port), 400);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
