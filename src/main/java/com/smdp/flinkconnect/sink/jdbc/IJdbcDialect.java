package com.smdp.flinkconnect.sink.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * JDBC 계열 DB마다 다른 부분 (인터페이스 — 파일명 {@code I*}). Postgres/MSSQL 어댑터가 직접 구현한다.
 *
 *   - 드라이버 이름
 *   - UPSERT 문장 (Postgres ON CONFLICT / MSSQL MERGE)
 *   - 대량 적재 (Postgres COPY / MSSQL SQLServerBulkCopy)
 *
 * 공통 배선({@link JdbcAttacher})과 Sink 들은 이 약속만 보고 일한다. DB 종류로 if 를 다시 하지 않는다.
 * Sink 함수 안에 실려 워커로 가므로 Serializable.
 */
public interface IJdbcDialect extends Serializable {

  /** {@code Class.forName} 대상. */
  String driver();

  /**
   * 키가 있으면 갱신, 없으면 넣는 문장. {@code ?} 순서는 {@code columns} 중 {@code sqlExprs} 에 없는 칸 순서.
   *
   * @param sqlExprs 컬럼명 → SQL 식 (대소문자 무시 맵)
   * @param primaryKeys 비어 있으면 안 된다 (호출자가 YAML 또는 DB PK로 채운다)
   */
  String upsertSql(
      String table, List<String> columns, Map<String, String> sqlExprs, List<String> primaryKeys
  );

  /**
   * 행 묶음을 대량 적재 통로로 한 번에 넣는다.
   *
   * @param columns 넣을 칸 (순서 = 각 행 배열 순서)
   * @param rows 행마다 칸 값 배열. 값은 {@link JdbcSql#toJdbcValue} 를 거친 상태
   */
  void bulkLoad(Connection c, String table, List<JdbcSql.Column> columns, List<Object[]> rows)
      throws SQLException;
}
