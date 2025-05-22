package com.andrelomba.process_service.repository.impl;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Repository;

import com.andrelomba.process_service.domain.model.ProcessProduct;
import com.andrelomba.process_service.repository.ProcessProductRepository;

@Repository
public class ProcessProductPostgresRepository implements ProcessProductRepository {

  private final JdbcTemplate jdbcTemplate;

  public ProcessProductPostgresRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void insertAll(List<ProcessProduct> processes) {
    jdbcTemplate.batchUpdate(
        "INSERT INTO tb_process (batch_id, product_id, product_name, created_at, processed_at) VALUES (?, ?, ?, ?, ?)",
        new BatchPreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setString(1, processes.get(i).getBatchId());
            ps.setString(2, processes.get(i).getProductId());
            ps.setString(3, processes.get(i).getProductName());
            ps.setTimestamp(4, Timestamp.valueOf(processes.get(i).getCreatedAt()));
            ps.setTimestamp(5, Timestamp.valueOf(processes.get(i).getProcessedAt()));
          }

          @Override
          public int getBatchSize() {
            return processes.size();
          }
        });
  }

  public void insertAllWithCopy(List<ProcessProduct> processes)
      throws IOException, CannotGetJdbcConnectionException, SQLException {
    String csv = buildCsvData(processes);
    try (Connection conn = DataSourceUtils.getConnection(jdbcTemplate.getDataSource())) {
      BaseConnection pgConn = conn.unwrap(BaseConnection.class);
      CopyManager copyManager = new CopyManager(pgConn);
      try (Reader reader = new StringReader(csv)) {
        copyManager.copyIn(
            "COPY tb_process(batch_id, product_id, product_name, created_at, processed_at) FROM STDIN WITH (FORMAT csv)",
            reader);
      }
    }
  }

  private String buildCsvData(List<ProcessProduct> processes) {
    StringBuilder builder = new StringBuilder();
    for (ProcessProduct p : processes) {
      builder.append(p.getBatchId()).append(',')
          .append(p.getProductId()).append(',')
          .append(p.getProductName().replace(",", " ")) // evita quebrar CSV
          .append(',')
          .append(p.getCreatedAt()).append(',')
          .append(p.getProcessedAt()).append('\n');
    }
    return builder.toString();
  }

}
