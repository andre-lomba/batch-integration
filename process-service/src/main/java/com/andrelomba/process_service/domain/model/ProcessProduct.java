package com.andrelomba.process_service.domain.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProcessProduct {

  private String batchId;
  private String productId;
  private String productName;
  private LocalDateTime createdAt;
  private LocalDateTime processedAt;

  public ProcessProduct(String batchId, String productId, String productName, LocalDateTime createdAt,
      LocalDateTime processedAt) {
    this.batchId = batchId;
    this.productId = productId;
    this.productName = productName;
    this.createdAt = createdAt;
    this.processedAt = processedAt;
  }

  public String getBatchId() {
    return batchId;
  }

  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public LocalDateTime getProcessedAt() {
    return processedAt;
  }

  public void setProcessedAt(LocalDateTime processedAt) {
    this.processedAt = processedAt;
  }

  public String toJson() {
    return String.format(
        "{\"batchId\":\"%s\",\"productId\":\"%s\",\"productName\":\"%s\",\"createdAt\":\"%s\",\"processedAt\":\"%s\"}",
        batchId,
        productId,
        productName.replace("\"", "\\\""),
        createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        processedAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

}
