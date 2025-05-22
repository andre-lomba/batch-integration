package com.andrelomba.product_service.domain.model;

public class KafkaSendResultCounter {
  private int successCount = 0;
  private int errorCount = 0;

  public int getSuccessCount() {
    return successCount;
  }

  public void incrementSuccessCount() {
    this.successCount++;
  }

  public int getErrorCount() {
    return errorCount;
  }

  public void incrementErrorCount() {
    this.errorCount++;
  }

}
