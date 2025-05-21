package com.andrelomba.product_service.domain.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KafkaHeader {

  private int recordsCount;
  private LocalDateTime startTime;

  public KafkaHeader(int recordsCount, LocalDateTime startTime) {
    this.recordsCount = recordsCount;
    this.startTime = startTime;
  }

  public int getRecordsCount() {
    return recordsCount;
  }

  public void setRecordsCount(int recordsCount) {
    this.recordsCount = recordsCount;
  }

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(LocalDateTime startTime) {
    this.startTime = startTime;
  }

  public String toJson() {
    return String.format(
        "{\"recordsCount\":\"%d\",\"startTime\":\"%s\"}",
        recordsCount,
        startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

}
