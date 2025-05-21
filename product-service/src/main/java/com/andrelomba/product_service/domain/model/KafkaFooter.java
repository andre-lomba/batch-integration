package com.andrelomba.product_service.domain.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KafkaFooter {

  private int recordsCount;
  private LocalDateTime endTime;

  public KafkaFooter(int recordsCount, LocalDateTime endTime) {
    this.recordsCount = recordsCount;
    this.endTime = endTime;
  }

  public int getRecordsCount() {
    return recordsCount;
  }

  public void setRecordsCount(int recordsCount) {
    this.recordsCount = recordsCount;
  }

  public LocalDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(LocalDateTime endTime) {
    this.endTime = endTime;
  }

  public String toJson() {
    return String.format(
        "{\"recordsCount\":\"%d\",\"endTime\":\"%s\"}",
        recordsCount,
        endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

}
