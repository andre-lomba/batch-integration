package com.andrelomba.product_service.domain.exception;

public class KafkaUnmatchedCountException extends RuntimeException {
  public KafkaUnmatchedCountException(String message) {
    super(message);
  }
}
