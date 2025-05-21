package com.andrelomba.product_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

  @Bean
  public NewTopic productBatchTopic() {
    return new NewTopic("product-batch", 6, (short) 1);
  }
}
