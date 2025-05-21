package com.andrelomba.product_service.config;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@Configuration
public class MongoConfig {

  // Desabilita a inserção do campo "_class" em cada documento do MongoDB
  @Bean
  MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory factory,
      MongoMappingContext context,
      MongoCustomConversions conversions) throws Exception {
    DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
    MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, context);
    converter.setCustomConversions(conversions);
    converter.setTypeMapper(new DefaultMongoTypeMapper(null));
    return converter;
  }

  // @Bean
  // public MongoClient mongoClient() {
  // MongoClientSettings settings = MongoClientSettings.builder()
  // .applyToClusterSettings(
  // builder -> builder.hosts(Collections.singletonList(new
  // ServerAddress("localhost", 27017)))
  // .serverSelectionTimeout(3L, TimeUnit.SECONDS) // fail-fast timeout
  // )
  // .build();

  // return MongoClients.create(settings);
  // }

}
