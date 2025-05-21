package com.andrelomba.product_service.domain.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.bson.types.ObjectId;

public class Product {

  private ObjectId id;
  private String name;
  private LocalDateTime createdAt;

  public Product() {
  }

  public Product(ObjectId id, String name, LocalDateTime createdAt) {
    this.id = id;
    this.name = name;
    this.createdAt = createdAt;
  }

  public ObjectId getId() {
    return id;
  }

  public void setId(ObjectId id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public String toJson() {
    return String.format(
        "{\"id\":\"%s\",\"name\":\"%s\",\"createdAt\":\"%s\"}",
        id.toHexString(),
        name.replace("\"", "\\\""),
        createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

}
