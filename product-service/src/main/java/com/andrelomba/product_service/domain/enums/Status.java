package com.andrelomba.product_service.domain.enums;

public enum Status {
  SUCCESS("sucesso"),
  ERROR("erro");

  private String name;

  Status(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
}
