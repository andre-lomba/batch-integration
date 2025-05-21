package com.andrelomba.product_service.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.andrelomba.product_service.service.ProductService;

@RestController
@RequestMapping("/api/products")
public class ProductController {

  private final ProductService productService;

  public ProductController(ProductService productService) {
    this.productService = productService;
  }

  @PostMapping("/integrate")
  public ResponseEntity<Void> extractAll() {
    productService.extractAll();
    return ResponseEntity.noContent().build();
  }
}
