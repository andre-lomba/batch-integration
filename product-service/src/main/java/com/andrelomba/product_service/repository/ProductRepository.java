package com.andrelomba.product_service.repository;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import com.andrelomba.product_service.domain.model.Product;

@Repository
public interface ProductRepository {

  List<Product> findBatchAfterId(ObjectId lastId, int batchSize);
}
