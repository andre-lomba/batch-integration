package com.andrelomba.product_service.repository.impl;

import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import com.andrelomba.product_service.domain.model.Product;
import com.andrelomba.product_service.repository.ProductRepository;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

@Repository
public class ProductMongoRepository implements ProductRepository {

  private final MongoDatabase database;
  private final MongoCollection<Document> collection;

  public ProductMongoRepository(MongoClient mongoClient) {
    this.database = mongoClient.getDatabase("batch_integration");
    this.collection = database.getCollection("product");
  }

  // Maneira performática de acessar documentos no MongoDB
  // Buscamos lotes de Document dentro da Collection baseado no último id buscado
  // Evitamos conversão on-the-fly para a classe Product
  public List<Product> findBatchAfterId(ObjectId lastId, int batchSize) {
    Bson filter = lastId != null ? gt("_id", lastId) : new Document();

    FindIterable<Document> iterable = collection.find(filter)
        .projection(include("_id", "name", "createdAt"))
        .sort(ascending("_id"))
        .limit(batchSize)
        .batchSize(batchSize);

    List<Product> batch = new ArrayList<>(batchSize);
    try (MongoCursor<Document> cursor = iterable.iterator()) {
      while (cursor.hasNext()) {
        Document entity = cursor.next();
        batch.add(new Product(
            entity.getObjectId("_id"), entity.get("name").toString(),
            LocalDateTime.ofInstant(entity.getDate("createdAt").toInstant(), ZoneOffset.UTC)));
      }
    }
    return batch;
  }

}
