package com.andrelomba.process_service.repository;

import java.util.List;

import com.andrelomba.process_service.domain.model.ProcessProduct;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessProductRepository {
  void insertAll(List<ProcessProduct> processes);
}
