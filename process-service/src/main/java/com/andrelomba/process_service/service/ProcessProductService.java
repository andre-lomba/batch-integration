package com.andrelomba.process_service.service;

import java.util.List;

import com.andrelomba.process_service.domain.model.ProcessProduct;

public interface ProcessProductService {
  void insertBatch(List<ProcessProduct> processes, String batchId, String logFileTimestamp);
}
