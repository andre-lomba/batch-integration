package com.andrelomba.process_service.service.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.andrelomba.process_service.domain.model.ProcessProduct;
import com.andrelomba.process_service.service.ProcessProductService;
import com.andrelomba.process_service.utils.FileLogger;
import com.andrelomba.process_service.utils.JsonFormatter;

@Service
public class KafkaConsumerService {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
  private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  private final ProcessProductService processProductService;

  // Estruturas temporárias para controle de lote
  private final Map<String, List<ProcessProduct>> batchBuffer = new ConcurrentHashMap<>();
  private final Map<String, Integer> expectedCount = new ConcurrentHashMap<>();
  private final Map<String, Long> startTime = new ConcurrentHashMap<>();
  private final Map<String, Integer> successCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> errorCount = new ConcurrentHashMap<>();

  public KafkaConsumerService(ProcessProductService processProductService) {
    this.processProductService = processProductService;
  }

  @KafkaListener(topics = "product-batch", groupId = "batch-integration-group")
  public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
    String batchId = getHeader(record, "batchId");
    String type = getHeader(record, "type");
    String logFileTimestamp = getHeader(record, "logFileTimestamp");

    if (batchId == null || type == null) {
      log.warn("Mensagem ignorada por falta de headers obrigatórios: {}", record);
      return;
    }

    switch (type) {
      case "HEADER" -> handleHeader(batchId, record.value());
      case "DATA" -> handleData(batchId, record.value(), record);
      case "FOOTER" -> handleFooter(batchId, record.value(), logFileTimestamp, ack);
      default -> log.warn("Tipo de mensagem desconhecido: {}", type);
    }
  }

  // HEADER: Inicializa estruturas de controle para o lote
  private void handleHeader(String batchId, String value) {
    if (batchBuffer.containsKey(batchId)) {
      log.warn("HEADER duplicado ignorado para batch {}", batchId);
      return;
    }

    try {
      int count = Integer.parseInt(JsonFormatter.extractValueFromJson(value, "recordsCount"));
      expectedCount.put(batchId, count);
      batchBuffer.put(batchId, new ArrayList<>(count));
      successCount.put(batchId, 0);
      errorCount.put(batchId, 0);
      startTime.put(batchId, System.currentTimeMillis());
      log.info("HEADER recebido. Esperando {} registros no batch {}.", count, batchId);
    } catch (NumberFormatException e) {
      log.error("HEADER inválido para batch {}: {}", batchId, value);
    }
  }

  // DATA: Processa o registro e armazena em memória
  private void handleData(String batchId, String value, ConsumerRecord<String, String> record) {
    List<ProcessProduct> buffer = batchBuffer.get(batchId);
    if (buffer == null) {
      log.warn("Recebido DATA para batch {} sem HEADER válido.", batchId);
      return;
    }

    try {
      String id = JsonFormatter.extractValueFromJson(value, "id");
      String name = JsonFormatter.extractValueFromJson(value, "name");
      LocalDateTime createdAt = LocalDateTime.parse(JsonFormatter.extractValueFromJson(value, "createdAt"));

      buffer.add(new ProcessProduct(batchId, id, name, createdAt, LocalDateTime.now()));
      successCount.compute(batchId, (k, v) -> v + 1);
    } catch (Exception e) {
      errorCount.compute(batchId, (k, v) -> v + 1);
      savePayloadError(batchId, record);
      log.error("Erro ao processar payload do batch {}: {}", batchId, e.getMessage(), e);
    }
  }

  // FOOTER: Finaliza e grava log apenas uma vez por batch
  private void handleFooter(String batchId, String value, String logFileTimestamp, Acknowledgment ack) {
    // Se não houver HEADER ou já foi processado antes
    if (!expectedCount.containsKey(batchId)) {
      log.warn("FOOTER recebido sem HEADER ou batch já finalizado: {}", batchId);
      return;
    }

    int expectedCountFooter = Integer.parseInt(JsonFormatter.extractValueFromJson(value, "recordsCount"));
    int expectedCountHeader = expectedCount.get(batchId);
    int success = successCount.getOrDefault(batchId, 0);
    int error = errorCount.getOrDefault(batchId, 0);
    long duration = System.currentTimeMillis() - startTime.getOrDefault(batchId, System.currentTimeMillis());
    String logFile = String.format("%s/kafka-consumer_%s.log",
        logFileTimestamp,
        logFileTimestamp);

    // Verifica se todos os registros esperados chegaram
    if (success + error == expectedCountHeader && expectedCountHeader == expectedCountFooter) {
      FileLogger.writeLog(logFile, String.format(
          "%s - Lote de id %s, sucesso: %d, erros: %d, processado em %d ms",
          LocalTime.now().format(logTimeFormatter), batchId, success, error, duration));
      if (success > 0) {
        processProductService.insertBatch(batchBuffer.get(batchId), batchId, logFileTimestamp);
      }
    } else {
      persistInvalidBatch(batchId, expectedCountHeader, success + error, logFileTimestamp, batchBuffer.get(batchId));
    }
    ack.acknowledge();

    // Remove todos os dados do batch
    cleanupBatch(batchId);
  }

  private void cleanupBatch(String batchId) {
    expectedCount.remove(batchId);
    batchBuffer.remove(batchId);
    successCount.remove(batchId);
    errorCount.remove(batchId);
    startTime.remove(batchId);
  }

  private void savePayloadError(String batchId, ConsumerRecord<String, String> record) {
    Path path = Path.of("kafka/errors_batch_" + batchId + ".log");
    try {
      String payload = String.format("Erro no offset %d: %s", record.offset(), record.value());
      Files.writeString(path, payload + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      log.error("Erro ao salvar payload com erro do batch {}: {}", batchId, e.getMessage(), e);
    }
  }

  private void persistInvalidBatch(String batchId, int expectedCount, int receivedCount, String logFileTimestamp,
      List<ProcessProduct> buffer) {
    String logFile = String.format("%s/kafka-consumer-batch-errors_%s.log",
        logFileTimestamp,
        logFileTimestamp);

    log.error("Contagem inconsistente para batch {}: esperados {}, recebidos {}.",
        batchId, expectedCount, receivedCount);

    FileLogger.writeLog(logFile,
        "====================================================================================================");
    FileLogger.writeLog(logFile,
        String.format("%s - Contagem inconsistente para batch %s: esperados %d, recebidos %d.",
            LocalDateTime.now().format(logTimeFormatter), batchId, expectedCount, receivedCount));
    for (ProcessProduct p : buffer) {
      FileLogger.writeLog(logFile, p.toJson());
    }
    FileLogger.writeLog(logFile,
        "====================================================================================================");
  }

  private String getHeader(ConsumerRecord<String, String> record, String key) {
    Header header = record.headers().lastHeader(key);
    return header != null ? new String(header.value()) : null;
  }
}
