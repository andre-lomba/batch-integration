package com.andrelomba.product_service.service.impl;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.andrelomba.product_service.domain.enums.Status;
import com.andrelomba.product_service.domain.exception.KafkaUnmatchedCountException;
import com.andrelomba.product_service.domain.model.KafkaFooter;
import com.andrelomba.product_service.domain.model.KafkaHeader;
import com.andrelomba.product_service.domain.model.KafkaSendResultCounter;
import com.andrelomba.product_service.domain.model.Product;
import com.andrelomba.product_service.utils.FileLogger;

@Service
public class KafkaProducerService {

  private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
  private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  private static final String TOPIC = "product-batch";
  private static final String ERROR_TOPIC = "product-batch-errors";

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendBatch(List<Product> products, String batchId, String logFileTimestamp) {
    String filename = String.format("%s/kafka-header-footer_%s.log",
        logFileTimestamp, logFileTimestamp);

    FileLogger.writeLog(
        filename,
        "====================================================================================================");
    FileLogger.writeLog(filename, String.format("Lote: %s", batchId));

    // Envia HEADER indicando o início de um novo lote
    KafkaHeader header = createAndSendHeader(products.size(), batchId, filename, logFileTimestamp);

    KafkaSendResultCounter result = sendBatchDataParallel(products, batchId, logFileTimestamp);

    int success = result.getSuccessCount();
    int errors = result.getErrorCount();

    // Envia FOOTER com a quantidade real enviada
    KafkaFooter footer = createAndSendFooter(success, batchId, filename, logFileTimestamp);

    boolean wasSuccessful = success == products.size() && errors == 0;
    FileLogger.writeLog(filename,
        String.format("Status: %s", (wasSuccessful ? Status.SUCCESS : Status.ERROR).getName()));
    FileLogger.writeLog(filename,
        String.format("Duração: %d ms", Duration.between(header.getStartTime(), footer.getEndTime()).toMillis()));
    FileLogger.writeLog(filename, String.format("Acertos: %d", success));
    FileLogger.writeLog(filename, String.format("Erros: %d", errors));
    FileLogger.writeLog(filename,
        "====================================================================================================");

    if (wasSuccessful) {
      log.info("Lote {} com {} registros enviado para o Kafka com sucesso.", batchId, success);
    } else {
      log.error("Lote {} apresentou erro no envio de {} registros.", batchId, errors);
      throw new KafkaUnmatchedCountException("Ocorreram erros na carga do Kafka.");
    }
  }

  private KafkaHeader createAndSendHeader(int recordsSize, String batchId, String logFilename,
      String logFileTimestamp) {
    LocalDateTime startTime = LocalDateTime.now();
    KafkaHeader header = new KafkaHeader(recordsSize, startTime);
    sendWithType("HEADER", logFileTimestamp, batchId, header.toJson()).join();
    FileLogger.writeLog(logFilename, String.format("Header: %s", header.toJson()));
    return header;
  }

  private KafkaSendResultCounter sendBatchDataParallel(
      List<Product> products,
      String batchId,
      String logFileTimestamp) {
    KafkaSendResultCounter counter = new KafkaSendResultCounter();

    List<CompletableFuture<Void>> futures = products.parallelStream()
        .map(product -> {
          return sendWithType("DATA", logFileTimestamp, batchId, product.toJson())
              .thenAccept(result -> {
                synchronized (counter) {
                  counter.incrementSuccessCount();
                }
              })
              .exceptionally(ex -> {
                synchronized (counter) {
                  counter.incrementErrorCount();
                }
                writeErrorAndSendToErrorTopic(product, batchId, logFileTimestamp, ex);
                return null;
              });
        })
        .toList();

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    return counter;
  }

  private KafkaFooter createAndSendFooter(int successCount, String batchId, String logFilename,
      String logFileTimestamp) {
    LocalDateTime endTime = LocalDateTime.now();
    // Envia FOOTER com a quantidade real enviada
    KafkaFooter footer = new KafkaFooter(successCount, endTime);
    sendWithType("FOOTER", logFileTimestamp, batchId, footer.toJson()).join();
    FileLogger.writeLog(logFilename, String.format("Footer: %s", footer.toJson()));
    return footer;
  }

  // Método auxiliar para enviar mensagens
  private CompletableFuture<SendResult<String, String>> sendWithType(String type, String logFileTimestamp,
      String batchId, String value) {

    // // 🔥 Simula falha proposital no producer
    // if (value.contains("Product 10000")) {
    // CompletableFuture<SendResult<String, String>> failed = new
    // CompletableFuture<>();
    // failed.completeExceptionally(new RuntimeException("Erro simulado ao enviar ao
    // Kafka"));
    // return failed;
    // }

    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, batchId, value);
    record.headers().add(new RecordHeader("type", type.getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader("logFileTimestamp", logFileTimestamp.getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader("batchId", batchId.getBytes(StandardCharsets.UTF_8)));

    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
    future.whenComplete((result, ex) -> {
      if (ex != null) {
        log.error("Erro ao enviar {} do batch {}: {}", type, batchId, ex.getMessage(), ex);
      }
    });
    return future;
  }

  private void writeErrorAndSendToErrorTopic(Product product, String batchId,
      String logFileTimestamp,
      Throwable ex) {
    String errorFile = String.format("%s/kafka-error_%s.log",
        logFileTimestamp, logFileTimestamp);
    String payload = product.toJson();
    String customMessage = "Ocorreram erros na carga do Kafka.";
    String fullMessage = String.format("%s - Erro ao enviar registro ID %s do batch %s: %s\nExceção: %s",
        LocalDateTime.now().format(logTimeFormatter),
        product.getId(), batchId, customMessage, ex.toString());

    log.error("Erro ao enviar produto {} do lote {}: {}", product.getId(), batchId, ex.getMessage(), ex);

    FileLogger.writeLog(
        errorFile,
        "====================================================================================================");
    FileLogger.writeLog(errorFile, fullMessage);
    FileLogger.writeLog(errorFile, "Trace:");
    for (StackTraceElement line : ex.getStackTrace()) {
      FileLogger.writeLog(errorFile, line.toString());
    }

    FileLogger.writeLog(errorFile, "Payload com erro: " + payload);
    // Envia para tópico de erro
    ProducerRecord<String, String> errorRecord = new ProducerRecord<>(ERROR_TOPIC, batchId, payload);
    errorRecord.headers().add(new RecordHeader("type", "ERROR".getBytes(StandardCharsets.UTF_8)));
    errorRecord.headers().add(new RecordHeader("originalBatchId", batchId.getBytes(StandardCharsets.UTF_8)));
    kafkaTemplate.send(errorRecord);

    FileLogger.writeLog(errorFile, String.format("Payload enviado para tópico %s", ERROR_TOPIC));
    FileLogger.writeLog(
        errorFile,
        "====================================================================================================");
  }
}
