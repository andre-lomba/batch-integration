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
import com.andrelomba.product_service.domain.model.Product;
import com.andrelomba.product_service.utils.FileLogger;

@Service
public class KafkaProducerService {

  private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
  private static final DateTimeFormatter fileDateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss-SSS");
  private final KafkaTemplate<String, String> kafkaTemplate;
  private static final String TOPIC = "product-batch";

  public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendBatch(List<Product> products, String batchId, LocalDateTime processStartDateTime) {
    String filename = String.format("%s/kafka-header-footer_%s.log",
        processStartDateTime.format(fileDateFormatter), processStartDateTime.format(fileDateFormatter));

    LocalDateTime startTime = LocalDateTime.now();

    FileLogger.writeLog(filename,
        "====================================================================================================");
    FileLogger.writeLog(filename, String.format("Lote: %s", batchId));
    // Envia HEADER indicando o início de um novo lote
    KafkaHeader header = new KafkaHeader(products.size(), startTime);
    sendWithType("HEADER", batchId, header.toJson()).join();
    FileLogger.writeLog(filename, String.format("Header: %s", header.toJson()));

    final int[] successCount = { 0 };
    final int[] errorCount = { 0 };

    // Usamos um stream paralelo para aproveitar múltiplos núcleos da máquina
    List<CompletableFuture<Void>> futures = products.parallelStream()
        .map(product -> {
          return sendWithType("DATA", batchId, product
              .toJson())
              .thenAccept(result -> {
                synchronized (successCount) {
                  successCount[0]++;
                }
              })
              .exceptionally(ex -> {
                synchronized (errorCount) {
                  errorCount[0]++;
                }
                log.error("Erro ao enviar produto {} do lote {}: {}", product.getId(), batchId, ex.getMessage(), ex);
                writeErrorAndSendToErrorTopic(product, batchId, processStartDateTime, ex);
                return null;
              });
        })
        .toList();

    // Aguarda todos os envios antes de prosseguir
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    LocalDateTime endTime = LocalDateTime.now();
    // Envia FOOTER com a quantidade real enviada
    KafkaFooter footer = new KafkaFooter(successCount[0], endTime);
    sendWithType("FOOTER", batchId, footer.toJson()).join();

    boolean wasSuccessful = successCount[0] == products.size() && errorCount[0] == 0;
    FileLogger.writeLog(filename, String.format("Footer: %s", footer.toJson()));
    FileLogger.writeLog(filename,
        String.format("Status: %s", (wasSuccessful ? Status.SUCCESS : Status.ERROR).getName()));
    FileLogger.writeLog(filename, String.format("Duração: %d ms", Duration.between(startTime, endTime).toMillis()));
    FileLogger.writeLog(filename, String.format("Acertos: %d", successCount[0]));
    FileLogger.writeLog(filename, String.format("Erros: %d", errorCount[0]));
    FileLogger.writeLog(filename,
        "====================================================================================================");

    if (wasSuccessful) {
      log.info("Lote {} com {} registros enviado para o Kafka com sucesso.", batchId, successCount[0]);
    } else {
      log.error("Lote {} apresentou erro no envio de {} registros.", batchId, errorCount[0]);
      throw new KafkaUnmatchedCountException("Ocorreram erros na carga do Kafka.");
    }
  }

  // Método auxiliar para enviar mensagens com HEADER ou FOOTER
  private CompletableFuture<SendResult<String, String>> sendWithType(String type, String batchId, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, batchId, value);
    record.headers().add(new RecordHeader("type", type.getBytes(StandardCharsets.UTF_8)));
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
      LocalDateTime processStartTime,
      Throwable ex) {
    String errorFile = String.format("%s/kafka-error_%s.log",
        processStartTime.format(fileDateFormatter), processStartTime.format(fileDateFormatter));
    String payload = product.toJson();

    // Mensagem customizada por categoria
    String customMessage = "Ocorreram erros na carga do Kafka.";
    String fullMessage = String.format("Erro ao enviar registro ID %s do batch %s: %s\nExceção: %s\n",
        product.getId(), batchId, customMessage, ex.toString());
    FileLogger.writeLog(errorFile, fullMessage);
    FileLogger.writeLog(errorFile, "Trace:");
    for (StackTraceElement line : ex.getStackTrace()) {
      FileLogger.writeLog(errorFile, line.toString());
    }
    FileLogger.writeLog(errorFile, "Payload com erro: " + payload);

    // Envia para tópico de erro
    ProducerRecord<String, String> errorRecord = new ProducerRecord<>("product-error-batch", batchId,
        payload);
    errorRecord.headers().add(new RecordHeader("type",
        "ERROR".getBytes(StandardCharsets.UTF_8)));
    errorRecord.headers()
        .add(new RecordHeader("originalBatchId",
            batchId.getBytes(StandardCharsets.UTF_8)));
    kafkaTemplate.send(errorRecord);
  }
}
