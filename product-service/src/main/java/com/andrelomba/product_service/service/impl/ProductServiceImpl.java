package com.andrelomba.product_service.service.impl;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.andrelomba.product_service.domain.enums.Status;
import com.andrelomba.product_service.domain.exception.KafkaUnmatchedCountException;
import com.andrelomba.product_service.domain.model.Product;
import com.andrelomba.product_service.repository.ProductRepository;
import com.andrelomba.product_service.service.ProductService;
import com.andrelomba.product_service.utils.FileLogger;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

@Service
public class ProductServiceImpl implements ProductService {

  private static final Logger log = LoggerFactory.getLogger(ProductServiceImpl.class);
  private static final DateTimeFormatter fileDateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss-SSS");
  private static final DateTimeFormatter brDateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS");
  private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  private static final int BATCH_SIZE = 50_000;

  private final ProductRepository productRepository;
  private final KafkaProducerService kafkaProducerService;

  public ProductServiceImpl(ProductRepository productRepository, KafkaProducerService kafkaProducerService) {
    this.productRepository = productRepository;
    this.kafkaProducerService = kafkaProducerService;
  }

  public void extractAll() {
    int totalRecords = 0;
    ObjectId lastId = null;
    long totalExtractionDuration = 0;
    long totalKafkaDuration = 0;
    LocalDateTime startDateTime = LocalDateTime.now();
    String filename = String.format("%s/extraction-and-kafka_%s.log",
        startDateTime.format(fileDateFormatter), startDateTime.format(fileDateFormatter));

    writeLogHeader(filename, startDateTime);
    log.info("Iniciando extração...");

    while (true) {
      List<Product> batch;
      long startBatchTime = System.currentTimeMillis();
      try {
        batch = productRepository.findBatchAfterId(lastId, BATCH_SIZE);
      } catch (MongoTimeoutException e) {
        log.error("O banco de dados está fora do ar! Sua Integração foi abortada.");
        writeErrorLog(filename, "O banco de dados está fora do ar! Sua Integração foi abortada.", startDateTime, e);
        writeLogFooter(filename, totalExtractionDuration, totalKafkaDuration, totalRecords, Status.ERROR);
        throw e;
      } catch (MongoException e) {
        log.error("Ocorreram erros na extração do banco de dados.");
        writeErrorLog(filename, "Ocorreram erros na extração do banco de dados.", startDateTime, e);
        writeLogFooter(filename, totalExtractionDuration, totalKafkaDuration, totalRecords, Status.ERROR);
        throw e;
      }

      long endBatchTime = System.currentTimeMillis();
      long totalBatchDuration = endBatchTime - startBatchTime;
      totalExtractionDuration += totalBatchDuration;

      if (batch.isEmpty()) {
        writeLogFooter(filename, totalExtractionDuration, totalKafkaDuration, totalRecords, Status.SUCCESS);
        log.info("Extração finalizada: {} registros em {} ms.", totalRecords, totalExtractionDuration);
        log.info("Envio ao Kafka finalizado: {} registros em {} ms.", totalRecords, totalKafkaDuration);
        break;
      }

      String batchId = String.format("product-batch_%s", LocalDateTime.now().format(fileDateFormatter));

      long startBatchKafkaTime = System.currentTimeMillis();
      try {
        kafkaProducerService.sendBatch(batch, batchId, startDateTime);
      } catch (KafkaUnmatchedCountException e) {
        throw e;
      }
      long endBatchKafkaTime = System.currentTimeMillis();
      long totalBatchKafkaDuration = endBatchKafkaTime - startBatchKafkaTime;
      totalKafkaDuration += totalBatchKafkaDuration;

      log.info("Lote de id {} extraído e enviado para o Kafka. Total: {}.", batchId, totalRecords);
      writeLogRecordSuccess(filename, batchId, batch.size(), totalBatchDuration, totalBatchKafkaDuration);
      totalRecords += batch.size();
      lastId = batch.get(batch.size() - 1).getId();

    }
  }

  private void writeLogHeader(String filename, LocalDateTime startDateTime) {
    FileLogger.writeLog(filename,
        String.format(
            "==================================== Extração de registros em Lotes e Envio ao Kafka ====================================\n- Iniciado em: %s\n- Tamanho máximo de lote: %d\n=========================================================================================================================",
            startDateTime.format(brDateFormatter), BATCH_SIZE));
  }

  private void writeLogRecordSuccess(String filename, String batchId, int batchSize, long extractionDuration,
      long kafkaDuration) {
    FileLogger.writeLog(filename,
        String.format("%s - Lote de id %s, tamanho %d, extraído em %d ms e enviado ao Kafka em %d ms",
            LocalTime.now().format(logTimeFormatter), batchId, batchSize, extractionDuration, kafkaDuration));
  }

  private void writeLogFooter(String filename, long extractionDuration, long kafkaDuration, int totalRecords,
      Status status) {
    FileLogger.writeLog(filename,
        String.format(
            "=========================================================================================================================\n- Finalizado em: %s\n- Duração total da extração: %d ms\n- Duração total do envio ao Kafka: %d ms\n- Total de registros extraídos: %d\n- Status: %s\n=========================================================================================================================",
            LocalDateTime.now().format(brDateFormatter), extractionDuration, kafkaDuration, totalRecords,
            status.getName()));
  }

  private void writeErrorLog(String mainLogFile, String customMessage, LocalDateTime processStartTime,
      Throwable ex) {
    String errorFile = String.format("%s/extraction-error_%s.log",
        processStartTime.format(fileDateFormatter), processStartTime.format(fileDateFormatter));

    String fullMessage = String.format("%s - Erro no banco de dados: %s\nExceção: %s\n",
        LocalDateTime.now().format(logTimeFormatter),
        customMessage, ex.toString());
    FileLogger.writeLog(mainLogFile, fullMessage);
    FileLogger.writeLog(errorFile, fullMessage);
    FileLogger.writeLog(errorFile, "Trace:");
    for (StackTraceElement line : ex.getStackTrace()) {
      FileLogger.writeLog(errorFile, line.toString());
    }
  }
}
