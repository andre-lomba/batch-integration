package com.andrelomba.process_service.service.impl;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.andrelomba.process_service.domain.model.ProcessProduct;
import com.andrelomba.process_service.repository.ProcessProductRepository;
import com.andrelomba.process_service.service.ProcessProductService;
import com.andrelomba.process_service.utils.FileLogger;

@Service
public class ProcessProductServiceImpl implements ProcessProductService {

    private static final Logger log = LoggerFactory.getLogger(ProcessProductServiceImpl.class);
    private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final ProcessProductRepository processProductRepository;

    public ProcessProductServiceImpl(ProcessProductRepository processProductRepository) {
        this.processProductRepository = processProductRepository;
    }

    public void insertBatch(List<ProcessProduct> processes, String batchId, String logFileTimestamp) {
        String logFilename = String.format("%s/insertion_%s.log",
                logFileTimestamp,
                logFileTimestamp);
        long start = System.currentTimeMillis();
        try {
            processProductRepository.insertAllWithCopy(processes);
            long duration = System.currentTimeMillis() - start;
            writeLog(logFilename, batchId, processes.size(), duration);
            log.info("Inseridos {} registros do lote {} em tb_process em {} ms", processes.size(), batchId, duration);
        } catch (Exception e) {
            writeErrorLog(logFilename, "Erro na inserção dos dados.", batchId, logFileTimestamp, e);
        }
    }

    private void writeLog(String fileName, String batchId, int batchSize, long duration) {
        FileLogger.writeLog(fileName,
                String.format(
                        "%s - Lote de id %s, tamanho %d, inserido em %d ms",
                        LocalTime.now().format(logTimeFormatter), batchId, batchSize, duration));
    }

    private void writeErrorLog(String mainLogFile, String customMessage, String batchId,
            String logFileTimestamp,
            Throwable ex) {
        String errorFile = String.format("%s/insertion-errors_%s.log", logFileTimestamp);

        String fullMessage = String.format("%s - Erro ao inserir dados do lote %s no banco de dados: %s\nExceção: %s\n",
                LocalDateTime.now().format(logTimeFormatter),
                batchId,
                customMessage, ex.toString());
        FileLogger.writeLog(mainLogFile, fullMessage);
        FileLogger.writeLog(errorFile, fullMessage);
        FileLogger.writeLog(errorFile, "Trace:");
        for (StackTraceElement line : ex.getStackTrace()) {
            FileLogger.writeLog(errorFile, line.toString());
        }
    }

}