package com.andrelomba.process_service.service.impl;

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

    public void insertBatch(List<ProcessProduct> processes, String batchId) {
        String logFilename = "insertion/insertion.log";
        long start = System.currentTimeMillis();
        processProductRepository.insertAll(processes);
        long duration = System.currentTimeMillis() - start;
        writeLog(logFilename, batchId, processes.size(), duration);
        log.info("Inseridos {} registros do lote {} em tb_process em {} ms", processes.size(), batchId, duration);
    }

    private void writeLog(String fileName, String batchId, int batchSize, long duration) {
        FileLogger.writeLog(fileName,
                String.format(
                        "%s - Lote de id %s, tamanho %d, inserido em %d ms",
                        LocalTime.now().format(logTimeFormatter), batchId, batchSize, duration));
    }

}