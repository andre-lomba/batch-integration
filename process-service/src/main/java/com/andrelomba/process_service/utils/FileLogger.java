package com.andrelomba.process_service.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileLogger {

  public static void writeLog(String fileName, String message) {
    Path logPath = Paths.get(String.format("logs/%s", fileName));
    try {
      Files.createDirectories(logPath.getParent());
      Files.writeString(
          logPath,
          message + System.lineSeparator(),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Erro ao escrever no arquivo de log %s", fileName), e);
    }
  }
}
