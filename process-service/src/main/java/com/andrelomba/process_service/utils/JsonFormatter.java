package com.andrelomba.process_service.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonFormatter {

  public static String extractValueFromJson(String json, String key) {
    JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
    return jsonObject.has(key) ? jsonObject.get(key).getAsString() : null;
  }
}
