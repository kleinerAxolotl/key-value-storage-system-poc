package com.task.keyvaluestorage.network;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class PutBatchHandler extends BaseRequestHandler implements HttpHandler {

    public PutBatchHandler(final KeyValueStoreCore store) {
        super(store);
    }

    public void handle(HttpExchange exchange) throws IOException {
        if (!PUT_OPERATION.equalsIgnoreCase(exchange.getRequestMethod())) {
            sendMethodNotAllowedResponse(exchange);
            return;
        }

        var isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
        var br = new BufferedReader(isr);
        var requestBody = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            requestBody.append(line);
        }

        var batch = parseJson(requestBody.toString());
        var keys = batch.get("keys");
        var values = batch.get("values");

        var containsNullKeys = keys.stream().anyMatch(Objects::isNull);
        if (containsNullKeys) {
            sendResponse(exchange, 400, "provided keys should not be null");
            return;
        }

        var containsNullValues = values.stream().anyMatch(Objects::isNull);
        if (containsNullValues) {
            sendResponse(exchange, 400, "provided values should not be null");
            return;
        }

        if (keys.size() != values.size()) {
            sendResponse(exchange, 400, "Keys and values must be same size");
            return;
        }

        store.batchPut(keys, values);
        sendResponse(exchange, 200, "Batch stored");
    }

    private static Map<String, List<String>> parseJson(String json) {

        System.out.println("json:" + json);
        var result = new HashMap<String, List<String>>();

        // Regular expression to match keys and values lists
        var pattern = Pattern.compile("(keys|values):\\[([^\\]]*)]");
        var matcher = pattern.matcher(json);

        while (matcher.find()) {
            // keys or values
            String key = matcher.group(1);
            // Extracted values inside []
            String values = matcher.group(2);

            // Split values by commas, remove quotes, and trim spaces
            var list = new ArrayList<String>();
            for (String value : values.split(",")) {
                list.add(value.trim().replace("\"", ""));
            }
            result.put(key, list);
        }

        return result;
    }

}
