package com.task.keyvaluestorage.network;

import com.sun.net.httpserver.HttpExchange;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BaseRequestHandler {

    public static final String GET_OPERATION = "GET";
    public static final String POST_OPERATION = "POST";
    public static final String PUT_OPERATION = "PUT";
    public static final String DELETE_OPERATION = "DELETE";

    protected KeyValueStoreCore store;

    public BaseRequestHandler(final KeyValueStoreCore store) {
        this.store = store;
    }

    protected static void sendMethodNotAllowedResponse(final HttpExchange exchange) throws IOException {
        sendResponse(exchange, 405, "Method Not Allowed");
    }

    protected static void sendMissingKeyResponse(final HttpExchange exchange) throws IOException {
        sendResponse(exchange, 400, "Missing key");
    }

    protected static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.sendResponseHeaders(statusCode, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes(StandardCharsets.UTF_8));
        os.close();
    }

    protected static Map<String, String> queryToMap(String query) {
        var result = new HashMap<String, String>();
        if (query == null) {
            return result;
        }

        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length > 1) {
                result.put(pair[0], pair[1]);
            }
        }
        return result;
    }
}
