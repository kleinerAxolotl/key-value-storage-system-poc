package com.task.keyvaluestorage.network;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import java.io.IOException;

public class PutHandler extends BaseRequestHandler implements HttpHandler {

    public PutHandler(final KeyValueStoreCore store) {
        super(store);
    }

    public void handle(HttpExchange exchange) throws IOException {
        if (!PUT_OPERATION.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendMethodNotAllowedResponse(exchange);
                return;
            }

            var params = queryToMap(exchange.getRequestURI().getQuery());
            var key = params.get("key");
            var value = params.get("value");

            if (key == null || value == null) {
                sendResponse(exchange, 400, "Missing key or value");
                return;
            }

            store.put(key, value);
            sendResponse(exchange, 200, "Stored " + key);
        }

}
