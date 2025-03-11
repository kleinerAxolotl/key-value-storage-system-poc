package com.task.keyvaluestorage.network;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import java.io.IOException;

public class DeleteHandler extends BaseRequestHandler implements HttpHandler {

    public DeleteHandler(final KeyValueStoreCore store) {
        super(store);
    }

    public void handle(HttpExchange exchange) throws IOException {
        if (!DELETE_OPERATION.equalsIgnoreCase(exchange.getRequestMethod())) {
            sendMethodNotAllowedResponse(exchange);
            return;
        }

        var params = queryToMap(exchange.getRequestURI().getQuery());
        var key = params.get("key");

        if (key == null) {
            sendMissingKeyResponse(exchange);
            return;
        }

        store.delete(key);
        sendResponse(exchange, 200, "Deleted " + key);
    }

}