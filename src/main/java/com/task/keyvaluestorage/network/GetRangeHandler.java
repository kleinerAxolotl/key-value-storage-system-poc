package com.task.keyvaluestorage.network;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import java.io.IOException;
import java.util.Map;

public class GetRangeHandler extends BaseRequestHandler implements HttpHandler {

    public GetRangeHandler(final KeyValueStoreCore store) {
        super(store);
    }

    public void handle(HttpExchange exchange) throws IOException {
        if (!GET_OPERATION.equalsIgnoreCase(exchange.getRequestMethod())) {
            sendMethodNotAllowedResponse(exchange);
            return;
        }

        var params = queryToMap(exchange.getRequestURI().getQuery());
        var startKey = params.get("startKey");
        var endKey = params.get("endKey");

        if (startKey == null || endKey == null) {
            sendResponse(exchange, 400, "Missing startKey or endKey");
            return;
        }

        final Map<String, String> range = store.getRange(startKey, endKey);
        sendResponse(exchange, 200, range.toString());
    }
}