package com.task.keyvaluestorage.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.task.keyvaluestorage.network.BaseRequestHandler.DELETE_OPERATION;
import static com.task.keyvaluestorage.network.BaseRequestHandler.GET_OPERATION;
import static com.task.keyvaluestorage.network.BaseRequestHandler.POST_OPERATION;
import static com.task.keyvaluestorage.network.BaseRequestHandler.PUT_OPERATION;

public class LeaderNode {

    public static final int LEADER_PORT = 8080;
    private static final int HEARTBEAT_INTERVAL = 5000;
    private static final int HEARTBEAT_TIMEOUT = 10000;
    private static final List<String> REPLICAS = new CopyOnWriteArrayList<>();
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();

    public void startLeader() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(LEADER_PORT), 0);
        server.createContext("/register", this::registerReplica);
        server.createContext("/heartbeat", this::handleHeartbeat);
        server.setExecutor(executor);
        server.start();
        System.out.println("Leader running on port " + LEADER_PORT);
        startHeartbeatMonitor();
    }

    public void replicateData(String data, String path) {
        for (String replica : REPLICAS) {
            executor.submit(() -> {
                try {
                    switch (path) {
                        case "/put", "/putBatch" -> replicateData(data, PUT_OPERATION, path, replica);
                        case "/delete" -> replicateData(data, DELETE_OPERATION, path, replica);
                    }
                } catch (IOException e) {
                    System.err.println("Failed to sync to replica: " + replica);
                }
            });
        }
    }

    private static void replicateData(final String data, String operation, String path, final String replica) throws IOException {
        var url = new URL(replica + path);
        var conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(operation);
        conn.setDoOutput(true);
        //TODO: parse data accordingly, payload and query params
        try (OutputStream os = conn.getOutputStream()) {
            os.write(data.getBytes());
        }
        conn.getResponseCode();
    }


    private void startHeartbeatMonitor() {
        executor.submit(() -> {
            while (true) {
                var currentTime = System.currentTimeMillis();
                lastHeartbeat.entrySet().removeIf(entry -> currentTime - entry.getValue() > HEARTBEAT_TIMEOUT);
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException ignored) {
                }
            }
        });
    }

    private void registerReplica(HttpExchange exchange) throws IOException {
        if (POST_OPERATION.equals(exchange.getRequestMethod())) {
            var reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            var replicaUrl = reader.readLine();
            REPLICAS.add(replicaUrl);
            lastHeartbeat.put(replicaUrl, System.currentTimeMillis());
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
            System.out.println("Registered replica: " + replicaUrl);
        }
    }

    private void handleHeartbeat(HttpExchange exchange) throws IOException {
        if (GET_OPERATION.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        }
    }

}
