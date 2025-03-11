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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.task.keyvaluestorage.network.BaseRequestHandler.GET_OPERATION;
import static com.task.keyvaluestorage.network.BaseRequestHandler.POST_OPERATION;

public class ReplicaNode {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final int HEARTBEAT_INTERVAL = 5000;

    private String currentLeader;
    private int replicaPort;

    public void startReplica(int replicaPort, int leaderPort, final String leaderUrl) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(replicaPort), 0);
        server.createContext("/heartbeat", this::handleHeartbeat);
        server.createContext("/leaderChange", this::leaderChanged);
        server.setExecutor(executor);

        setLeaderData(replicaPort, leaderPort, leaderUrl);

        server.start();
        System.out.println("Replica running on port " + replicaPort);
        registerWithLeader();
        startHeartbeatSender();
    }

    private void setLeaderData(final int replicaPort, final int leaderPort, final String leaderUrl) {
        this.replicaPort = replicaPort;
        this.currentLeader = leaderUrl + leaderPort;
    }

    private void handleHeartbeat(HttpExchange exchange) throws IOException {
        if (GET_OPERATION.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        }
    }

    private void leaderChanged(HttpExchange exchange) throws IOException {
        if (POST_OPERATION.equals(exchange.getRequestMethod())) {
            var reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            var data = reader.readLine();
            //TODO: parse to new leader host and port payload
            //setLeaderData();
            registerWithLeader();
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        }
    }

    private void registerWithLeader() {
        try {
            var url = new URL(this.currentLeader + "/register");
            var conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(POST_OPERATION);
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(("http://localhost:" + this.replicaPort).getBytes());
            }
            conn.getResponseCode();
        } catch (IOException e) {
            System.err.println("Failed to register with leader.");
        }
    }

    private void startHeartbeatSender() {
        executor.submit(() -> {
            while (true) {
                try {
                    var url = new URL(this.currentLeader + "/heartbeat");
                    var conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod(GET_OPERATION);
                    conn.getResponseCode();
                } catch (IOException e) {
                    System.err.println("Leader heartbeat failed.");
                    //TODO: new leader is elected after n failures
                }
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException ignored) {
                }
            }
        });
    }

}
