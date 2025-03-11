package com.task.keyvaluestorage;

import com.sun.net.httpserver.HttpServer;
import com.task.keyvaluestorage.core.KeyValueStoreCore;
import com.task.keyvaluestorage.network.DeleteHandler;
import com.task.keyvaluestorage.network.GetHandler;
import com.task.keyvaluestorage.network.GetRangeHandler;
import com.task.keyvaluestorage.network.PutBatchHandler;
import com.task.keyvaluestorage.network.PutHandler;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;

public class KeyValueStorageServer {

    private static final int PORT = 8080;

    public static void main(String[] args) throws Exception {
        //var storageDir = System.getProperty("user.home");
        var separator = java.io.File.separator;
        System.out.println("detected path separator " + separator);
        var storageDir = getStorageExecutionDirectory() + separator;
        System.out.println("exec dir " + storageDir);

        // TODO: accept the file names and paths thru the args?
        var store = new KeyValueStoreCore(storageDir + "storage.db", storageDir + "storage.index", storageDir + "recovery_logs/");
        var server = HttpServer.create(new InetSocketAddress(PORT), 0);

        // add the handlers for every endpoint
        server.createContext("/get", new GetHandler(store));
        server.createContext("/getRange", new GetRangeHandler(store));
        server.createContext("/put", new PutHandler(store));
        server.createContext("/putBatch", new PutBatchHandler(store));
        server.createContext("/delete", new DeleteHandler(store));

        // server will use multiple threads to handle incoming requests
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();

        System.out.println("Key Value Storage started on port " + PORT);
    }

    /**
     * If directory is running through and IDE will return the project directory, otherwise if running from a jar file will return the directory where the jar
     * is located.
     *
     * @return the current directory where the project is running
     */
    private static Path getStorageExecutionDirectory() throws Exception {
        // try to get the directory of the running jar
        var codeSourceLocation = new File(KeyValueStorageServer.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        // check if running from a jar
        if (codeSourceLocation.isFile() && codeSourceLocation.getName().endsWith(".jar")) {
            // jar's directory
            return codeSourceLocation.toPath().getParent();
        }

        // if not a jar, assume running in an IDE (e.g., project root)
        // current working directory
        return new File("").toPath().toAbsolutePath();
    }
}
