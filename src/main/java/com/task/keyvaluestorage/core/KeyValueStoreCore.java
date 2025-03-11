package com.task.keyvaluestorage.core;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KeyValueStoreCore {

    private static final String DELETED_ENTRY = "DELETED||KEY|VALUE";
    //TODO: increase, a bit small and might not be optimal for high-throughput workloads, maybe to 100. :o
    private static final int BATCH_WRITE_SIZE = 10;
    private static final int MAX_LOG_FILE_SIZE = 1024 * 1024; // 1MB before rotating
    public static final String ACCESS_MODE_RW = "rw"; // read and write

    // more efficient than using hash map,
    // the Map is faster than a tree or skip list algorithm (O(1) instead of log(n)),
    // but you cannot select ranges (everything in between x and y)
    // the tree and skip lists algorithms support this in Log(n) whereas hash indexes can result in a full scan O(n) :O
    // SkipList is a data structure which is sorted and allow fast search concurrently. All the elements are sorted based on their natural sorting order of keys.
    // Navigable map for the index ensures efficient lookups even when the dataset is much larger than available RAM.
    private final ConcurrentNavigableMap<String, Long> index = new ConcurrentSkipListMap<>();

    // to reduce disk I/O
    // trade off, most recent values stored won't be persisted in case of the system going down
    // but since there is a log file that stores this values on the buffer, the data loss won't happen
    // Note: LinkedHashMap could be used to maintain insertion order and ensures that keys are written sequentially, reducing fragmentation making future sequential reads more efficient.
    // since this is used only as a temporal buffer which is not expected to hold a lot of elements, concurrent hash map is better as is thread safe
    private final ConcurrentHashMap<String, String> writeBuffer = new ConcurrentHashMap<>();

    //Rules:
    //Read lock and Write lock which allows a thread to lock the ReadWriteLock either for reading or writing.
    //Read lock: If there is no thread that has requested the write lock and the lock for writing, then multiple threads can lock the lock for reading.
    // It means multiple threads can read the data at the very moment, as long as there’s no thread to write the data or to update the data.
    //Write Lock: If no threads are writing or reading, only one thread at a moment can lock the lock for writing.
    // Other threads have to wait until the lock gets released. It means, only one thread can write the data at the very moment, and other threads have to wait.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    //executors to perform background tasks
    private final ScheduledExecutorService compactionExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService writeBufferFlusher = Executors.newSingleThreadScheduledExecutor();

    // TODO maybe using java.nio would be more efficient? but also harder to implement due non blocking and bytes buffers :|
    // RandomAccessFile behaves like a large array of bytes stored in the file system.
    // The reading from a random access file can be done from any position given by the file pointer, followed by the quantity of bytes to be read and after that increasing the value of the file pointer.
    //file that holds all the key value records
    private RandomAccessFile dataFile;

    //write ahead logging provide durability guarantee without the storage data structures to be flushed to disk
    private RandomAccessFile logFile;

    //used to add a suffix on the log files being rotated
    private int logFilesCounter = 1;

    private final String indexFileName;
    private final String dataFileName;
    private String logFileName;

    // log files are rotated to ensure a file does not grow so big that can cause problems
    //"recovery_logs/";
    private final String logsDir;

    public KeyValueStoreCore(final String dataFileName, final String indexFileName, final String logsDir) throws IOException {
        this.indexFileName = indexFileName;
        this.dataFileName = dataFileName;
        this.logsDir = logsDir;
        this.dataFile = new RandomAccessFile(dataFileName, ACCESS_MODE_RW);

        loadIndex(indexFileName);

        //create/load the log files folder
        if (new File(logsDir).mkdirs()) {
            System.out.println("logs directory created: " + logsDir);
        }
        recoverFromLogFiles();
        rotateLogFile();

        //start background housekeeping tasks
        startCompactionThread();
        starWriteBufferFlusher();
    }


    /**
     * Adds the key and the value to the storage. if the key already exists, replaces the existing value for the new provided.
     *
     * @param key   A given key. it should not be null.
     * @param value A given value. it should not be null.
     */
    public void put(final String key, final String value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("provided key should not be null");
        }

        if (value == null) {
            throw new IllegalArgumentException("provided value should not be null");
        }
        put(key, value, true);
    }


    /**
     * Get the value of a given key. if the values is present on the buffer, it will retrieve it from there and won't go to the disk, otherwise retrieves the
     * position of the given key on the index map and then reads the key followed by the value, which is then retrieved from the disk file.
     *
     * @param key They key to retrieve the value for.
     * @return The value associated to the key.
     */
    public String get(final String key) throws IOException {

        if (key == null) {
            return null;
        }

        // if present on the buffer, won't be on disk
        if (writeBuffer.containsKey(key)) {
            return writeBuffer.get(key);
        }

        lock.readLock().lock();
        try {
            var position = index.get(key);
            if (position == null) {
                return null;
            }

            //moves the file pointer directly to the requested offset
            dataFile.seek(position);
            //read the key
            dataFile.readUTF();
            //read and return the value
            return dataFile.readUTF();
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * Adds the list of keys and their corresponding values to the storage. The size of the keys and values should be the same, otherwise a
     * IllegalArgumentException will be thrown.
     *
     * @param keys   list containing the keys, should have the same length as values.
     * @param values list containing the values, should have the same length as keys.
     */
    public void batchPut(List<String> keys, List<String> values) throws IOException {
        var containsNullKeys = keys.stream().anyMatch(Objects::isNull);
        if (containsNullKeys) {
            throw new IllegalArgumentException("provided keys should not be null");
        }

        var containsNullValues = values.stream().anyMatch(Objects::isNull);
        if (containsNullValues) {
            throw new IllegalArgumentException("provided values should not be null");
        }

        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys and values must be same size");
        }

        lock.writeLock().lock();
        try {
            for (int i = 0; i < keys.size(); i++) {
                writeBuffer.put(keys.get(i), values.get(i));
                logFile.writeUTF(keys.get(i));
                logFile.writeUTF(values.get(i));
            }
            logFile.getFD().sync();

            if (writeBuffer.size() >= BATCH_WRITE_SIZE) {
                flushWriteBuffer();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves a map containing the entry sets of the given stat key and end key. Start and end key are included on the result. Important to note that the
     * keys are handled as strings, so the retrieval of numeric keys would be according to the order of their string representation.
     *
     * @param startKey The key start of the range.
     * @param endKey   The key up to what the range will be retrieved.
     * @return A LinkedHashMap containing the results. LinkedHashMap will maintain the order of the range.
     */
    public Map<String, String> getRange(String startKey, String endKey) throws IOException {
        lock.readLock().lock();
        try {
            //maintains the order
            var result = new LinkedHashMap<String, String>();
            for (String key : index.subMap(startKey, true, endKey, true).keySet()) {
                result.put(key, get(key));
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Deletes the key and the value of the given key.
     *
     * @param key The key to remove along with its value.
     */
    public void delete(final String key) throws IOException {
        //remove from the buffer if contained there
        writeBuffer.remove(key);

        lock.writeLock().lock();
        try {
            //remove the key from the index and its position
            //when the dataFile is compacted, as the index has not the position anymore, the new file won't contain the key neither its value
            index.remove(key);

            //mark the value as DELETED
            //as the write buffer does not have the key anymore it won't be flushed from the log to the datafile
            logFile.seek(logFile.length());
            logFile.writeUTF(key);
            logFile.writeUTF(DELETED_ENTRY);
            logFile.getFD().sync();

            persistIndex(this.indexFileName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compact file to remove deleted and overwritten data. Read and write operations still use the active index and write buffer while compaction works on a
     * separate copy of the data file. Since compaction only modifies the data file, the in-memory index (ConcurrentNavigableMap) remains stable.
     */
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            //TODO: provide the compacted.db as configuration on constructor
            var tempFile = new File("compacted.db");
            var newFile = new RandomAccessFile(tempFile, ACCESS_MODE_RW);
            //as it is a side process, no other threads will try to use it and there is no need to be concurrent.
            var newIndex = new TreeMap<String, Long>();

            // only processes the existing data file and index it does not touch the in-memory write buffer.
            for (Map.Entry<String, Long> entry : index.entrySet()) {
                // only processes the current snapshot of the data and does not interfere with new writes.
                dataFile.seek(entry.getValue());
                var key = dataFile.readUTF();
                var value = dataFile.readUTF();

                var newPosition = newFile.getFilePointer();
                newFile.writeUTF(key);
                newFile.writeUTF(value);
                newIndex.put(key, newPosition);
            }

            dataFile.close();
            newFile.close();
            if (!new File(dataFileName).delete()){
                System.err.println("Old datafile could not be deleted by compact process! "+dataFileName);
            }
            if (!tempFile.renameTo(new File(dataFileName))){
                System.err.println("New datafile could not renamed by compact process!");
            }

            this.dataFile = new RandomAccessFile(dataFileName, ACCESS_MODE_RW);
            this.index.clear();
            this.index.putAll(newIndex);
            persistIndex(this.indexFileName);
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Shutdown the housekeeping processes and close the files.
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flushWriteBuffer();
            // stop compact thread
            compactionExecutor.shutdown();
            writeBufferFlusher.shutdown();
            dataFile.close();
            logFile.close();
        } finally {
            lock.writeLock().unlock();
        }
    }


    private void put(final String key, final String value, final boolean useWriteAheadLog) throws IOException {
        writeBuffer.put(key, value);

        //TODO: make more efficient? multiple puts would repeat multiple times the same key on the log file,
        // but on read file only last val would be overwritten on the final buffer map,
        // it will eventually repeat the value on final storage file and cleaned by the compact task
        if (useWriteAheadLog) {
            lock.writeLock().lock();
            try {
                if (logFile.length() > MAX_LOG_FILE_SIZE) {
                    rotateLogFile();
                }
                logFile.seek(logFile.length());
                logFile.writeUTF(key);
                logFile.writeUTF(value);
                logFile.getFD().sync();
            } finally {
                lock.writeLock().unlock();
            }
        }


        if (writeBuffer.size() >= BATCH_WRITE_SIZE) {
            flushWriteBuffer();
        }

    }


    private void clearLogFiles() throws IOException {
        File[] logFiles = new File(logsDir).listFiles();
        if (logFiles == null) {
            return;
        }

        for (File file : logFiles) {
            file.delete();
        }

        logFile = null;
        logFilesCounter = 1;
        rotateLogFile();
    }

    /**
     * Flush buffer to disk
     */
    private void flushWriteBuffer() throws IOException {
        lock.writeLock().lock();
        try {
            //TODO: avoid repeated values? eventually the file will have repeated keys as they are not replaced,
            // on load the index from file only the last position is overwritten in the map :O
            // when flushing to datafile nbo deleted entries will be added
            dataFile.seek(dataFile.length());
            // when the buffer is flushed to disk, it appends new data after the compacted data file without conflicts.
            for (Map.Entry<String, String> entry : writeBuffer.entrySet()) {
                //gets the last position of the file
                var position = dataFile.getFilePointer();
                dataFile.writeUTF(entry.getKey());
                dataFile.writeUTF(entry.getValue());
                index.put(entry.getKey(), position);
            }
            writeBuffer.clear();
            persistIndex(this.indexFileName);
            //clearLogFile();
            clearLogFiles();
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Persist index.
     *
     * @param indexFileName the name of the file that will store the index map.
     */
    private void persistIndex(final String indexFileName) throws IOException {
        try (DataOutputStream indexStream = new DataOutputStream(new FileOutputStream(indexFileName))) {
            for (Map.Entry<String, Long> entry : index.entrySet()) {
                indexStream.writeUTF(entry.getKey());
                indexStream.writeLong(entry.getValue());
            }
        }
    }

    /**
     * Load index from file
     */
    private void loadIndex(final String indexFileName) throws IOException {
        lock.writeLock().lock();
        try {
            var file = new File(indexFileName);
            if (!file.exists()) {
                System.out.println("loadIndex does not exists: " + indexFileName);
                return;
            }

            try (DataInputStream indexStream = new DataInputStream(new FileInputStream(file))) {
                while (indexStream.available() > 0) {
                    var key = indexStream.readUTF();
                    var position = indexStream.readLong();
                    index.put(key, position);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    private void recoverFromLogFiles() throws IOException {
        lock.writeLock().lock();
        try {
            var logFiles = new File(logsDir).listFiles();
            if (logFiles == null) {
                return;
            }

            Arrays.sort(logFiles, Comparator.comparing(File::getName));
            for (File file : logFiles) {
                try (DataInputStream logStream = new DataInputStream(new FileInputStream(file))) {
                    while (logStream.available() > 0) {
                        var key = logStream.readUTF();
                        var value = logStream.readUTF();
                        //we don't want to load deleted entries, as they could be written on the datafile
                        if (!DELETED_ENTRY.equals(value)) {
                            put(key, value, false);
                        }
                    }
                }
                // Remove old log file after recovery
                file.delete();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rotate log files to keep recovery fast.
     */
    private void rotateLogFile() throws IOException {
        if (logFile != null) {
            logFile.close();
        }

        logFileName = logsDir + "log_" + (logFilesCounter++) + ".log";
        logFile = new RandomAccessFile(logFileName, ACCESS_MODE_RW);
    }


    /**
     * Background compaction minimizes interference with operations. The compaction process does not block the main read/write operations because it runs in a
     * separate thread using ScheduledExecutorService.
     */
    private void startCompactionThread() {
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                long currentMillis = System.currentTimeMillis();
                System.out.println("********************** start compaction thread id: " + currentMillis + "**********************");
                compact();
                System.out.println("********************** finish compaction thread id: " +
                                   currentMillis +
                                   "********************** total task time:" +
                                   ((System.currentTimeMillis() - currentMillis) / 1000.0) +
                                   " Seconds");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    /**
     * Reduces write latency and ensures that log files writing does not become a bottleneck by reducing blocking writes.
     */
    private void starWriteBufferFlusher() {
        writeBufferFlusher.scheduleAtFixedRate(() -> {
            try {
                long currentMillis = System.currentTimeMillis();
                System.out.println("********************** start flusher thread id: " + currentMillis + "**********************");
                flushWriteBuffer();
                System.out.println("********************** finish flusher thread id: " +
                                   currentMillis +
                                   "********************** total task time:" +
                                   ((System.currentTimeMillis() - currentMillis) / 1000.0) +
                                   " Seconds");
            } catch (IOException e) {
                e.printStackTrace();
            }
            //TODO: increase for higher throughput maybe to every 5 min :O
        }, 5, 10, TimeUnit.SECONDS);
    }


    //****************** methods to use non rotative log files *****************

    /**
     * Clear LogFile
     */
    private void clearLogFile() throws IOException {
        lock.writeLock().lock();
        try {
            new File(logFileName).delete();
            logFile = new RandomAccessFile(logFileName, ACCESS_MODE_RW);
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Recover from Log File
     */
    private void recoverFromLogFile(final String logFileName) throws IOException {
        lock.writeLock().lock();
        try {
            var file = new File(logFileName);
            if (!file.exists()) {
                System.out.println("recoverFromLogFile does not exists: " + logFileName);
                return;
            }

            try (DataInputStream logStream = new DataInputStream(new FileInputStream(file))) {
                while (logStream.available() > 0) {
                    var key = logStream.readUTF();
                    var value = logStream.readUTF();
                    put(key, value, false);
                }
            }
            file.delete();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
