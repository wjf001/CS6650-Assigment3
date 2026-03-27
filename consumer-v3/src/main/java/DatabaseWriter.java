import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseWriter {

    private final MongoCollection<Document> collection;
    private final BlockingQueue<Document> buffer;
    private final int batchSize;
    private final int flushIntervalMs;
    private final int writerThreads;
    private final Gson gson = new Gson();

    // Metrics
    private final AtomicLong totalWritten   = new AtomicLong(0);
    private final AtomicLong totalFailed    = new AtomicLong(0);
    private final AtomicLong totalBatches   = new AtomicLong(0);
    private final AtomicLong totalDuplicates = new AtomicLong(0);

    private ExecutorService writerPool;
    private ScheduledExecutorService flushScheduler;
    private volatile boolean running = true;

    public DatabaseWriter(MongoCollection<Document> collection,
                          int batchSize, int flushIntervalMs, int writerThreads) {
        this.collection     = collection;
        this.batchSize      = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.writerThreads  = writerThreads;
        this.buffer         = new LinkedBlockingQueue<>(100000); // backpressure at 100K
    }

    public void start() {
        // Writer threads drain buffer and write batches
        writerPool = Executors.newFixedThreadPool(writerThreads);
        for (int i = 0; i < writerThreads; i++) {
            writerPool.submit(this::writerLoop);
        }

        // Flush scheduler ensures partial batches are written even under low load
        flushScheduler = Executors.newSingleThreadScheduledExecutor();
        flushScheduler.scheduleAtFixedRate(this::flushBuffer,
                flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        System.out.println("[DatabaseWriter] Started | batchSize=" + batchSize
                + " | flushInterval=" + flushIntervalMs + "ms"
                + " | writerThreads=" + writerThreads);
    }

    public boolean enqueue(ChatMessage msg) {
        Document doc = new Document()
                .append("messageId", msg.messageId)
                .append("roomId", msg.roomId)
                .append("userId", msg.userId)
                .append("username", msg.username)
                .append("message", msg.message)
                .append("messageType", msg.messageType)
                .append("timestamp", msg.timestamp)
                .append("serverId", msg.serverId)
                .append("clientIp", msg.clientIp);

        return buffer.offer(doc);
    }

    private void writerLoop() {
        List<Document> batch = new ArrayList<>(batchSize);

        while (running || !buffer.isEmpty()) {
            try {
                // Block until at least one message is available
                Document first = buffer.poll(500, TimeUnit.MILLISECONDS);
                if (first == null) continue;

                batch.add(first);

                // Drain up to batchSize
                buffer.drainTo(batch, batchSize - 1);

                // Write batch to MongoDB
                writeBatch(batch);
                batch.clear();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[DatabaseWriter] Writer error: " + e.getMessage());
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            writeBatch(batch);
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;

        List<Document> batch = new ArrayList<>(batchSize);
        buffer.drainTo(batch, batchSize);

        if (!batch.isEmpty()) {
            writeBatch(batch);
        }
    }

    private void writeBatch(List<Document> batch) {
        if (batch.isEmpty()) return;

        try {
            // ordered(false): continues if error
            collection.insertMany(batch, new InsertManyOptions().ordered(false));
            totalWritten.addAndGet(batch.size());
            totalBatches.incrementAndGet();

        } catch (com.mongodb.MongoBulkWriteException e) {
            int inserted = e.getWriteResult().getInsertedCount();
            int duplicates = batch.size() - inserted;
            totalWritten.addAndGet(inserted);
            totalDuplicates.addAndGet(duplicates);
            totalBatches.incrementAndGet();

        } catch (Exception e) {
            System.err.println("[DatabaseWriter] Batch write failed (" + batch.size()
                    + " msgs): " + e.getMessage());
            totalFailed.addAndGet(batch.size());
        }
    }

    public void shutdown() {
        running = false;
        flushScheduler.shutdown();
        writerPool.shutdown();
        try {
            writerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flushBuffer();
        System.out.println("[DatabaseWriter] Shutdown. Written=" + totalWritten.get()
                + " | Failed=" + totalFailed.get()
                + " | Duplicates=" + totalDuplicates.get());
    }

    public long getTotalWritten()    { return totalWritten.get(); }
    public long getTotalFailed()     { return totalFailed.get(); }
    public long getTotalBatches()    { return totalBatches.get(); }
    public long getTotalDuplicates() { return totalDuplicates.get(); }
    public int  getBufferSize()      { return buffer.size(); }
}