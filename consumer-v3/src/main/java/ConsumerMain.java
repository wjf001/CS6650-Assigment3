import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.*;
import com.sun.net.httpserver.HttpServer;
import org.bson.Document;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerMain extends WebSocketServer {

    private static final String RABBITMQ_HOST     = "172.31.21.136";
    private static final String MONGODB_HOST      = "172.31.24.204";  // MongoDB private IP
    private static final String EXCHANGE_NAME     = "chat.exchange";
    private static final int    NUM_ROOMS         = 20;
    private static final int    CHANNEL_POOL_SIZE = 50;
    private static final int    HEALTH_CHECK_PORT = 8081;
    private static final int    METRICS_API_PORT  = 8082;

    private static final int THREADS_PER_ROOM  = 4;
    private static final int PREFETCH_COUNT    = 25;
    private static final int DB_BATCH_SIZE     = 1000;   // Test: 100, 500, 1000, 5000
    private static final int DB_FLUSH_INTERVAL = 500;    // Test: 100, 500, 1000 ms
    private static final int DB_WRITER_THREADS = 4;

    private static final Gson gson = new Gson();
    private static String consumerId;
    private static Connection rabbitConnection;
    private static ChannelPool channelPool;
    private static MongoClient mongoClient;
    private static MongoDatabase mongoDb;

    private static RoomManager roomManager;
    private static ConsumerPool consumerPool;
    private static DatabaseWriter dbWriter;

    public static final AtomicLong messagesProcessed = new AtomicLong(0);
    public static final AtomicLong messagesFailed    = new AtomicLong(0);
    public static final AtomicLong messagesPublished = new AtomicLong(0);

    public ConsumerMain(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String path = handshake.getResourceDescriptor();
        if (path == null || !path.startsWith("/chat/")) {
            conn.close(1002, "Invalid endpoint. Use /chat/{roomId}");
            return;
        }
        String roomId = path.substring(6).trim();
        if (roomId.isEmpty()) { conn.close(1002, "Missing roomId"); return; }

        conn.setAttachment(roomId);
        roomManager.addClient(roomId, conn);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        roomManager.removeClient(conn);
    }

    @Override
    public void onMessage(WebSocket conn, String rawMessage) {
        try {
            String roomId = conn.getAttachment();
            if (roomId == null) return;

            ChatMessage msg = gson.fromJson(rawMessage, ChatMessage.class);
            if (msg == null) return;

            msg.messageId = UUID.randomUUID().toString();
            msg.roomId    = roomId;
            msg.serverId  = consumerId;
            msg.clientIp  = conn.getRemoteSocketAddress().getAddress().getHostAddress();
            msg.timestamp = Instant.now().toString();

            Channel channel = channelPool.borrowChannel();
            try {
                channel.basicPublish(EXCHANGE_NAME, "room." + roomId,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
                messagesPublished.incrementAndGet();
            } finally {
                channelPool.returnChannel(channel);
            }
        } catch (Exception e) {
            System.err.println("[Consumer] Error publishing: " + e.getMessage());
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[Consumer] WS error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[Consumer] WebSocket server listening on port 8080");
    }


    private static void declareQueues(Connection connection) throws Exception {
        Channel ch = connection.createChannel();
        ch.exchangeDeclare(EXCHANGE_NAME, "topic", true);

        String dlxExchange = "chat.dlx";
        String dlqQueue    = "chat.dead-letter-queue";
        ch.exchangeDeclare(dlxExchange, "fanout", true);
        ch.queueDeclare(dlqQueue, true, false, false, null);
        ch.queueBind(dlqQueue, dlxExchange, "");

        for (int i = 1; i <= NUM_ROOMS; i++) {
            String queueName  = "room-queue-" + i;
            String routingKey = "room." + i;
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", dlxExchange);
            ch.queueDeclare(queueName, true, false, false, queueArgs);
            ch.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
        ch.close();
        System.out.println("[Consumer] All " + NUM_ROOMS + " queues declared with DLQ.");
    }


    private static void startHealthCheckServer() {
        Thread healthThread = new Thread(() -> {
            try {
                HttpServer httpServer = HttpServer.create(new InetSocketAddress(HEALTH_CHECK_PORT), 0);
                httpServer.createContext("/health", exchange -> {
                    String status = String.format(
                            "{\"status\":\"UP\",\"consumerId\":\"%s\","
                                    + "\"processed\":%d,\"published\":%d,\"failed\":%d,"
                                    + "\"clients\":%d,\"consumers\":%d,"
                                    + "\"dbWritten\":%d,\"dbFailed\":%d,\"dbBuffer\":%d}",
                            consumerId, messagesProcessed.get(),
                            messagesPublished.get(), messagesFailed.get(),
                            roomManager.getTotalClients(),
                            consumerPool.getTotalConsumers(),
                            dbWriter.getTotalWritten(), dbWriter.getTotalFailed(),
                            dbWriter.getBufferSize()
                    );
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, status.getBytes().length);
                    exchange.getResponseBody().write(status.getBytes());
                    exchange.getResponseBody().close();
                });
                httpServer.setExecutor(Executors.newSingleThreadExecutor());
                httpServer.start();
                System.out.println("[Consumer] Health check on port " + HEALTH_CHECK_PORT);
            } catch (Exception e) {
                System.err.println("[Consumer] Health check failed: " + e.getMessage());
            }
        });
        healthThread.setDaemon(true);
        healthThread.start();
    }

    private static void startMetricsReporter() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.printf("[Consumer] Metrics | processed=%d | published=%d | failed=%d"
                            + " | clients=%d | dbWritten=%d | dbBuffer=%d%n",
                    messagesProcessed.get(), messagesPublished.get(),
                    messagesFailed.get(), roomManager.getTotalClients(),
                    dbWriter.getTotalWritten(), dbWriter.getBufferSize());
        }, 5, 5, TimeUnit.SECONDS);
    }


    public static void main(String[] args) throws Exception {
        consumerId = java.net.InetAddress.getLocalHost().getHostAddress();
        System.out.println("[Consumer-v3] Starting. ConsumerId=" + consumerId);

        // 1. Connect to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        rabbitConnection = factory.newConnection();
        System.out.println("[Consumer-v3] Connected to RabbitMQ at " + RABBITMQ_HOST);

        // 2. Connect to MongoDB
        mongoClient = MongoClients.create("mongodb://" + MONGODB_HOST + ":27017");
        mongoDb = mongoClient.getDatabase("chatdb");
        MongoCollection<Document> messagesCollection = mongoDb.getCollection("messages");
        System.out.println("[Consumer-v3] Connected to MongoDB at " + MONGODB_HOST);

        // 3. Declare queues
        declareQueues(rabbitConnection);

        // 4. Initialize channel pool
        channelPool = new ChannelPool(rabbitConnection, CHANNEL_POOL_SIZE);
        channelPool.init();

        // 5. Initialize Room Manager
        roomManager = new RoomManager(NUM_ROOMS);

        // 6. Initialize Database Writer
        dbWriter = new DatabaseWriter(messagesCollection, DB_BATCH_SIZE, DB_FLUSH_INTERVAL, DB_WRITER_THREADS);
        dbWriter.start();
        System.out.println("[Consumer-v3] DatabaseWriter started | batch=" + DB_BATCH_SIZE
                + " | flush=" + DB_FLUSH_INTERVAL + "ms | threads=" + DB_WRITER_THREADS);

        // 7. Start WebSocket server
        ConsumerMain app = new ConsumerMain(8080);
        app.start();

        // 8. Start Consumer Pool (with DB writer)
        consumerPool = new ConsumerPool(
                rabbitConnection, roomManager, dbWriter, NUM_ROOMS,
                messagesProcessed, messagesFailed, PREFETCH_COUNT
        );
        consumerPool.start(THREADS_PER_ROOM);

        // 9. Start health check
        startHealthCheckServer();
        startMetricsReporter();

        // 10. Start Metrics API
        MetricsAPI metricsApi = new MetricsAPI(mongoDb, dbWriter, METRICS_API_PORT);
        metricsApi.start();

        System.out.println("[Consumer-v3] Fully started.");
        System.out.println("[Consumer-v3] Metrics API at http://localhost:" + METRICS_API_PORT + "/api/metrics");

        // Shutdown hook for clean DB flush
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Consumer-v3] Shutting down...");
            dbWriter.shutdown();
            mongoClient.close();
        }));

        Thread.currentThread().join();
    }
}