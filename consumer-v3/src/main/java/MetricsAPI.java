import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.sun.net.httpserver.HttpServer;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;

public class MetricsAPI {

    private final MongoCollection<Document> collection;
    private final DatabaseWriter dbWriter;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final int port;

    public MetricsAPI(MongoDatabase database, DatabaseWriter dbWriter, int port) {
        this.collection = database.getCollection("messages");
        this.dbWriter   = dbWriter;
        this.port       = port;
    }

    public void start() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/api/messages",    this::handleRoomMessages);
        server.createContext("/api/user/messages", this::handleUserMessages);
        server.createContext("/api/activeusers", this::handleActiveUsers);
        server.createContext("/api/user/rooms",  this::handleUserRooms);

        server.createContext("/api/analytics/summary",  this::handleAnalyticsSummary);
        server.createContext("/api/analytics/topusers",  this::handleTopUsers);
        server.createContext("/api/analytics/toprooms",  this::handleTopRooms);

        server.createContext("/api/metrics", this::handleAllMetrics);

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("[MetricsAPI] Running on port " + port);
    }

    private void handleRoomMessages(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String roomId = params.getOrDefault("roomId", "1");
            String start  = params.getOrDefault("start",
                    Instant.now().minus(1, ChronoUnit.HOURS).toString());
            String end    = params.getOrDefault("end", Instant.now().toString());

            long t0 = System.currentTimeMillis();
            List<Document> messages = collection.find(
                            Filters.and(
                                    Filters.eq("roomId", roomId),
                                    Filters.gte("timestamp", start),
                                    Filters.lte("timestamp", end)
                            ))
                    .sort(Sorts.ascending("timestamp"))
                    .limit(1000)
                    .into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "messages_by_room");
            result.put("roomId", roomId);
            result.put("count", messages.size());
            result.put("queryTimeMs", elapsed);
            result.put("messages", messages);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleUserMessages(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String userId = params.getOrDefault("userId", "1");

            long t0 = System.currentTimeMillis();
            List<Document> messages = collection.find(Filters.eq("userId", userId))
                    .sort(Sorts.descending("timestamp"))
                    .limit(1000)
                    .into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "user_message_history");
            result.put("userId", userId);
            result.put("count", messages.size());
            result.put("queryTimeMs", elapsed);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleActiveUsers(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String start = params.getOrDefault("start",
                    Instant.now().minus(1, ChronoUnit.HOURS).toString());
            String end   = params.getOrDefault("end", Instant.now().toString());

            long t0 = System.currentTimeMillis();
            List<String> distinctUsers = collection.distinct("userId", String.class)
                    .filter(Filters.and(
                            Filters.gte("timestamp", start),
                            Filters.lte("timestamp", end)))
                    .into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "active_users");
            result.put("uniqueUserCount", distinctUsers.size());
            result.put("queryTimeMs", elapsed);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleUserRooms(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String userId = params.getOrDefault("userId", "1");

            long t0 = System.currentTimeMillis();
            List<Document> rooms = collection.aggregate(Arrays.asList(
                    Aggregates.match(Filters.eq("userId", userId)),
                    Aggregates.group("$roomId",
                            Accumulators.max("lastActivity", "$timestamp"),
                            Accumulators.sum("messageCount", 1)),
                    Aggregates.sort(Sorts.descending("lastActivity"))
            )).into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "user_rooms");
            result.put("userId", userId);
            result.put("roomCount", rooms.size());
            result.put("queryTimeMs", elapsed);
            result.put("rooms", rooms);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }


    private void handleAnalyticsSummary(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            long t0 = System.currentTimeMillis();
            long totalMessages = collection.countDocuments();

            List<Document> typeStats = collection.aggregate(Arrays.asList(
                    Aggregates.group("$messageType", Accumulators.sum("count", 1))
            )).into(new ArrayList<>());

            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "analytics_summary");
            result.put("totalMessages", totalMessages);
            result.put("messagesByType", typeStats);
            result.put("dbWriterStats", Map.of(
                    "totalWritten", dbWriter.getTotalWritten(),
                    "totalFailed", dbWriter.getTotalFailed(),
                    "totalDuplicates", dbWriter.getTotalDuplicates(),
                    "totalBatches", dbWriter.getTotalBatches(),
                    "bufferSize", dbWriter.getBufferSize()
            ));
            result.put("queryTimeMs", elapsed);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleTopUsers(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            int n = Integer.parseInt(params.getOrDefault("n", "10"));

            long t0 = System.currentTimeMillis();
            List<Document> topUsers = collection.aggregate(Arrays.asList(
                    Aggregates.group("$userId", Accumulators.sum("messageCount", 1)),
                    Aggregates.sort(Sorts.descending("messageCount")),
                    Aggregates.limit(n)
            )).into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "top_users");
            result.put("topN", n);
            result.put("queryTimeMs", elapsed);
            result.put("users", topUsers);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleTopRooms(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            int n = Integer.parseInt(params.getOrDefault("n", "10"));

            long t0 = System.currentTimeMillis();
            List<Document> topRooms = collection.aggregate(Arrays.asList(
                    Aggregates.group("$roomId", Accumulators.sum("messageCount", 1)),
                    Aggregates.sort(Sorts.descending("messageCount")),
                    Aggregates.limit(n)
            )).into(new ArrayList<>());
            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", "top_rooms");
            result.put("topN", n);
            result.put("queryTimeMs", elapsed);
            result.put("rooms", topRooms);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleAllMetrics(com.sun.net.httpserver.HttpExchange exchange) {
        try {
            long t0 = System.currentTimeMillis();

            String now   = Instant.now().toString();
            String start = Instant.now().minus(1, ChronoUnit.HOURS).toString();

            // Total messages
            long totalMessages = collection.countDocuments();

            // Active users (last hour)
            List<String> activeUsers = collection.distinct("userId", String.class)
                    .filter(Filters.gte("timestamp", start))
                    .into(new ArrayList<>());

            // Messages by type
            List<Document> typeStats = collection.aggregate(Arrays.asList(
                    Aggregates.group("$messageType", Accumulators.sum("count", 1))
            )).into(new ArrayList<>());

            // Top 10 users
            List<Document> topUsers = collection.aggregate(Arrays.asList(
                    Aggregates.group("$userId", Accumulators.sum("messageCount", 1)),
                    Aggregates.sort(Sorts.descending("messageCount")),
                    Aggregates.limit(10)
            )).into(new ArrayList<>());

            // Top 10 rooms
            List<Document> topRooms = collection.aggregate(Arrays.asList(
                    Aggregates.group("$roomId", Accumulators.sum("messageCount", 1)),
                    Aggregates.sort(Sorts.descending("messageCount")),
                    Aggregates.limit(10)
            )).into(new ArrayList<>());

            // Messages per room
            long room1Count = collection.countDocuments(Filters.eq("roomId", "1"));

            long elapsed = System.currentTimeMillis() - t0;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("totalMessages", totalMessages);
            result.put("activeUsersLastHour", activeUsers.size());
            result.put("messagesByType", typeStats);
            result.put("topUsers", topUsers);
            result.put("topRooms", topRooms);
            result.put("room1MessageCount", room1Count);
            result.put("dbWriter", Map.of(
                    "written", dbWriter.getTotalWritten(),
                    "failed", dbWriter.getTotalFailed(),
                    "duplicates", dbWriter.getTotalDuplicates(),
                    "batches", dbWriter.getTotalBatches(),
                    "bufferSize", dbWriter.getBufferSize()
            ));
            result.put("totalQueryTimeMs", elapsed);

            sendJson(exchange, result);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }


    private Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null) return params;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) params.put(kv[0], kv[1]);
        }
        return params;
    }

    private void sendJson(com.sun.net.httpserver.HttpExchange exchange, Object data) {
        try {
            String json = gson.toJson(data);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, json.getBytes().length);
            exchange.getResponseBody().write(json.getBytes());
            exchange.getResponseBody().close();
        } catch (Exception e) {
            System.err.println("[MetricsAPI] Send error: " + e.getMessage());
        }
    }

    private void sendError(com.sun.net.httpserver.HttpExchange exchange, Exception e) {
        try {
            String json = gson.toJson(Map.of("error", e.getMessage()));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(500, json.getBytes().length);
            exchange.getResponseBody().write(json.getBytes());
            exchange.getResponseBody().close();
        } catch (Exception ex) {
            System.err.println("[MetricsAPI] Error sending error: " + ex.getMessage());
        }
    }
}