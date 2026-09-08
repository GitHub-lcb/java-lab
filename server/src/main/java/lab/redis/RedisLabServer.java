package lab.redis;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import lab.jvm.JvmProbeService;

public final class RedisLabServer {
    private final RespClient redis;
    private final RedisCommandPolicy policy = new RedisCommandPolicy();
    private final ConcurrentHashMap<String, Set<String>> sessionKeys = new ConcurrentHashMap<String, Set<String>>();
    private final String allowedOrigin;

    private RedisLabServer(RespClient redis, String allowedOrigin) { this.redis = redis; this.allowedOrigin = allowedOrigin; }

    public static void main(String[] args) throws Exception {
        String redisHost = env("LAB_REDIS_HOST", "127.0.0.1");
        int redisPort = Integer.parseInt(env("LAB_REDIS_PORT", "6379"));
        int httpPort = Integer.parseInt(env("LAB_HTTP_PORT", "8787"));
        String origin = env("LAB_ALLOWED_ORIGIN", "local");
        RedisLabServer application = new RedisLabServer(new RespClient(redisHost, redisPort), origin);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", httpPort), 0);
        server.createContext("/api/runtime", application.new RuntimeHandler());
        server.createContext("/api/command", application.new CommandHandler());
        server.createContext("/api/reset", application.new ResetHandler());
        server.createContext("/api/jvm", application.new JvmHandler());
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("Java Redis Lab gateway listening on http://127.0.0.1:" + httpPort);
    }

    private final class RuntimeHandler implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException {
            if (preflight(exchange)) return;
            if (!"GET".equals(exchange.getRequestMethod())) { respond(exchange, 405, error("Method not allowed")); return; }
            boolean connected = false;
            String message;
            try {
                Object reply = redis.execute(Collections.singletonList("PING"));
                connected = "PONG".equals(reply);
                message = connected ? "Java 网关与 Redis 已连接" : "Redis 返回了非预期响应";
            } catch (IOException error) {
                message = "Java 网关已启动，Redis 未连接";
            }
            respond(exchange, 200, "{\"java\":true,\"javaVersion\":" + Json.quote(System.getProperty("java.version")) + ",\"redis\":" + connected + ",\"message\":" + Json.quote(message) + "}");
        }
    }

    private final class CommandHandler implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException {
            if (preflight(exchange)) return;
            if (!"POST".equals(exchange.getRequestMethod())) { respond(exchange, 405, error("Method not allowed")); return; }
            try {
                String session = session(exchange);
                List<String> command = policy.sanitize(session, CommandParser.parse(readBody(exchange, 512)));
                Object result = redis.execute(command);
                if (result instanceof RespClient.RedisError) { respond(exchange, 422, error(((RespClient.RedisError) result).message)); return; }
                Set<String> touched = sessionKeys.get(session);
                if (touched == null) {
                    Set<String> candidate = Collections.synchronizedSet(new HashSet<String>());
                    Set<String> existing = sessionKeys.putIfAbsent(session, candidate);
                    touched = existing == null ? candidate : existing;
                }
                if (touched.size() < 200) touched.addAll(policy.keysOf(command));
                respond(exchange, 200, "{\"ok\":true,\"result\":" + Json.value(result) + "}");
            } catch (IllegalArgumentException error) {
                respond(exchange, 400, error(error.getMessage()));
            } catch (IOException error) {
                respond(exchange, 503, error("Redis is unavailable at the configured host and port"));
            }
        }
    }

    private final class ResetHandler implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException {
            if (preflight(exchange)) return;
            if (!"POST".equals(exchange.getRequestMethod())) { respond(exchange, 405, error("Method not allowed")); return; }
            try {
                String session = session(exchange);
                Set<String> keys = sessionKeys.remove(session);
                if (keys == null || keys.isEmpty()) { respond(exchange, 200, "{\"ok\":true,\"deleted\":0}"); return; }
                List<String> values;
                synchronized (keys) { values = new ArrayList<String>(keys); }
                long deleted = 0;
                for (int start = 0; start < values.size(); start += 50) {
                    List<String> command = new ArrayList<String>();
                    command.add("DEL");
                    command.addAll(values.subList(start, Math.min(values.size(), start + 50)));
                    Object result = redis.execute(command);
                    if (result instanceof Number) deleted += ((Number) result).longValue();
                }
                respond(exchange, 200, "{\"ok\":true,\"deleted\":" + deleted + "}");
            } catch (IllegalArgumentException error) {
                respond(exchange, 400, error(error.getMessage()));
            } catch (IOException error) {
                respond(exchange, 503, error("Redis is unavailable at the configured host and port"));
            }
        }
    }

    private final class JvmHandler implements HttpHandler {
        private final JvmProbeService probes = new JvmProbeService();
        public void handle(HttpExchange exchange) throws IOException {
            if (preflight(exchange)) return;
            if (!"GET".equals(exchange.getRequestMethod())) { respond(exchange, 405, error("Method not allowed")); return; }
            String query = exchange.getRequestURI().getRawQuery();
            String probe = query != null && query.startsWith("probe=") ? query.substring(6) : "";
            if (!probe.matches("[a-z]{2,24}")) { respond(exchange, 400, error("Invalid JVM probe")); return; }
            try { respond(exchange, 200, probes.probe(probe)); }
            catch (IllegalArgumentException error) { respond(exchange, 400, error(error.getMessage())); }
            catch (Exception error) { respond(exchange, 500, error("JVM probe failed: " + error.getMessage())); }
        }
    }

    private String session(HttpExchange exchange) {
        String value = exchange.getRequestHeaders().getFirst("X-Lab-Session");
        policy.sanitize(value, Collections.singletonList("PING"));
        return value;
    }

    private boolean preflight(HttpExchange exchange) throws IOException {
        applyHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(204, -1); exchange.close(); return true; }
        return false;
    }

    private void applyHeaders(HttpExchange exchange) {
        Headers headers = exchange.getResponseHeaders();
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if ("local".equals(allowedOrigin)) {
            if (origin != null && origin.matches("https?://(127\\.0\\.0\\.1|localhost)(:[0-9]+)?")) headers.set("Access-Control-Allow-Origin", origin);
        } else if (allowedOrigin.equals(origin)) headers.set("Access-Control-Allow-Origin", origin);
        headers.set("Vary", "Origin");
        headers.set("Access-Control-Allow-Headers", "Content-Type, X-Lab-Session");
        headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        headers.set("Cache-Control", "no-store");
        headers.set("Content-Type", "application/json; charset=utf-8");
    }

    private String readBody(HttpExchange exchange, int maximum) throws IOException {
        InputStream input = exchange.getRequestBody();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (output.size() + read > maximum) throw new IllegalArgumentException("Request body is too large");
            output.write(buffer, 0, read);
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        applyHeaders(exchange);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private String error(String message) { return "{\"ok\":false,\"error\":" + Json.quote(message) + "}"; }
    private static String env(String name, String fallback) { String value = System.getenv(name); return value == null || value.trim().isEmpty() ? fallback : value.trim(); }
}
