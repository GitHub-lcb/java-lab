package lab.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class RedisCommandPolicy {
    private static final Pattern SESSION = Pattern.compile("[A-Za-z0-9_-]{8,64}");
    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9_.:-]{1,80}");
    private static final Set<String> ALLOWED = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
        "PING", "SET", "GET", "DEL", "MGET", "EXISTS", "INCR", "DECR", "HSET", "HGET",
        "LPUSH", "RPUSH", "LRANGE", "SADD", "SMEMBERS", "ZADD", "ZRANGE", "EXPIRE", "TTL", "PTTL", "TYPE"
    )));

    public List<String> sanitize(String session, List<String> tokens) {
        if (!SESSION.matcher(session == null ? "" : session).matches()) throw new IllegalArgumentException("Invalid session identifier");
        if (tokens == null || tokens.isEmpty()) throw new IllegalArgumentException("Command is required");
        String command = tokens.get(0).toUpperCase(Locale.ROOT);
        if (!ALLOWED.contains(command)) throw new IllegalArgumentException("Command is not allowed in the learning environment");
        validateArity(command, tokens.size() - 1);
        List<String> sanitized = new ArrayList<String>(tokens);
        sanitized.set(0, command);
        for (Integer index : keyIndexes(command, tokens.size())) sanitized.set(index, namespace(session, sanitized.get(index)));
        for (int i = 1; i < sanitized.size(); i++) if (sanitized.get(i).length() > 256) throw new IllegalArgumentException("Command argument is too long");
        return sanitized;
    }

    public List<String> keysOf(List<String> sanitized) {
        if (sanitized == null || sanitized.isEmpty() || "PING".equals(sanitized.get(0))) return Collections.emptyList();
        List<String> keys = new ArrayList<String>();
        for (Integer index : keyIndexes(sanitized.get(0), sanitized.size())) keys.add(sanitized.get(index));
        return keys;
    }

    private String namespace(String session, String key) {
        if (!KEY.matcher(key).matches()) throw new IllegalArgumentException("Invalid key; use letters, numbers, colon, dot, dash or underscore");
        return "lab:" + session + ":" + key;
    }

    private List<Integer> keyIndexes(String command, int tokenCount) {
        if ("PING".equals(command)) return Collections.emptyList();
        List<Integer> indexes = new ArrayList<Integer>();
        if ("DEL".equals(command) || "MGET".equals(command) || "EXISTS".equals(command)) {
            for (int i = 1; i < tokenCount; i++) indexes.add(i);
        } else {
            indexes.add(1);
        }
        return indexes;
    }

    private void validateArity(String command, int arguments) {
        int minimum;
        int maximum;
        if ("PING".equals(command)) { minimum = 0; maximum = 0; }
        else if ("SET".equals(command)) { minimum = 2; maximum = 5; }
        else if ("DEL".equals(command) || "MGET".equals(command) || "EXISTS".equals(command)) { minimum = 1; maximum = 5; }
        else if ("HSET".equals(command) || "ZADD".equals(command)) { minimum = 3; maximum = 3; }
        else if ("HGET".equals(command)) { minimum = 2; maximum = 2; }
        else if ("LPUSH".equals(command) || "RPUSH".equals(command) || "SADD".equals(command)) { minimum = 2; maximum = 10; }
        else if ("LRANGE".equals(command)) { minimum = 3; maximum = 3; }
        else if ("ZRANGE".equals(command)) { minimum = 3; maximum = 4; }
        else if ("EXPIRE".equals(command)) { minimum = 2; maximum = 2; }
        else { minimum = 1; maximum = 1; }
        if (arguments < minimum || arguments > maximum) throw new IllegalArgumentException("Invalid argument count for " + command);
    }
}
