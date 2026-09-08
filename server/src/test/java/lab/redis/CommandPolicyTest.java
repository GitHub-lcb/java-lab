package lab.redis;

import java.util.Arrays;
import java.util.List;

public final class CommandPolicyTest {
    public static void main(String[] args) {
        parsesQuotedArguments();
        namespacesKeys();
        rejectsDangerousCommands();
        rejectsInvalidSessionsAndKeys();
        System.out.println("CommandPolicyTest: PASS");
    }

    private static void parsesQuotedArguments() {
        assertEquals(Arrays.asList("SET", "user:1", "Chen Bo"), CommandParser.parse("SET user:1 \"Chen Bo\""));
        assertEquals(Arrays.asList("GET", "user:1"), CommandParser.parse("  GET   user:1  "));
        assertThrows("Unclosed quote", new Runnable() { public void run() { CommandParser.parse("SET key \"value"); } });
    }

    private static void namespacesKeys() {
        RedisCommandPolicy policy = new RedisCommandPolicy();
        assertEquals(Arrays.asList("SET", "lab:abc12345:user:1", "value"), policy.sanitize("abc12345", CommandParser.parse("SET user:1 value")));
        assertEquals(Arrays.asList("SET", "lab:abc12345:lock:1", "token", "NX", "EX", "10"), policy.sanitize("abc12345", CommandParser.parse("SET lock:1 token NX EX 10")));
        assertEquals(Arrays.asList("DEL", "lab:abc12345:a", "lab:abc12345:b"), policy.sanitize("abc12345", CommandParser.parse("DEL a b")));
        assertEquals(Arrays.asList("MGET", "lab:abc12345:a", "lab:abc12345:b"), policy.sanitize("abc12345", CommandParser.parse("MGET a b")));
    }

    private static void rejectsDangerousCommands() {
        final RedisCommandPolicy policy = new RedisCommandPolicy();
        assertThrows("not allowed", new Runnable() { public void run() { policy.sanitize("abc12345", CommandParser.parse("FLUSHALL")); } });
        assertThrows("not allowed", new Runnable() { public void run() { policy.sanitize("abc12345", CommandParser.parse("EVAL return\\ 1 0")); } });
        assertThrows("argument", new Runnable() { public void run() { policy.sanitize("abc12345", CommandParser.parse("GET a b")); } });
    }

    private static void rejectsInvalidSessionsAndKeys() {
        final RedisCommandPolicy policy = new RedisCommandPolicy();
        assertThrows("session", new Runnable() { public void run() { policy.sanitize("bad", CommandParser.parse("GET key")); } });
        assertThrows("key", new Runnable() { public void run() { policy.sanitize("abc12345", CommandParser.parse("GET ../secret")); } });
    }

    private static void assertEquals(List<String> expected, List<String> actual) {
        if (!expected.equals(actual)) throw new AssertionError("Expected " + expected + " but got " + actual);
    }

    private static void assertThrows(String message, Runnable action) {
        try { action.run(); } catch (IllegalArgumentException error) {
            if (!error.getMessage().toLowerCase().contains(message.toLowerCase())) throw new AssertionError(error.getMessage());
            return;
        }
        throw new AssertionError("Expected error containing: " + message);
    }
}
