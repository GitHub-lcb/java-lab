package lab.redis;

import java.util.ArrayList;
import java.util.List;

public final class CommandParser {
    private CommandParser() {}

    public static List<String> parse(String input) {
        if (input == null || input.trim().isEmpty()) throw new IllegalArgumentException("Command is required");
        if (input.length() > 512) throw new IllegalArgumentException("Command is too long");
        List<String> tokens = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        boolean escaped = false;
        boolean started = false;
        for (int i = 0; i < input.length(); i++) {
            char value = input.charAt(i);
            if (escaped) {
                current.append(value);
                escaped = false;
                started = true;
            } else if (value == '\\') {
                escaped = true;
                started = true;
            } else if (value == '"') {
                quoted = !quoted;
                started = true;
            } else if (Character.isWhitespace(value) && !quoted) {
                if (started) {
                    tokens.add(current.toString());
                    current.setLength(0);
                    started = false;
                }
            } else {
                current.append(value);
                started = true;
            }
        }
        if (escaped) current.append('\\');
        if (quoted) throw new IllegalArgumentException("Unclosed quote in command");
        if (started) tokens.add(current.toString());
        if (tokens.size() > 12) throw new IllegalArgumentException("Too many command arguments");
        return tokens;
    }
}
