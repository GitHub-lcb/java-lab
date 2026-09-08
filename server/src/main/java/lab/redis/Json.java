package lab.redis;

import java.util.List;

public final class Json {
    private Json() {}

    public static String value(Object value) {
        if (value == null) return "null";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof RespClient.RedisError) return quote(((RespClient.RedisError) value).message);
        if (value instanceof List) {
            StringBuilder result = new StringBuilder("[");
            List<?> list = (List<?>) value;
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) result.append(',');
                result.append(value(list.get(i)));
            }
            return result.append(']').toString();
        }
        return quote(value.toString());
    }

    public static String quote(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '"': result.append("\\\""); break;
                case '\\': result.append("\\\\"); break;
                case '\n': result.append("\\n"); break;
                case '\r': result.append("\\r"); break;
                case '\t': result.append("\\t"); break;
                default:
                    if (character < 32) result.append(String.format("\\u%04x", (int) character));
                    else result.append(character);
            }
        }
        return result.append('"').toString();
    }
}
