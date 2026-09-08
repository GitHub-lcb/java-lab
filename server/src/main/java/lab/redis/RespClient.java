package lab.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class RespClient {
    public static final class RedisError {
        public final String message;
        RedisError(String message) { this.message = message; }
    }

    private final String host;
    private final int port;

    public RespClient(String host, int port) { this.host = host; this.port = port; }

    public Object execute(List<String> command) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), 700);
            socket.setSoTimeout(2000);
            OutputStream output = new BufferedOutputStream(socket.getOutputStream());
            writeCommand(output, command);
            output.flush();
            return readValue(new BufferedInputStream(socket.getInputStream()));
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void writeCommand(OutputStream output, List<String> command) throws IOException {
        output.write(("*" + command.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String argument : command) {
            byte[] bytes = argument.getBytes(StandardCharsets.UTF_8);
            output.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            output.write(bytes);
            output.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    private Object readValue(InputStream input) throws IOException {
        int prefix = input.read();
        if (prefix < 0) throw new EOFException("Redis closed the connection");
        String line;
        switch (prefix) {
            case '+': return readLine(input);
            case '-': return new RedisError(readLine(input));
            case ':': return Long.valueOf(readLine(input));
            case '$':
                int length = Integer.parseInt(readLine(input));
                if (length < 0) return null;
                byte[] value = readExactly(input, length);
                expectCrlf(input);
                return new String(value, StandardCharsets.UTF_8);
            case '*':
                int count = Integer.parseInt(readLine(input));
                if (count < 0) return null;
                List<Object> values = new ArrayList<Object>(count);
                for (int i = 0; i < count; i++) values.add(readValue(input));
                return values;
            default: throw new IOException("Unknown Redis response prefix: " + (char) prefix);
        }
    }

    private String readLine(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int current = input.read();
            if (current < 0) throw new EOFException("Redis response ended unexpectedly");
            if (previous == '\r' && current == '\n') break;
            if (previous >= 0) output.write(previous);
            previous = current;
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private byte[] readExactly(InputStream input, int length) throws IOException {
        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = input.read(bytes, offset, length - offset);
            if (read < 0) throw new EOFException("Redis bulk response ended unexpectedly");
            offset += read;
        }
        return bytes;
    }

    private void expectCrlf(InputStream input) throws IOException {
        if (input.read() != '\r' || input.read() != '\n') throw new IOException("Invalid Redis bulk response terminator");
    }
}
