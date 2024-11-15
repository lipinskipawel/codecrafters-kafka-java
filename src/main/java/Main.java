import handler.ApiVersionsHandler;
import handler.DescribeTopicPartitionsHandler;
import protocol.ApiVersionsRequest;
import protocol.DescribeTopicPartitionsRequest;
import protocol.RequestParser;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

// Sending a request
// echo -n "Placeholder request" | nc -v localhost 9092 | hexdump -C
public class Main {
    private static final int PORT = 9092;
    private static final ExecutorService POOL = newVirtualThreadPerTaskExecutor();

    public static void main(String[] args) {
        try (final var serverSocket = new ServerSocket(PORT)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            while (true) {
                final var socket = serverSocket.accept();
                POOL.submit(() -> handleConnection(socket));
            }

        } catch (IOException e) {
            // in theory, we can be here also because of new ServerSocket(PORT)
            System.err.println("Exception while waiting for incoming connection: " + e.getMessage());
        }
    }

    public static void handleConnection(Socket socket) {
        try {
            while (!socket.isClosed()) {
                final var requestParser = new RequestParser();
                final var request = requestParser.parseRequest(socket.getInputStream());

                switch (request) {
                    case ApiVersionsRequest req -> new ApiVersionsHandler(socket).run(req);
                    case DescribeTopicPartitionsRequest req -> new DescribeTopicPartitionsHandler(socket).run(req);
                    default ->
                        throw new RuntimeException("API_KEYS not supported yet [%s]".formatted(request.getClass()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while handing connection: ", e);
        }
    }
}
