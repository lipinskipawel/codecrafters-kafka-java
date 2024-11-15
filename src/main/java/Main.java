import protocol.ApiKeys;
import protocol.ApiKeys.ApiKey;
import protocol.RequestParser;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static protocol.Response.Builder.errorResponse;
import static protocol.Response.Builder.response;

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

                if (request.requestApiVersion() < 0 || request.requestApiVersion() > 4) {
                    final var response = errorResponse(request.correlationId(), new byte[]{0, 35});

                    final var outputStream = socket.getOutputStream();
                    outputStream.write(response);
                    outputStream.flush();
                    return;
                }
                final var response = response()
                    .correlationId(request.correlationId())
                    .errorCode(new byte[]{0, 0})
                    .apiKeys(new ApiKeys(List.of(
                        new ApiKey(
                            new byte[]{0, 18}, // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
                            new byte[]{0, 3},
                            new byte[]{0, 4}
                        ),
                        new ApiKey(
                            new byte[]{0, 75}, // https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
                            new byte[]{0, 0},
                            new byte[]{0, 0}
                        )
                    )))
                    .throttleTimeMs(new byte[]{0, 0, 0, 0})
                    .build();

                final var outputStream = socket.getOutputStream();
                outputStream.write(response.toByteArray());
                outputStream.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while handing connection: ", e);
        }
    }
}
