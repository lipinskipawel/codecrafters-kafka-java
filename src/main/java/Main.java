import protocol.RequestParser;

import java.io.IOException;
import java.net.ServerSocket;

import static protocol.Response.Builder.response;

// Sending a request
// echo -n "Placeholder request" | nc -v localhost 9092 | hexdump -C
public class Main {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try (final var serverSocket = new ServerSocket(PORT)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            final var clientSocket = serverSocket.accept();

            final var requestParser = new RequestParser();
            final var request = requestParser.parseRequest(clientSocket.getInputStream());

            if (request.requestApiVersion() < 0 || request.requestApiVersion() > 4) {
                final var response = response()
                    .messageSize(new byte[]{0, 0, 0, 0})
                    .correlationId(request.correlationId())
                    .errorCode(new byte[]{0, 35})
                    .build();

                final var outputStream = clientSocket.getOutputStream();
                outputStream.write(response.messageSize());
                outputStream.write(response.correlationId());
                outputStream.write(response.errorCode());
                outputStream.flush();
                Thread.sleep(100);
                return;
            }

            final var response = response()
                .messageSize(request.messageSize())
                .correlationId(request.correlationId())
                .build();

            final var outputStream = clientSocket.getOutputStream();
            outputStream.write(response.messageSize());
            outputStream.write(response.correlationId());
            outputStream.flush();
            Thread.sleep(100);

        } catch (IOException | InterruptedException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
