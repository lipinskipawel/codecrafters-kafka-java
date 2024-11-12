import java.io.IOException;
import java.net.ServerSocket;

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

            final var outputStream = clientSocket.getOutputStream();
            outputStream.write(new byte[]{0, 0, 0, 0});
            outputStream.write(new byte[]{0, 0, 0, 7});
            outputStream.flush();
            Thread.sleep(100);

        } catch (IOException | InterruptedException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
