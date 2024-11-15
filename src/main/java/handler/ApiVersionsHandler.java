package handler;

import protocol.ApiKeys;
import protocol.ApiKeys.ApiKey;
import protocol.ApiVersionsRequest;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static protocol.ApiVersionsResponse.Builder.apiVersionsResponse;
import static protocol.ApiVersionsResponse.Builder.errorResponse;

public final class ApiVersionsHandler {
    private final Socket socket;

    public ApiVersionsHandler(Socket socket) {
        this.socket = requireNonNull(socket);
    }

    public void run(ApiVersionsRequest apiVersionsRequest) {
        try {
            if (apiVersionsRequest.requestApiVersion() < 0 || apiVersionsRequest.requestApiVersion() > 4) {
                final var response = errorResponse(apiVersionsRequest.correlationId(), new byte[]{0, 35});

                final var outputStream = socket.getOutputStream();
                outputStream.write(response);
                outputStream.flush();
                return;
            }
            final var response = apiVersionsResponse()
                .correlationId(apiVersionsRequest.correlationId())
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
        } catch (IOException e) {
            throw new RuntimeException("Exception while handling ApiVersions request: ", e);
        }
    }
}
