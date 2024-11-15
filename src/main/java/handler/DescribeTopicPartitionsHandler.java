package handler;

import protocol.DescribeTopicPartitionsRequest;
import protocol.DescribeTopicPartitionsResponse.Topics;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.fromString;
import static protocol.DescribeTopicPartitionsResponse.Builder.describeTopicPartitionsResponse;
import static protocol.DescribeTopicPartitionsResponse.Topic;

public final class DescribeTopicPartitionsHandler {
    private final Socket socket;

    public DescribeTopicPartitionsHandler(Socket socket) {
        this.socket = requireNonNull(socket);
    }

    public void run(DescribeTopicPartitionsRequest request) {
        try {
            if (request.topics().topics().size() > 1) {
                throw new RuntimeException("To be implemented");
            }
            final var response = describeTopicPartitionsResponse()
                .throttleTime(new byte[]{0, 0, 0, 0})
                .correlationId(request.correlationId())
                .topics(new Topics(List.of(
                    new Topic(
                        new byte[]{0, 3}, // UNKNOWN_TOPIC_OR_PARTITION
                        request.topics().topics().get(0).name(),
                        fromString("00000000-0000-0000-0000-000000000000"),
                        false,
                        List.of(),
                        new byte[]{0, 13, 15, 8}, // 0000 1101 1111 1000
                        (byte) 0
                    )
                )))
                .nextCursor((byte) 0xFF)
                .tagBuffer((byte) 0x00)
                .lastTagBuffer((byte) 0x00)
                .build();

            final var outputStream = socket.getOutputStream();
            outputStream.write(response.toByteArray());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException("Exception while handling DescribeTopicPartitions request: ", e);
        }
    }
}
