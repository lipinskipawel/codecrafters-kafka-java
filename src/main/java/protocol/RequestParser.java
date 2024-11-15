package protocol;

import protocol.DescribeTopicPartitionsRequest.Topics;
import protocol.DescribeTopicPartitionsRequest.Topics.Topic;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static protocol.ApiVersionsRequest.Builder.apiVersionsRequest;
import static protocol.DescribeTopicPartitionsRequest.Builder.describeTopicPartitionsRequest;

public final class RequestParser {

    public RequestMessage parseRequest(InputStream inputStream) {
        try {
            final var reader = new BufferedInputStream(inputStream);
            final var messageSize = reader.readNBytes(4);
            final var apiKey = readShort(reader.readNBytes(2));
            final var apiRequestVersion = readShort(reader.readNBytes(2));
            final var correlationId = reader.readNBytes(4);

            if (apiKey == 18) {
                return apiVersionsRequest()
                    .messageSize(messageSize)
                    .requestApiKey(apiKey)
                    .requestApiVersion(apiRequestVersion)
                    .correlationId(correlationId)
                    .build();
            }
            if (apiKey == 75) {
                final var clientIdLength = readShort(reader.readNBytes(2));
                final var clientId = new String(reader.readNBytes(clientIdLength), UTF_8);
                final var tagBuffer = reader.readNBytes(1)[0];

                final var numberOfTopics = reader.readNBytes(1)[0] - 1; // COMPACT_ARRAY
                final var topics = new Topics(range(0, numberOfTopics)
                    .mapToObj(__ -> parseTopic(reader))
                    .toList());
                final var responsePartitionLimit = reader.readNBytes(4);
                final var cursor = reader.readNBytes(1)[0];
                if (cursor != (byte) 0xFF) {
                    throw new RuntimeException("Cursor is not null value. Implement correct parsing");
                }
                final var lastTagBuffer = reader.readNBytes(1)[0];

                return describeTopicPartitionsRequest()
                    .messageSize(messageSize)
                    .requestApiKey(apiKey)
                    .requestApiVersion(apiRequestVersion)
                    .correlationId(correlationId)
                    .clientIdLength(clientIdLength)
                    .clientId(clientId)
                    .tagBuffer(tagBuffer)
                    .topics(topics)
                    .responsePartitionLimit(responsePartitionLimit)
                    .cursor(cursor)
                    .lastTagBuffer(lastTagBuffer)
                    .build();
            }
            throw new RuntimeException("Could not parse request with ApiKeys [%d]".formatted(apiKey));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Topic parseTopic(BufferedInputStream reader) {
        try {
            final var topicNameLength = reader.readNBytes(1)[0];
            final var topicName = new String(reader.readNBytes(topicNameLength - 1), UTF_8);
            final var tagBuffers = reader.readNBytes(1)[0];
            return new Topic(topicName, tagBuffers);
        } catch (IOException e) {
            throw new RuntimeException("Exception while parsing Topic's: ", e);
        }
    }

    private short readShort(byte[] bytes) {
        return (short) (bytes[0] << 8 | (bytes[1] & 0xFF));
    }
}
