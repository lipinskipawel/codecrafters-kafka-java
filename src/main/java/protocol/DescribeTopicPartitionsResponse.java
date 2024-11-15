package protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public record DescribeTopicPartitionsResponse(
    byte[] messageSize, // 4b
    byte[] correlationId, // 4b
    byte tagBuffer,
    byte[] throttleTime, // 4b
    Topics topics,
    byte nextCursor,
    byte lastTagBuffer
) {

    public DescribeTopicPartitionsResponse {
        requireNonNull(messageSize);
        requireNonNull(correlationId);
        requireNonNull(throttleTime);
    }

    public record Topics(List<Topic> topics) {
        public Topics {
            requireNonNull(topics);
        }

        private byte[] toByteArray() {
            try {
                final var byteArray = new ByteArrayOutputStream();

                byteArray.write(topics.size() + 1);
                for (var topic : topics) {
                    byteArray.write(topic.toByteArray());
                }

                return byteArray.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public record Topic(
        byte[] errorCode, // 2b
        String topicName,
        UUID topicId,
        boolean isInternal, // 1b
        List<Partitions> partitions,
        byte[] topicAuthorizedOperations, // 4b
        byte tagBuffer
    ) {

        public record Partitions() {

        }

        private byte[] toByteArray() {
            try {
                final var byteArray = new ByteArrayOutputStream();

                byteArray.write(errorCode());

                byteArray.write(topicName().length() + 1);
                byteArray.write(topicName().getBytes(UTF_8));

                final var uuidBytes = allocate(topicId().toString().replace("-", "").length() / 2)
                    .putLong(topicId().getMostSignificantBits())
                    .putLong(topicId().getLeastSignificantBits())
                    .array();
                byteArray.write(uuidBytes);

                byteArray.write(isInternal() ? (byte) 0x01 : (byte) 0x00);
                byteArray.write(1); // fix when needed

                byteArray.write(topicAuthorizedOperations());
                byteArray.write(tagBuffer());

                return byteArray.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public byte[] toByteArray() {
        try {
            final var byteArray = new ByteArrayOutputStream();

            byteArray.write(messageSize);
            byteArray.write(correlationId);
            byteArray.write(tagBuffer);
            byteArray.write(throttleTime);
            byteArray.write(topics.toByteArray());
            byteArray.write(nextCursor);
            byteArray.write(lastTagBuffer);

            return byteArray.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder {
        private byte[] correlationId;
        private byte tagBuffer;
        private byte[] throttleTime;
        private Topics topics;
        private byte nextCursor;
        private byte lastTagBuffer;

        private Builder() {
        }

        public static Builder describeTopicPartitionsResponse() {
            return new Builder();
        }

        public Builder correlationId(byte[] correlationId) {
            this.correlationId = correlationId;
            return this;
        }


        public Builder tagBuffer(byte tagBuffer) {
            this.tagBuffer = tagBuffer;
            return this;
        }

        public Builder throttleTime(byte[] throttleTime) {
            this.throttleTime = throttleTime;
            return this;
        }

        public Builder topics(Topics topics) {
            this.topics = topics;
            return this;
        }

        public Builder nextCursor(byte nextCursor) {
            this.nextCursor = nextCursor;
            return this;
        }

        public Builder lastTagBuffer(byte lastTagBuffer) {
            this.lastTagBuffer = lastTagBuffer;
            return this;
        }

        public DescribeTopicPartitionsResponse build() {
            int msgSize = correlationId.length
                + 1 // tagBuffer
                + throttleTime.length
                + topics.toByteArray().length
                + 1 // nextCursor
                + 1; // lastTagBuffer
            return new DescribeTopicPartitionsResponse(
                new byte[]{0, 0, 0, (byte) msgSize},
                correlationId,
                tagBuffer,
                throttleTime,
                topics,
                nextCursor,
                lastTagBuffer
            );
        }
    }
}
