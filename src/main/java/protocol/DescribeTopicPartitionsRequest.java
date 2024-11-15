package protocol;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record DescribeTopicPartitionsRequest(
    byte[] messageSize, // 4b
    short requestApiKey, // 2b
    short requestApiVersion, //2b
    byte[] correlationId, // 4b
    short clientIdLength,
    String clientId,
    byte tagBuffer,
    Topics topics,
    byte[] responsePartitionLimit, // 4b
    byte cursor,
    byte lastTagBuffer
) implements RequestMessage {

    public record Topics(
        List<Topic> topics
    ) {
        public Topics {
            requireNonNull(topics);
        }

        public record Topic(
            String name,
            byte tagBuffers
        ) {
            public Topic {
                requireNonNull(name);
            }
        }
    }

    public static class Builder {
        private byte[] messageSize;
        private short requestApiKey;
        private short requestApiVersion;
        private byte[] correlationId;
        private short clientIdLength;
        private String clientId;
        private byte tagBuffer;
        private Topics topics;
        private byte[] responsePartitionLimit;
        private byte cursor;
        private byte lastTagBuffer;

        private Builder() {
        }

        public static Builder describeTopicPartitionsRequest() {
            return new Builder();
        }

        public Builder messageSize(byte[] messageSize) {
            this.messageSize = messageSize;
            return this;
        }

        public Builder requestApiKey(short requestApiKey) {
            this.requestApiKey = requestApiKey;
            return this;
        }

        public Builder requestApiVersion(short requestApiVersion) {
            this.requestApiVersion = requestApiVersion;
            return this;
        }

        public Builder correlationId(byte[] correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder clientIdLength(short clientIdLength) {
            this.clientIdLength = clientIdLength;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder tagBuffer(byte tagBuffer) {
            this.tagBuffer = tagBuffer;
            return this;
        }

        public Builder topics(Topics topics) {
            this.topics = topics;
            return this;
        }

        public Builder responsePartitionLimit(byte[] responsePartitionLimit) {
            this.responsePartitionLimit = responsePartitionLimit;
            return this;
        }

        public Builder cursor(byte cursor) {
            this.cursor = cursor;
            return this;
        }

        public Builder lastTagBuffer(byte lastTagBuffer) {
            this.lastTagBuffer = lastTagBuffer;
            return this;
        }

        public DescribeTopicPartitionsRequest build() {
            return new DescribeTopicPartitionsRequest(
                messageSize,
                requestApiKey,
                requestApiVersion,
                correlationId,
                clientIdLength,
                clientId,
                tagBuffer,
                topics,
                responsePartitionLimit,
                cursor,
                lastTagBuffer
            );
        }
    }
}
