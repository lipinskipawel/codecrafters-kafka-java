package protocol;

public record ApiVersionsRequest(
    byte[] messageSize, // 4b
    short requestApiKey, // 2b
    short requestApiVersion, //2b
    byte[] correlationId // 4b
) implements RequestMessage {

    public static class Builder {
        private byte[] messageSize;
        private short requestApiKey;
        private short requestApiVersion;
        private byte[] correlationId;

        private Builder() {
        }

        public static Builder apiVersionsRequest() {
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

        public ApiVersionsRequest build() {
            return new ApiVersionsRequest(messageSize, requestApiKey, requestApiVersion, correlationId);
        }
    }
}
