package protocol;

public record Request(
    byte[] messageSize, // 4b
    byte[] requestApiKey, // 2b
    short requestApiVersion, //2b
    byte[] correlationId // 4b
) {

    public static class Builder {
        private byte[] messageSize;
        private byte[] requestApiKey;
        private short requestApiVersion;
        private byte[] correlationId;

        private Builder() {
        }

        public static Builder request() {
            return new Builder();
        }

        public Builder messageSize(byte[] messageSize) {
            this.messageSize = messageSize;
            return this;
        }

        public Builder requestApiKey(byte[] requestApiKey) {
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

        public Request build() {
            return new Request(messageSize, requestApiKey, requestApiVersion, correlationId);
        }
    }
}
