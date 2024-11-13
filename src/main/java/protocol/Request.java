package protocol;

public record Request(
    byte[] messageSize, // 4b
    byte[] requestApiKey, // 2b
    byte[] requestApiVersion, //2b
    byte[] correlationId // 4b
) {

    public static class Builder {
        private byte[] messageSize;
        private byte[] requestApiKey;
        private byte[] requestApiVersion;
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

        public Builder requestApiVersion(byte[] requestApiVersion) {
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
