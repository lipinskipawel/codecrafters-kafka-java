package protocol;

public record Response(
    byte[] messageSize, // 4b
    byte[] correlationId, // 4b
    byte[] errorCode
) {

    public static class Builder {
        private byte[] messageSize;
        private byte[] correlationId;
        private byte[] errorCode;

        private Builder() {
        }

        public static Builder response() {
            return new Builder();
        }

        public Builder messageSize(byte[] messageSize) {
            this.messageSize = messageSize;
            return this;
        }

        public Builder correlationId(byte[] correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder errorCode(byte[] errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Response build() {
            return new Response(messageSize, correlationId, errorCode);
        }
    }
}
