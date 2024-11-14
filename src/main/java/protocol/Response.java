package protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public record Response(
    byte[] messageSize, // 4b
    byte[] correlationId, // 4b
    byte[] errorCode, // 2b
    ApiKeys apiKeys,
    byte[] throttleTimeMs, // 4b
    byte tagBuffer
) {

    public byte[] toByteArray() {
        try {
            final var byteArray = new ByteArrayOutputStream();

            byteArray.write(messageSize);
            byteArray.write(correlationId);
            byteArray.write(errorCode);
            byteArray.write(apiKeys.toByteArray());
            byteArray.write(throttleTimeMs);
            byteArray.write(tagBuffer);

            return byteArray.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder {
        private byte[] correlationId;
        private byte[] errorCode;
        private ApiKeys apiKeys;
        private byte[] throttleTimeMs;

        private Builder() {
        }

        public static Builder response() {
            return new Builder();
        }

        public static byte[] errorResponse(byte[] correlationId, byte[] errorCode) {
            return ByteBuffer.allocate(10)
                .putInt(6)
                .put(correlationId)
                .put(errorCode)
                .array();
        }

        public Builder correlationId(byte[] correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder errorCode(byte[] errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder apiKeys(ApiKeys apiKeys) {
            this.apiKeys = apiKeys;
            return this;
        }

        public Builder throttleTimeMs(byte[] throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs;
            return this;
        }

        public Response build() {
            int msgSize = correlationId.length
                + errorCode.length
                + apiKeys.toByteArray().length
                + throttleTimeMs.length
                + 1; // tagBuffer
            return new Response(
                new byte[]{0, 0, 0, (byte) (msgSize)}, // fix when needed
                correlationId,
                errorCode,
                apiKeys,
                throttleTimeMs,
                (byte) 0 // tagBuffer
            );
        }
    }
}
