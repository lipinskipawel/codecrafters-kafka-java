package protocol;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

// https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
public final class ApiKeys {
    public record ApiKey(
        byte[] apiKey,
        byte[] minVersion,
        byte[] maxVersion
    ) {
        public ApiKey {
            requireLength(apiKey, 2);
            requireLength(minVersion, 2);
            requireLength(maxVersion, 2);
        }

        private void requireLength(byte[] bytes, int length) {
            requireNonNull(bytes);
            if (bytes.length != length) {
                throw new RuntimeException("Length is different, wanted=[%d], got=[%d]".formatted(bytes.length, length));
            }
        }
    }

    private final byte[] bytes;

    public ApiKeys(List<ApiKey> apiKeys) {
        this.bytes = toBytes(apiKeys);
    }

    public byte[] toByteArray() {
        return bytes;
    }

    private byte[] toBytes(List<ApiKey> apiKeys) {
        final var bytes = ByteBuffer.allocate((apiKeys.size() * 7) + 1); // +1 for COMPACT_ARRAY
        bytes.put((byte) (apiKeys.size() + 1)); // COMPACT_ARRAY https://kafka.apache.org/protocol.html#protocol_types

        apiKeys.forEach(it -> {
            bytes.put(it.apiKey);
            bytes.put(it.minVersion);
            bytes.put(it.maxVersion);
            bytes.put((byte) 0); // TAG_BUFFER / tagged fields
        });

        return bytes.array();
    }
}
