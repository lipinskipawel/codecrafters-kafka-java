package protocol;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static protocol.Request.Builder.request;

public final class RequestParser {

    public Request parseRequest(InputStream inputStream) {
        try {
            final var reader = new BufferedInputStream(inputStream);
            final var bytes = new byte[12];
            final var read = reader.read(bytes, 0, 12);
            if (read != 12) {
                throw new RuntimeException("Read only [%d]".formatted(read));
            }
            return request()
                .messageSize(slice(bytes, 0, 4))
                .requestApiKey(slice(bytes, 4, 6))
                .requestApiVersion(readShort(slice(bytes, 6, 8)))
                .correlationId(slice(bytes, 8, 12))
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] slice(byte[] bytes, int start, int end) {
        final var byteBuffer = ByteBuffer.wrap(bytes).order(BIG_ENDIAN);
        byteBuffer.position(start);
        byteBuffer.limit(end);

        var slice = new byte[byteBuffer.remaining()];
        byteBuffer.get(slice);

        return slice;
    }

    public short readShort(byte[] bytes) {
        return (short) (bytes[0] << 8 | (bytes[1] & 0xFF));
    }
}
