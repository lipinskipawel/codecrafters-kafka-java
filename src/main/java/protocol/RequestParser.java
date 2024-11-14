package protocol;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static protocol.Request.Builder.request;

public final class RequestParser {

    public Request parseRequest(InputStream inputStream) {
        try {
            final var reader = new BufferedInputStream(inputStream);
            return request()
                .messageSize(reader.readNBytes(4))
                .requestApiKey(reader.readNBytes(2))
                .requestApiVersion(readShort(reader.readNBytes(2)))
                .correlationId(reader.readNBytes(4))
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public short readShort(byte[] bytes) {
        return (short) (bytes[0] << 8 | (bytes[1] & 0xFF));
    }
}
