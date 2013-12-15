package com.jad.offheap.collections.serializer;

/**
 * @author: Ilya Krokhmalyov YC14IK1
 * @since: 12/15/13
 */

public class SerializationException extends RuntimeException {

    public SerializationException() {
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
