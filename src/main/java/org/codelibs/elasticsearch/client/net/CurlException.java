package org.codelibs.elasticsearch.client.net;

public class CurlException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CurlException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public CurlException(final String message) {
        super(message);
    }

}
