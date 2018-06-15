package org.codelibs.elasticsearch.client.net;

import org.elasticsearch.node.Node;

public class Curl {

    protected Curl() {
        // nothing
    }

    public static CurlRequest get(final Node node, final String path) {
        return new CurlRequest(Method.GET, node, path);
    }

    public static CurlRequest post(final Node node, final String path) {
        return new CurlRequest(Method.POST, node, path);
    }

    public static CurlRequest put(final Node node, final String path) {
        return new CurlRequest(Method.PUT, node, path);
    }

    public static CurlRequest delete(final Node node, final String path) {
        return new CurlRequest(Method.DELETE, node, path);
    }

    public static CurlRequest head(final Node node, final String path) {
        return new CurlRequest(Method.HEAD, node, path);
    }

    public static CurlRequest get(final String url) {
        return new CurlRequest(Method.GET, url);
    }

    public static CurlRequest post(final String url) {
        return new CurlRequest(Method.POST, url);
    }

    public static CurlRequest put(final String url) {
        return new CurlRequest(Method.PUT, url);
    }

    public static CurlRequest delete(final String url) {
        return new CurlRequest(Method.DELETE, url);
    }

    public static CurlRequest head(final String url) {
        return new CurlRequest(Method.HEAD, url);
    }

    public enum Method {
        GET, POST, PUT, DELETE, HEAD;
    }
}
