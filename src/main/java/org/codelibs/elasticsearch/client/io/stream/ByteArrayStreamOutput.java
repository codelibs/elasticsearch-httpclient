package org.codelibs.elasticsearch.client.io.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ByteArrayStreamOutput extends StreamOutput {
    private final ByteArrayOutputStream out;

    public ByteArrayStreamOutput() {
        this.out = new ByteArrayOutputStream();
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        out.write(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        out.write(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }

    public StreamInput toStreamInput() {
        return new InputStreamStreamInput(new ByteArrayInputStream(toByteArray()));
    }
}
