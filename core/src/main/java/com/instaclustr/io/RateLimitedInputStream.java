package com.instaclustr.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.RateLimiter;

public class RateLimitedInputStream extends FilterInputStream {

    final RateLimiter limiter;
    private final AtomicBoolean shouldCancel;

    public RateLimitedInputStream(final InputStream in,
                                  final RateLimiter limiter,
                                  final AtomicBoolean shouldCancel) {
        super(in);
        this.limiter = limiter;
        this.shouldCancel = shouldCancel;
    }

    @Override
    public int read() throws IOException {
        limiter.acquire();

        if (shouldCancel.get()) {
            throw new IOException("read was cancelled");
        }

        return super.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        limiter.acquire(Math.max(1, b.length));

        if (shouldCancel.get()) {
            throw new IOException("read was cancelled");
        }

        return super.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        limiter.acquire(Math.max(1, len));

        if (shouldCancel.get()) {
            throw new IOException("read was cancelled");
        }

        return super.read(b, off, len);
    }
}