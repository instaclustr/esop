package com.instaclustr.esop.impl.hash;

import java.nio.file.Path;

import com.instaclustr.esop.impl.ManifestEntry;

public interface HashService {

    String hash(Path file) throws HashingException;

    String hash(ManifestEntry entry) throws HashingException;

    void verify(ManifestEntry entry) throws HashVerificationException;

    void verify(Path file, String hash) throws HashVerificationException;

    class HashingException extends RuntimeException {

        public HashingException(final String message) {
            super(message);
        }

        public HashingException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    class HashVerificationException extends RuntimeException {

        public HashVerificationException(final String message) {
            super(message);
        }

        public HashVerificationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
