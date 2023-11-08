package com.instaclustr.esop.s3.v1;

public final class S3ModuleException extends RuntimeException {

    public S3ModuleException(String message, Throwable cause) {
        super(message, cause);
    }

    public S3ModuleException(String message) {
        super(message);
    }
}
