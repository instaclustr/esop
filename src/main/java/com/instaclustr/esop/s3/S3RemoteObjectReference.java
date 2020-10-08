package com.instaclustr.esop.s3;

import java.nio.file.Path;

import com.instaclustr.esop.impl.RemoteObjectReference;

public class S3RemoteObjectReference extends RemoteObjectReference {

    public S3RemoteObjectReference(final Path objectKey, final String canonicalPath) {
        super(objectKey, canonicalPath);
    }

    @Override
    public Path getObjectKey() {
        return objectKey;
    }
}
