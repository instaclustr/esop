package com.instaclustr.cassandra.backup.s3;

import java.nio.file.Path;

import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;

public class S3RemoteObjectReference extends RemoteObjectReference {

    public S3RemoteObjectReference(final Path objectKey, final String canonicalPath) {
        super(objectKey, canonicalPath);
    }

    @Override
    public Path getObjectKey() {
        return objectKey;
    }
}
