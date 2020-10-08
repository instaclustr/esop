package com.instaclustr.esop.local;

import java.nio.file.Path;

import com.instaclustr.esop.impl.RemoteObjectReference;

public class LocalFileObjectReference extends RemoteObjectReference {

    public LocalFileObjectReference(final Path objectKey, final String canonicalPath) {
        super(objectKey, canonicalPath);
    }

    @Override
    public Path getObjectKey() {
        return objectKey;
    }
}
