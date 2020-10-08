package com.instaclustr.esop.gcp;

import java.nio.file.Path;

import com.google.cloud.storage.BlobId;
import com.instaclustr.esop.impl.RemoteObjectReference;

public class GCPRemoteObjectReference extends RemoteObjectReference {
    public final BlobId blobId;

    public GCPRemoteObjectReference(final Path objectKey, final String canonicalPath, final String bucket) {
        super(objectKey, canonicalPath);
        this.blobId = BlobId.of(bucket, canonicalPath);
    }

    public Path getObjectKey() {
        return objectKey;
    }
}
